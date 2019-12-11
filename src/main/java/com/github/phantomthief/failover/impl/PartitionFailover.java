package com.github.phantomthief.failover.impl;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.github.phantomthief.failover.Failover;

/**
 * @author huangli
 * Created on 2019-12-10
 */
public class PartitionFailover<T> implements Failover<T>, Closeable {

    private final Map<T, Integer> initWeightMap;
    private final WeightFailover<T> weightFailover;
    private final int corePartitionSize;
    private final long maxExternalPoolIdleMillis;

    private volatile ResEntry<T>[] resources;

    private static class ResEntry<T> {
        ResEntry(T object, int initWeight, int initConcurrency) {
            this.object = object;
            this.initWeight = initWeight;
            this.concurrency = new AtomicInteger(initConcurrency);
        }

        @SuppressWarnings("checkstyle:VisibilityModifier")
        final T object;
        @SuppressWarnings("checkstyle:VisibilityModifier")
        final int initWeight;

        @SuppressWarnings("checkstyle:VisibilityModifier")
        volatile long lastReturnTime;
        @SuppressWarnings("checkstyle:VisibilityModifier")
        AtomicInteger concurrency;

    }

    private static class ResEntryEx<T> extends ResEntry<T> {
        @SuppressWarnings("checkstyle:VisibilityModifier")
        double scoreWeight;

        ResEntryEx(T object, int initWeight, int initConcurrency) {
            super(object, initWeight, initConcurrency);
        }
    }

    PartitionFailover(PartitionFailoverBuilder<T> partitionFailoverBuilder,
            WeightFailover<T> weightFailover, Map<T, Integer> initWeightMap) {
        this.weightFailover = weightFailover;
        this.initWeightMap = initWeightMap;
        this.corePartitionSize = partitionFailoverBuilder.corePartitionSize;
        this.maxExternalPoolIdleMillis = partitionFailoverBuilder.maxExternalPoolIdleMillis;
        resources = new ResEntry[corePartitionSize];

        List<T> list = new ArrayList<>();
        for (int i = 0; i < corePartitionSize; i++) {
            T one = weightFailover.getOneAvailableExclude(list);
            list.add(one);
            resources[i] = new ResEntry<>(one, initWeightMap.get(one), 0);
        }
    }

    private ResEntryEx<T>[] deepCopyResource() {
        ResEntry<T>[] refCopy = resources;
        ResEntryEx[] copy = new ResEntryEx[refCopy.length];
        for (int i = 0; i < refCopy.length; i++) {
            ResEntry<T> res = refCopy[i];
            ResEntryEx<T> resEx = new ResEntryEx<>(res.object, res.initWeight, res.concurrency.get());
            resEx.lastReturnTime = res.lastReturnTime;
            copy[i] = resEx;
        }
        return copy;
    }

    @Override
    public List<T> getAll() {
        return weightFailover.getAll();
    }

    @Override
    public void fail(@Nonnull T object) {
        weightFailover.fail(object);
        subtractConcurrency(object);
        if (weightFailover.currentWeight(object) <= 0) {
            replaceDownResource(object);
        }
    }

    @Override
    public void down(@Nonnull T object) {
        weightFailover.down(object);
        subtractConcurrency(object);
        replaceDownResource(object);
    }

    @Override
    public void success(@Nonnull T object) {
        weightFailover.success(object);
        subtractConcurrency(object);
    }

    @Nullable
    private ResEntry lookup(Object object) {
        ResEntry<T>[] refCopy = resources;
        for (ResEntry res : refCopy) {
            if (res.object == object) {
                return res;
            }
        }
        return null;
    }

    private void subtractConcurrency(@Nonnull T object) {
        ResEntry resEntry = lookup(object);
        if (resEntry == null) {
            return;
        }
        resEntry.lastReturnTime = System.currentTimeMillis();
        resEntry.concurrency.updateAndGet(oldValue -> oldValue - 1 < 0 ? 0 : oldValue - 1);
    }

    private void addConcurrency(@Nonnull T object) {
        ResEntry resEntry = lookup(object);
        if (resEntry == null) {
            return;
        }
        resEntry.lastReturnTime = System.currentTimeMillis();
        resEntry.concurrency.updateAndGet(oldValue -> oldValue + 1 < 0 ? 1 : oldValue + 1);
    }

    // weak consistency
    private void replaceDownResource(T object) {
        ResEntry<T>[] resourceRefCopy = resources;
        if (resourceRefCopy.length == initWeightMap.size()) {
            // so there is no more resource in weightFailover
            return;
        }
        ResEntry<T>[] newList = new ResEntry[resourceRefCopy.length];
        int index = -1;
        for (int i = 0; i < resourceRefCopy.length; i++) {
            newList[i] = resourceRefCopy[i];
            if (newList[i].object == object) {
                index = i;
            }
        }
        if (index == -1) {
            // maybe replaced by another thread
            return;
        }
        List<T> excludes = Stream.of(resourceRefCopy).map(r -> r.object).collect(Collectors.toList());
        T newOne = weightFailover.getOneAvailableExclude(excludes);
        if (newOne == null) {
            //no more available
            return;
        }
        newList[index] = new ResEntry<>(newOne, initWeightMap.get(newOne), 0);
        resources = newList;
    }

    @Nullable
    @Override
    public T getOneAvailable() {
        // we use recent resource when:
        // 1, tps of caller is slow (all concurrency is 0)
        // 2, all resources are healthy (all currentWeight == initWeight)
        // 3, at least there is one call returned in recent
        boolean noCallInProgress = true;
        boolean allResIsHealthy = true;
        boolean hasRecentReturnedCall = false;
        long now = System.currentTimeMillis();
        ResEntryEx<T>[] resourcesCopy = deepCopyResource();
        for (ResEntryEx<T> res : resourcesCopy) {
            int currentWeight = weightFailover.currentWeight(res.object);
            res.scoreWeight = currentWeight / (res.concurrency.get() + 1);
            if (res.scoreWeight < 0) {
                // something wrong
                res.scoreWeight = 0;
            }
            if (res.concurrency.get() > 0) {
                noCallInProgress = false;
            }
            if (currentWeight != res.initWeight) {
                allResIsHealthy = false;
            }
            if ((now - res.lastReturnTime) < maxExternalPoolIdleMillis) {
                hasRecentReturnedCall = true;
            }
        }
        T one;
        if (maxExternalPoolIdleMillis > 0 && noCallInProgress && allResIsHealthy && hasRecentReturnedCall) {
            one = selectRecentOne(resourcesCopy);
        } else {
            one = selectByScore(resourcesCopy);
        }
        addConcurrency(one);
        return one;
    }

    private static <T> T selectRecentOne(ResEntryEx<T>[] resourcesCopy) {
        return Stream.of(resourcesCopy).max((r1, r2) -> {
            if (r1.lastReturnTime == r2.lastReturnTime) {
                return 0;
            } else if (r1.lastReturnTime < r2.lastReturnTime) {
                return -1;
            } else {
                return 1;
            }
        }).get().object;
    }

    private static <T> T selectByScore(ResEntryEx<T>[] resourcesCopy) {
        double sumOfScore = Stream.of(resourcesCopy).mapToDouble(r -> r.scoreWeight).sum();
        if (sumOfScore <= 0) {
            // all down
            return null;
        }
        double selectValue = ThreadLocalRandom.current().nextDouble(sumOfScore);
        double x = 0;
        for (ResEntryEx<T> res : resourcesCopy) {
            x += res.scoreWeight;
            if (selectValue < x) {
                return res.object;
            }
        }
        // something wrong or there are float precision problem
        return resourcesCopy[0].object;
    }

    @Override
    public List<T> getAvailable() {
        return Stream.of(resources)
                .filter(r -> weightFailover.currentWeight(r.object) > 0)
                .map(r -> r.object)
                .collect(Collectors.toList());
    }

    @Override
    public Set<T> getFailed() {
        return weightFailover.getFailed();
    }

    @Override
    public void close() {
        weightFailover.close();
    }

    @Override
    public List<T> getAvailable(int n) {
        // we don't know which resource is used, so this method is not supported
        throw new UnsupportedOperationException();
    }
}
