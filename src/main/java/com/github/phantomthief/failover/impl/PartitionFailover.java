package com.github.phantomthief.failover.impl;

import static java.util.Comparator.comparingLong;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.github.phantomthief.failover.Failover;

/**
 * @author huangli
 * Created on 2019-12-10
 */
public class PartitionFailover<T> implements Failover<T>, Closeable {

    private final WeightFailover<T> weightFailover;
    private final long maxExternalPoolIdleMillis;

    private volatile List<ResEntry<T>> resources;

    @SuppressWarnings("checkstyle:VisibilityModifier")
    private static class ResEntry<T> {

        final T object;
        final int initWeight;
        final AtomicInteger concurrency;
        volatile long lastReturnTime;

        ResEntry(T object, int initWeight) {
            this(object, initWeight, 0);
        }

        ResEntry(T object, int initWeight, int initConcurrency) {
            this.object = object;
            this.initWeight = initWeight;
            this.concurrency = new AtomicInteger(initConcurrency);
        }

    }

    private static class ResEntryEx<T> extends ResEntry<T> {

        private int scoreWeight;

        ResEntryEx(T object, int initWeight) {
            super(object, initWeight);
        }

        ResEntryEx(T object, int initWeight, int initConcurrency) {
            super(object, initWeight, initConcurrency);
        }

    }

    PartitionFailover(PartitionFailoverBuilder<T> partitionFailoverBuilder,
            WeightFailover<T> weightFailover) {
        this.weightFailover = weightFailover;
        this.maxExternalPoolIdleMillis = partitionFailoverBuilder.maxExternalPoolIdleMillis;
        this.resources = weightFailover.getAvailable(partitionFailoverBuilder.corePartitionSize).stream()
                .map(instance -> new ResEntry<>(instance, weightFailover.initWeight(instance)))
                .collect(toList());
    }

    private List<ResEntryEx<T>> deepCopyResource() {
        return resources.stream()
                .map(res -> {
                    ResEntryEx<T> ex = new ResEntryEx<>(res.object, res.initWeight, res.concurrency.get());
                    ex.lastReturnTime = res.lastReturnTime;
                    return ex;
                })
                .collect(toList());
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
    private ResEntry<T> lookup(T object) {
        List<ResEntry<T>> refCopy = resources;
        for (ResEntry<T> res : refCopy) {
            if (res.object == object) {
                return res;
            }
        }
        return null;
    }

    private void subtractConcurrency(@Nonnull T object) {
        ResEntry<T> resEntry = lookup(object);
        if (resEntry == null) {
            return;
        }
        resEntry.lastReturnTime = System.currentTimeMillis();
        resEntry.concurrency.updateAndGet(oldValue -> Math.max(oldValue - 1, 0));
    }

    private void addConcurrency(@Nonnull T object) {
        ResEntry<T> resEntry = lookup(object);
        if (resEntry == null) {
            return;
        }
        resEntry.lastReturnTime = System.currentTimeMillis();
        resEntry.concurrency.updateAndGet(oldValue -> oldValue + 1 < 0 ? 1 : oldValue + 1);
    }

    // weak consistency
    private void replaceDownResource(T object) {
        List<ResEntry<T>> resourceRefCopy = resources;
        if (resourceRefCopy.size() == weightFailover.getAll().size()) {
            // so there is no more resource in weightFailover
            return;
        }
        List<ResEntry<T>> newList = new ArrayList<>(resourceRefCopy.size());
        int index = -1;
        for (int i = 0; i < resourceRefCopy.size(); i++) {
            newList.set(i, resourceRefCopy.get(i));
            if (newList.get(i).object == object) {
                index = i;
            }
        }
        if (index == -1) {
            // maybe replaced by another thread
            return;
        }
        List<T> excludes = resourceRefCopy.stream().map(r -> r.object).collect(toList());
        T newOne = weightFailover.getOneAvailableExclude(excludes);
        if (newOne == null) {
            //no more available
            return;
        }
        newList.set(index, new ResEntry<>(newOne, weightFailover.initWeight(newOne)));
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
        List<ResEntryEx<T>> resourcesCopy = deepCopyResource();
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

    /**
     * FIXME 待实现
     */
    @Nullable
    @Override
    public T getOneAvailableExclude(Collection<T> exclusions) {
        return null;
    }

    private static <T> T selectRecentOne(List<ResEntryEx<T>> resourcesCopy) {
        return resourcesCopy.stream()
                .max(comparingLong(r -> r.lastReturnTime))
                .orElse(resourcesCopy.get(0))
                .object;
    }

    private static <T> T selectByScore(List<ResEntryEx<T>> resourcesCopy) {
        int sumOfScore = resourcesCopy.stream().mapToInt(r -> r.scoreWeight).sum();
        if (sumOfScore <= 0) {
            // all down
            return null;
        }
        int selectValue = ThreadLocalRandom.current().nextInt(sumOfScore);
        int x = 0;
        for (ResEntryEx<T> res : resourcesCopy) {
            x += res.scoreWeight;
            if (selectValue < x) {
                return res.object;
            }
        }
        // something wrong or there are float precision problem
        return resourcesCopy.get(0).object;
    }

    @Override
    public List<T> getAvailable() {
        List<T> available = weightFailover.getAvailable();
        return resources.stream()
                .map(r -> r.object)
                .filter(available::contains)
                .collect(collectingAndThen(toList(), Collections::unmodifiableList));
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