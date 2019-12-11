package com.github.phantomthief.failover.impl;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import com.github.phantomthief.util.ThrowableFunction;
import com.github.phantomthief.util.ThrowablePredicate;

public class PartitionFailoverBuilder<T> {

    private WeightFailoverBuilder<T> weightFailoverBuilder = new WeightFailoverBuilder<>();

    @SuppressWarnings("checkstyle:VisibilityModifier")
    int corePartitionSize = 0;

    @SuppressWarnings("checkstyle:VisibilityModifier")
    long maxExternalPoolIdleMillis;

    @Nonnull
    public PartitionFailover<T> build(Collection<T> original) {
        ensure();
        WeightFailover<T> weightFailover = weightFailoverBuilder.build(original);
        return new PartitionFailover<>(this, weightFailover);
    }

    @Nonnull
    public PartitionFailover<T> build(Collection<T> original, int initWeight) {
        ensure();
        WeightFailover<T> weightFailover = weightFailoverBuilder.build(original, initWeight);
        return new PartitionFailover<>(this, weightFailover);
    }

    @Nonnull
    public PartitionFailover<T> build(Map<T, Integer> original) {
        ensure();
        checkArgument(corePartitionSize <= original.size(), "illegal coreSize: " + corePartitionSize);
        WeightFailover<T> weightFailover = weightFailoverBuilder.build(original);
        return new PartitionFailover<>(this, weightFailover);
    }

    private void ensure() {
        checkArgument(corePartitionSize > 0, "corePartitionSize has to be positive");
        checkArgument(maxExternalPoolIdleMillis > 0, "maxExternalPoolIdleMillis has to be positive");
    }

    @CheckReturnValue
    @Nonnull
    public PartitionFailoverBuilder<T> corePartitionSize(int size) {
        checkArgument(size > 0, "corePartitionSize has to be positive");
        this.corePartitionSize = size;
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public PartitionFailoverBuilder<T> externalPool(long millis) {
        checkArgument(millis > 0, "maxExternalPoolIdleMillis has to be positive");
        this.maxExternalPoolIdleMillis = millis;
        return this;
    }

    //-------------------------methods delegate to weightFailoverBuilder below---------------------

    @CheckReturnValue
    @Nonnull
    public PartitionFailoverBuilder<T> name(String value) {
        weightFailoverBuilder.name(value);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public PartitionFailoverBuilder<T> autoAddOnMissing(int weight) {
        weightFailoverBuilder.autoAddOnMissing(weight);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public PartitionFailoverBuilder<T> onMinWeight(Consumer<T> listener) {
        weightFailoverBuilder.onMinWeight(listener);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public PartitionFailoverBuilder<T> onRecovered(Consumer<T> listener) {
        weightFailoverBuilder.onRecovered(listener);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public PartitionFailoverBuilder<T> minWeight(int value) {
        weightFailoverBuilder.minWeight(value);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public PartitionFailoverBuilder<T> failReduceRate(double rate) {
        weightFailoverBuilder.failReduceRate(rate);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public PartitionFailoverBuilder<T> failReduce(int weight) {
        weightFailoverBuilder.failReduce(weight);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public PartitionFailoverBuilder<T> successIncreaseRate(double rate) {
        weightFailoverBuilder.successIncreaseRate(rate);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public PartitionFailoverBuilder<T> successIncrease(int weight) {
        weightFailoverBuilder.successIncrease(weight);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public PartitionFailoverBuilder<T> checkDuration(long time, TimeUnit unit) {
        weightFailoverBuilder.checkDuration(time, unit);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public PartitionFailoverBuilder<T> filter(@Nonnull Predicate<T> filter) {
        weightFailoverBuilder.filter(filter);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public PartitionFailoverBuilder<T> checker(
            @Nonnull ThrowableFunction<? super T, Double, Throwable> failChecker) {
        weightFailoverBuilder.checker(failChecker);
        return this;
    }

    @CheckReturnValue
    @Nonnull
    public PartitionFailoverBuilder<T> checker(
            @Nonnull ThrowablePredicate<? super T, Throwable> failChecker,
            @Nonnegative double recoveredInitRate) {
        weightFailoverBuilder.checker(failChecker, recoveredInitRate);
        return this;
    }

}