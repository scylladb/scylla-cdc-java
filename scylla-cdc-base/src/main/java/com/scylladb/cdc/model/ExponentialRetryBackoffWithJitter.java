package com.scylladb.cdc.model;

import com.google.common.base.Preconditions;

import java.util.concurrent.ThreadLocalRandom;

public class ExponentialRetryBackoffWithJitter implements RetryBackoff {
    private final int maximumBackoffMs;
    private final int backoffBase;
    private final double jitterPercentage;
    private final int maxJitterMs;

    public ExponentialRetryBackoffWithJitter(int backoffBase, int maximumBackoffMs, double jitterPercentage) {
        Preconditions.checkArgument(maximumBackoffMs > 0);
        this.maximumBackoffMs = maximumBackoffMs;

        Preconditions.checkArgument(backoffBase > 0);
        this.backoffBase = backoffBase;

        Preconditions.checkArgument(jitterPercentage > 0.0);
        Preconditions.checkArgument(jitterPercentage <= 1.0);
        this.jitterPercentage = jitterPercentage;

        this.maxJitterMs = maximumBackoffMs;
    }

    public ExponentialRetryBackoffWithJitter(int backoffBase, int maximumBackoffMs, double jitterPercentage, int maxJitterMs) {
        Preconditions.checkArgument(maximumBackoffMs > 0);
        this.maximumBackoffMs = maximumBackoffMs;

        Preconditions.checkArgument(backoffBase > 0);
        this.backoffBase = backoffBase;

        Preconditions.checkArgument(jitterPercentage > 0.0);
        Preconditions.checkArgument(jitterPercentage <= 1.0);
        this.jitterPercentage = jitterPercentage;

        Preconditions.checkArgument(maxJitterMs >= 0.0);
        this.maxJitterMs = Math.min(maxJitterMs, maximumBackoffMs);
    }

    @Override
    public int getRetryBackoffTimeMs(int tryAttempt) {
        // Performing the calculation in doubles, because doing the exponentation
        // in int could result in overflow. But in case of doubles, overflow will equal
        // +Infinity. Math.min() then will properly limit it to maximumBackoffMs.
        double backoff = Math.min(maximumBackoffMs, (double) backoffBase * Math.pow(2.0, tryAttempt));
        // Calculate jitter that is percentage of current backoff.
        double jitter = Math.min(ThreadLocalRandom.current().nextDouble(jitterPercentage) * backoff, maxJitterMs);
        return (int) (backoff - jitter);
    }
}
