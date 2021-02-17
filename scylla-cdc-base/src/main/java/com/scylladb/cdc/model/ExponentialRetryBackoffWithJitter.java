package com.scylladb.cdc.model;

import com.google.common.base.Preconditions;

import java.util.Random;

public class ExponentialRetryBackoffWithJitter implements RetryBackoff {
    private final Random random;
    private final int maximumBackoffMs;
    private final int backoffBase;

    public ExponentialRetryBackoffWithJitter(int backoffBase, int maximumBackoffMs) {
        Preconditions.checkArgument(maximumBackoffMs > 0);
        this.maximumBackoffMs = maximumBackoffMs;

        Preconditions.checkArgument(backoffBase > 0);
        this.backoffBase = backoffBase;

        this.random = new Random();
    }

    @Override
    public int getRetryBackoffTimeMs(int tryAttempt) {
        // Performing the calculation in doubles, because doing the exponentation
        // in int could result in overflow. But in case of doubles, overflow will equal
        // +Infinity. Math.min() then will properly limit it to maximumBackoffMs.
        int backoff = (int) Math.min(maximumBackoffMs, (double) backoffBase * Math.pow(2.0, tryAttempt));
        // Add jitter.
        return random.nextInt(backoff);
    }
}
