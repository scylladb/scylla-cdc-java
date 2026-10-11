package com.scylladb.cdc.model.worker;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import com.google.common.flogger.FluentLogger;

/**
 * Tracks whether "noisy" exception logging should be suppressed, per key.
 *
 * <p>When a noisy exception first occurs for a given key, it is logged normally
 * and subsequent occurrences are suppressed for a configurable time window.
 * After the window elapses, the next occurrence is logged again (along with a
 * count of how many were suppressed) and a new suppression window starts.
 *
 * <p>This class is thread-safe and shared across all workers.
 */
final class NoisyExceptionSuppressor {

    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private final long suppressionWindowMs;
    private final long suppressionWindowNanos;
    private final LongSupplier nanoTime;
    private final ConcurrentHashMap<Object, SuppressionState> states = new ConcurrentHashMap<>();

    NoisyExceptionSuppressor(long suppressionWindowMs, LongSupplier nanoTime) {
        this.suppressionWindowMs = suppressionWindowMs;
        this.suppressionWindowNanos = TimeUnit.MILLISECONDS.toNanos(suppressionWindowMs);
        this.nanoTime = nanoTime;
    }

    private static final class SuppressionState {
        boolean active;
        long windowStartedNanos;
        long suppressedCount;
    }

    /**
     * Returns {@code true} if the exception should be suppressed (not logged).
     * If not suppressed, starts a new suppression window. When a window expires,
     * logs how many exceptions were suppressed during the previous window.
     *
     * @param key the suppression key (typically a table name).
     */
    boolean shouldSuppress(Object key) {
        if (suppressionWindowMs <= 0) {
            return false;
        }
        SuppressionState state = states.computeIfAbsent(key, k -> new SuppressionState());
        long suppressed;
        synchronized (state) {
            long now = nanoTime.getAsLong();
            if (state.active && now - state.windowStartedNanos < suppressionWindowNanos) {
                state.suppressedCount++;
                return true;
            }
            suppressed = state.suppressedCount;
            state.suppressedCount = 0;
            state.active = true;
            state.windowStartedNanos = now;
        }
        if (suppressed > 0) {
            logger.atWarning().log("%d noisy exception(s) were suppressed during the previous %d ms window for %s.",
                    suppressed, suppressionWindowMs, key);
        }
        return false;
    }
}
