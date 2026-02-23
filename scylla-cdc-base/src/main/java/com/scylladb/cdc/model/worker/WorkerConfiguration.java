package com.scylladb.cdc.model.worker;

import java.time.Clock;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.cql.WorkerCQL;
import com.scylladb.cdc.model.ExponentialRetryBackoffWithJitter;
import com.scylladb.cdc.model.RetryBackoff;
import com.scylladb.cdc.transport.WorkerTransport;

public final class WorkerConfiguration {
    public static final long DEFAULT_QUERY_TIME_WINDOW_SIZE_MS = 30000;
    public static final long DEFAULT_CONFIDENCE_WINDOW_SIZE_MS = 30000;
    public static final long DEFAULT_MINIMAL_WAIT_FOR_WINDOW_MS = 0;
    public static final RetryBackoff DEFAULT_WORKER_RETRY_BACKOFF =
            new ExponentialRetryBackoffWithJitter(50, 30000, 0.20);

    public final WorkerTransport transport;
    public final WorkerCQL cql;
    public final Consumer consumer;

    public final long queryTimeWindowSizeMs;
    public final long confidenceWindowSizeMs;
    public final long minimalWaitForWindowMs;

    public final RetryBackoff workerRetryBackoff;

    private final ScheduledExecutorService executorService;

    private final Clock clock;

    public final long noisyExceptionsSuppressionWindowMs;

    private AtomicLong noisyExceptionsSuppressedUntil = new AtomicLong(0);
    
    private WorkerConfiguration(WorkerTransport transport, WorkerCQL cql, Consumer consumer, long queryTimeWindowSizeMs,
            long confidenceWindowSizeMs, RetryBackoff workerRetryBackoff, ScheduledExecutorService executorService, Clock clock, long minimalWaitForWindowMs, long noisyExceptionsSuppressionWindowMs) {
        this.transport = Preconditions.checkNotNull(transport);
        this.cql = Preconditions.checkNotNull(cql);
        this.consumer = Preconditions.checkNotNull(consumer);
        Preconditions.checkArgument(queryTimeWindowSizeMs > 0);
        this.queryTimeWindowSizeMs = queryTimeWindowSizeMs;
        Preconditions.checkArgument(confidenceWindowSizeMs > 0);
        this.confidenceWindowSizeMs = confidenceWindowSizeMs;
        this.workerRetryBackoff = Preconditions.checkNotNull(workerRetryBackoff);
        this.executorService = executorService;
        this.clock = Preconditions.checkNotNull(clock);
        this.minimalWaitForWindowMs = minimalWaitForWindowMs;
        this.noisyExceptionsSuppressionWindowMs = noisyExceptionsSuppressionWindowMs;
    }
    
    public ScheduledExecutorService getExecutorService() {
        return executorService;
    }

    /**
     * Returns the configured <code>Clock</code> for the worker.
     * This clock will be used by the worker to get the current time,
     * for example to determine if it is safe to read the next window.
     *
     * @return the configured clock.
     */
    public Clock getClock() {
        return clock;
    }

    /**
     * Whether the feature of noisy exception suppression is enabled.
     * @return boolean value.
     */
    public boolean isNoisyExceptionSuppressionEnabled() {
        return this.noisyExceptionsSuppressionWindowMs > 0;
    }

    /**
     * Checks whether noisy exceptions should be currently suppressed.
     * @return boolean value.
     */
    public boolean areNoisyExceptionsCurrentlySuppressed() {
        if (!isNoisyExceptionSuppressionEnabled()) return false;
        return System.currentTimeMillis() < noisyExceptionsSuppressedUntil.get();
    }

    /**
     * Suppresses the logging of noisy exceptions for {@code durationMs} from the current
     * system time. Reapplying with lower duration will not shorten previous applications.
     * Concurrent calls are equivalent to applying the highest value.
     */
    public void suppressNoisyExceptions(long durationMs) {
        noisyExceptionsSuppressedUntil.getAndUpdate(current -> Math.max(current, System.currentTimeMillis() + durationMs));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private WorkerTransport transport;
        private WorkerCQL cql;
        private Consumer consumer;
        private ScheduledExecutorService executorService;

        private long queryTimeWindowSizeMs = DEFAULT_QUERY_TIME_WINDOW_SIZE_MS;
        private long confidenceWindowSizeMs = DEFAULT_CONFIDENCE_WINDOW_SIZE_MS;

        private long minimalWaitForWindowMs = DEFAULT_MINIMAL_WAIT_FOR_WINDOW_MS;

        private RetryBackoff workerRetryBackoff = DEFAULT_WORKER_RETRY_BACKOFF;

        private Clock clock = Clock.systemDefaultZone();

        private long noisyExceptionsSuppressionWindowMs = 0;

        public Builder withTransport(WorkerTransport transport) {
            this.transport = Preconditions.checkNotNull(transport);
            return this;
        }

        public Builder withCQL(WorkerCQL cql) {
            this.cql = Preconditions.checkNotNull(cql);
            return this;
        }

        public Builder withConsumer(Consumer consumer) {
            this.consumer = consumer;
            return this;
        }

        public Builder withConsumer(TaskAndRawChangeConsumer consumer) {
            return withTaskAndRawChangeConsumer(consumer);
        }

        public Builder withTaskAndRawChangeConsumer(TaskAndRawChangeConsumer consumer) {
            return withConsumer(Consumer.forTaskAndRawChangeConsumer(Preconditions.checkNotNull(consumer)));
        }

        public Builder withConsumer(RawChangeConsumer consumer) {
            return withRawChangeConsumer(consumer);
        }

        public Builder withRawChangeConsumer(RawChangeConsumer consumer) {
            return withConsumer(Consumer.forRawChangeConsumer(Preconditions.checkNotNull(consumer)));
        }

        /**
         * Sets the size of the query time window. Smaller size means smaller queries,
         * but more queries will be sent to cover the same time period.
         * @param queryTimeWindowSizeMs the size of the query time window in milliseconds
         * @return this builder
         */
        public Builder withQueryTimeWindowSizeMs(long queryTimeWindowSizeMs) {
            Preconditions.checkArgument(queryTimeWindowSizeMs > 0);
            this.queryTimeWindowSizeMs = queryTimeWindowSizeMs;
            return this;
        }

        public Builder withConfidenceWindowSizeMs(long confidenceWindowSizeMs) {
            Preconditions.checkArgument(confidenceWindowSizeMs > 0);
            this.confidenceWindowSizeMs = confidenceWindowSizeMs;
            return this;
        }

        public Builder withWorkerRetryBackoff(RetryBackoff workerRetryBackoff) {
            this.workerRetryBackoff = Preconditions.checkNotNull(workerRetryBackoff);
            return this;
        }
        
        public Builder withExecutorService(ScheduledExecutorService executorService) {
            this.executorService = executorService;
            return this;
        }

        /**
         * Sets the default time window for suppression of {@link com.scylladb.cdc.cql.NoisyCQLExceptionWrapper}
         * exceptions. Whenever an exception of this type appears it will be logged then subsequent calls will
         * be silenced until the window time elapses.
         * @param noisyExceptionsSuppressionWindowMs default window time in ms.
         * @return reference to this builder.
         */
        public Builder withNoisyExceptionsSuppressionWindowMs(long noisyExceptionsSuppressionWindowMs) {
            this.noisyExceptionsSuppressionWindowMs = noisyExceptionsSuppressionWindowMs;
            return this;
        }

        /**
         * Sets the <code>Clock</code> in the configuration.
         * <p>
         * This clock will be used by the worker to get the
         * current time, for example to determine if it is
         * safe to read the next window.
         * <p>
         * By default a system clock with default time-zone
         * is used.
         *
         * @param clock the clock to set.
         * @return reference to this builder.
         */
        public WorkerConfiguration.Builder withClock(Clock clock) {
            this.clock = Preconditions.checkNotNull(clock);
            return this;
        }

        /**
         * Sets the minimal wait time between read windows.
         * <p>
         * Can be used as a simple way to throttle worker.
         * @param minimalWaitForWindowMs the minimal wait time in milliseconds
         * @return this builder
         */
        public Builder withMinimalWaitForWindowMs(long minimalWaitForWindowMs) {
            Preconditions.checkArgument(minimalWaitForWindowMs >= 0);
            this.minimalWaitForWindowMs = minimalWaitForWindowMs;
            return this;
        }

        public WorkerConfiguration build() {
            if (executorService == null) {
                executorService = Executors.newScheduledThreadPool(1);
            }
            return new WorkerConfiguration(transport, cql, consumer, queryTimeWindowSizeMs, confidenceWindowSizeMs,
                    workerRetryBackoff, executorService, clock, minimalWaitForWindowMs, noisyExceptionsSuppressionWindowMs);
        }
    }
}
