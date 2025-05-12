package com.scylladb.cdc.model.worker;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.scylladb.cdc.cql.WorkerCQL;
import com.scylladb.cdc.model.ExponentialRetryBackoffWithJitter;
import com.scylladb.cdc.model.RetryBackoff;
import com.scylladb.cdc.transport.WorkerTransport;
import shaded.com.scylladb.cdc.driver3.driver.core.EndPoint;
import shaded.com.scylladb.cdc.driver3.driver.core.exceptions.BusyPoolException;
import shaded.com.scylladb.cdc.driver3.driver.core.exceptions.NoHostAvailableException;
import shaded.com.scylladb.cdc.driver3.driver.core.exceptions.OverloadedException;
import shaded.com.scylladb.cdc.driver3.driver.core.exceptions.ReadTimeoutException;

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

    public final boolean suppressNoisyExceptions;

    protected final long noisyExceptionsSuppressionWindowMs = 15000;

    private long noisyExceptionsSuppressedUntil = 0;

    protected final ImmutableSet<Class<? extends Throwable>> noisyExceptions =
        ImmutableSet.of(BusyPoolException.class, OverloadedException.class, ReadTimeoutException.class);
    
    private WorkerConfiguration(WorkerTransport transport, WorkerCQL cql, Consumer consumer, long queryTimeWindowSizeMs,
            long confidenceWindowSizeMs, RetryBackoff workerRetryBackoff, ScheduledExecutorService executorService, Clock clock, long minimalWaitForWindowMs, boolean suppressNoisyExceptions) {
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
        this.suppressNoisyExceptions = suppressNoisyExceptions;
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
     * Returns timestamp in milliseconds marking the latest moment the BusyPoolException, OverloadedException
     * and ReadTimeoutException are to be silenced.
     * @return timestamp in milliseconds.
     */
    public long getNoisyExceptionsSuppressedUntil() {
        return noisyExceptionsSuppressedUntil;
    }

    /**
     * Suppresses the logging of BusyPoolException, OverloadedException
     * and ReadTimeoutException for a given duration in milliseconds.
     * @param durationMs
     */
    public void suppressNoisyExceptions(long durationMs) {
        this.noisyExceptionsSuppressedUntil = System.currentTimeMillis() + durationMs;
    }

    public boolean isNoisyException(Throwable ex) {
        return (noisyExceptions.stream().anyMatch(v -> v.isInstance(ex)));
    }

    public boolean isNoisyExceptionInduced(Throwable ex) {
        if (isNoisyException(ex)) {
            return true;
        }

        if (ex instanceof NoHostAvailableException) {
            Map<EndPoint, Throwable> errors = ((NoHostAvailableException) ex).getErrors();
            if (errors.isEmpty()) {
                return false;
            }
            if(errors.values().stream().allMatch(this::isNoisyException)) {
                return true;
            }
        }
        return false;
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

        private Boolean suppressNoisyExceptions = false;

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

        public Builder withSuppressNoisyExceptions(boolean suppressNoisyExceptions) {
            this.suppressNoisyExceptions = suppressNoisyExceptions;
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
         * @param minimalWaitForWindowMs
         * @return
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
                    workerRetryBackoff, executorService, clock, minimalWaitForWindowMs, suppressNoisyExceptions);
        }
    }
}
