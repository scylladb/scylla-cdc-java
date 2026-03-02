package com.scylladb.cdc.model.worker;

import java.time.Clock;
import java.time.Duration;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.base.Preconditions;
import com.google.common.flogger.FluentLogger;
import com.scylladb.cdc.cql.WorkerCQL;
import com.scylladb.cdc.model.CatchUpConfiguration;
import com.scylladb.cdc.model.ExponentialRetryBackoffWithJitter;
import com.scylladb.cdc.model.RetryBackoff;
import com.scylladb.cdc.transport.WorkerTransport;

public final class WorkerConfiguration {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
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

    public final CatchUpConfiguration catchUpConfig;

    private final ScheduledExecutorService executorService;

    private final Clock clock;

    private WorkerConfiguration(WorkerTransport transport, WorkerCQL cql, Consumer consumer, long queryTimeWindowSizeMs,
            long confidenceWindowSizeMs, RetryBackoff workerRetryBackoff, ScheduledExecutorService executorService, Clock clock, long minimalWaitForWindowMs,
            CatchUpConfiguration catchUpConfig) {
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
        this.catchUpConfig = Preconditions.checkNotNull(catchUpConfig);
    }

    public ScheduledExecutorService getExecutorService() {
        return executorService;
    }

    /**
     * Computes the catch-up cutoff date based on the current time and
     * the configured catch-up window size. Returns empty if catch-up
     * is disabled (catchUpWindowSizeSeconds == 0).
     */
    public Optional<Date> computeCatchUpCutoff() {
        return catchUpConfig.computeCatchUpCutoff(clock);
    }

    /**
     * Creates a new {@link CatchUpProber} using the current configuration.
     */
    CatchUpProber createCatchUpProber() {
        return new CatchUpProber(cql, queryTimeWindowSizeMs, catchUpConfig.getProbeTimeoutSeconds());
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

        private final CatchUpConfiguration.Builder catchUpHelper = new CatchUpConfiguration.Builder();

        private Clock clock = Clock.systemDefaultZone();

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

        /**
         * Sets the catch-up window size in seconds. When greater than zero,
         * enables catch-up optimization for first-time startup, allowing the
         * worker to skip empty windows in closed generations when the window
         * start is older than {@code now - catchUpWindowSizeSeconds}.
         *
         * @param catchUpWindowSizeSeconds the catch-up window size in seconds (0 = disabled)
         * @return this builder
         */
        public Builder withCatchUpWindowSizeSeconds(long catchUpWindowSizeSeconds) {
            catchUpHelper.setCatchUpWindowSizeSeconds(catchUpWindowSizeSeconds);
            return this;
        }

        /**
         * Sets the catch-up window size using a {@link Duration}. This is
         * equivalent to {@link #withCatchUpWindowSizeSeconds(long)} but more
         * idiomatic for Java 8+ callers. Sub-second precision is truncated.
         *
         * @param catchUpWindow the catch-up window duration (0 = disabled)
         * @return this builder
         */
        public Builder withCatchUpWindow(Duration catchUpWindow) {
            catchUpHelper.setCatchUpWindow(catchUpWindow);
            return this;
        }

        /**
         * Sets the timeout in seconds for individual catch-up probe operations.
         * Default: {@value com.scylladb.cdc.model.CatchUpConfiguration#DEFAULT_PROBE_TIMEOUT_SECONDS} seconds.
         * Maximum: ~24 days ({@link CatchUpConfiguration#MAX_PROBE_TIMEOUT_SECONDS}).
         * On slow clusters with many tombstones, probes may take longer than
         * the default. If a probe times out, the worker falls back to reading
         * from the original window start for that task.
         *
         * @param probeTimeoutSeconds the probe timeout in seconds (must be positive)
         * @return this builder
         */
        public Builder withProbeTimeoutSeconds(long probeTimeoutSeconds) {
            catchUpHelper.setProbeTimeoutSeconds(probeTimeoutSeconds);
            return this;
        }

        /**
         * Sets the timeout for individual catch-up probe operations using a
         * {@link Duration}. Equivalent to {@link #withProbeTimeoutSeconds(long)}
         * but more idiomatic for Java 8+ callers.
         *
         * @param probeTimeout the probe timeout duration (must be positive, sub-second precision is truncated)
         * @return this builder
         */
        public Builder withProbeTimeout(Duration probeTimeout) {
            Preconditions.checkNotNull(probeTimeout);
            Preconditions.checkArgument(!probeTimeout.isNegative() && !probeTimeout.isZero(),
                    "probeTimeout must be positive, got %s", probeTimeout);
            catchUpHelper.setProbeTimeoutSeconds(probeTimeout.getSeconds());
            return this;
        }

        public WorkerConfiguration build() {
            if (executorService == null) {
                executorService = Executors.newScheduledThreadPool(1);
            }
            long catchUpSeconds = catchUpHelper.getCatchUpWindowSizeSeconds();
            if (catchUpSeconds > 0 && catchUpSeconds < confidenceWindowSizeMs / 1000) {
                logger.atWarning().log("catchUpWindowSizeSeconds (%d) is less than confidenceWindowSizeMs / 1000 (%d). "
                        + "The confidence window may cause the worker to wait before reading the first window, "
                        + "partially negating the catch-up benefit. Consider increasing the catch-up window.",
                        catchUpSeconds, confidenceWindowSizeMs / 1000);
            }
            return new WorkerConfiguration(transport, cql, consumer, queryTimeWindowSizeMs, confidenceWindowSizeMs,
                    workerRetryBackoff, executorService, clock, minimalWaitForWindowMs, catchUpHelper.build());
        }
    }
}
