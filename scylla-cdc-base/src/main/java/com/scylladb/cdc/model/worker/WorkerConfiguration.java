package com.scylladb.cdc.model.worker;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.cql.WorkerCQL;
import com.scylladb.cdc.model.ExponentialRetryBackoffWithJitter;
import com.scylladb.cdc.model.RetryBackoff;
import com.scylladb.cdc.transport.WorkerTransport;

public final class WorkerConfiguration {
    public static final long DEFAULT_QUERY_TIME_WINDOW_SIZE_MS = 30000;
    public static final long DEFAULT_CONFIDENCE_WINDOW_SIZE_MS = 30000;
    public static final RetryBackoff DEFAULT_WORKER_RETRY_BACKOFF =
            new ExponentialRetryBackoffWithJitter(10, 30000);

    public final WorkerTransport transport;
    public final WorkerCQL cql;
    public final Consumer consumer;

    public final long queryTimeWindowSizeMs;
    public final long confidenceWindowSizeMs;

    public final RetryBackoff workerRetryBackoff;

    public final DelayedFutureService delayedFutureService;

    private final ScheduledExecutorService executorService;
    
    private WorkerConfiguration(WorkerTransport transport, WorkerCQL cql, Consumer consumer,
                               long queryTimeWindowSizeMs, long confidenceWindowSizeMs, RetryBackoff workerRetryBackoff,
                                DelayedFutureService delayedFutureService, ScheduledExecutorService executorService) {
        this.transport = Preconditions.checkNotNull(transport);
        this.cql = Preconditions.checkNotNull(cql);
        this.consumer = Preconditions.checkNotNull(consumer);
        Preconditions.checkArgument(queryTimeWindowSizeMs > 0);
        this.queryTimeWindowSizeMs = queryTimeWindowSizeMs;
        Preconditions.checkArgument(confidenceWindowSizeMs > 0);
        this.confidenceWindowSizeMs = confidenceWindowSizeMs;
        this.workerRetryBackoff = Preconditions.checkNotNull(workerRetryBackoff);
        this.delayedFutureService = Preconditions.checkNotNull(delayedFutureService);
        this.executorService = executorService;
    }
    
    public ScheduledExecutorService getExecutorService() {
        return executorService;
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

        private RetryBackoff workerRetryBackoff = DEFAULT_WORKER_RETRY_BACKOFF;

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

        public WorkerConfiguration build() {
            if (executorService == null) {
                executorService = Executors.newScheduledThreadPool(1);
            }
            return new WorkerConfiguration(transport, cql, consumer,
                    queryTimeWindowSizeMs, confidenceWindowSizeMs, workerRetryBackoff, new DelayedFutureService(), executorService);
        }
    }
}
