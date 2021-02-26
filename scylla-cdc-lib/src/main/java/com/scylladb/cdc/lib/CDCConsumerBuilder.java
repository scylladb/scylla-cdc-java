package com.scylladb.cdc.lib;

import java.util.Set;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.cql.driver3.Driver3Session;
import com.scylladb.cdc.model.ExponentialRetryBackoffWithJitter;
import com.scylladb.cdc.model.RetryBackoff;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.worker.RawChangeConsumer;
import com.scylladb.cdc.model.worker.WorkerConfiguration;

public final class CDCConsumerBuilder {
    private final Driver3Session session;
    private final RawChangeConsumerProvider consumer;
    private final Set<TableName> tables;
    private int workersCount = getDefaultWorkersCount();
    private long queryTimeWindowSizeMs = WorkerConfiguration.DEFAULT_QUERY_TIME_WINDOW_SIZE_MS;
    private long confidenceWindowSizeMs = WorkerConfiguration.DEFAULT_CONFIDENCE_WINDOW_SIZE_MS;
    private RetryBackoff workerRetryBackoff = WorkerConfiguration.DEFAULT_WORKER_RETRY_BACKOFF;

    private CDCConsumerBuilder(Driver3Session session, RawChangeConsumerProvider consumer, Set<TableName> tables) {
        this.consumer = Preconditions.checkNotNull(consumer);
        Preconditions.checkNotNull(tables);
        Preconditions.checkArgument(!tables.isEmpty());
        this.tables = tables;
        this.session = Preconditions.checkNotNull(session);
    }

    public static CDCConsumerBuilder builder(Driver3Session session, RawChangeConsumerProvider consumer, Set<TableName> tables) {
        return new CDCConsumerBuilder(session, consumer, tables);
    }

    public CDCConsumerBuilder workersCount(int count) {
        Preconditions.checkArgument(count > 0);
        workersCount = count;
        return this;
    }

    public CDCConsumerBuilder queryTimeWindowSizeMs(long windowSizeMs) {
        Preconditions.checkArgument(windowSizeMs >= 0);
        queryTimeWindowSizeMs = windowSizeMs;
        return this;
    }

    public CDCConsumerBuilder confidenceWindowSizeMs(long windowSizeMs) {
        Preconditions.checkArgument(windowSizeMs >= 0);
        confidenceWindowSizeMs = windowSizeMs;
        return this;
    }

    public CDCConsumerBuilder workerRetryBackoff(RetryBackoff retryBackoff) {
        workerRetryBackoff = Preconditions.checkNotNull(retryBackoff);
        return this;
    }

    public CDCConsumer build() {
        return new CDCConsumer(session, consumer, tables, workersCount,
                queryTimeWindowSizeMs, confidenceWindowSizeMs, workerRetryBackoff);
    }

    private static int getDefaultWorkersCount() {
        int result = Runtime.getRuntime().availableProcessors() - 1;
        return result > 0 ? result : 1;
    }

}
