package com.scylladb.cdc.lib;

import java.util.Set;

import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;
import com.scylladb.cdc.model.ExponentialRetryBackoffWithJitter;
import com.scylladb.cdc.model.RetryBackoff;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.worker.RawChangeConsumer;

public final class CDCConsumerBuilder {
    private static final long DEFAULT_QUERY_TIME_WINDOW_SIZE_MS = 30000;
    private static final long DEFAULT_CONFIDENCE_WINDOW_SIZE_MS = 30000;
    private static final RetryBackoff DEFAULT_WORKER_RETRY_BACKOFF =
            new ExponentialRetryBackoffWithJitter(10, 30000);

    private final Session session;
    private final RawChangeConsumerProvider consumer;
    private final Set<TableName> tables;
    private int workersCount = getDefaultWorkersCount();
    private long queryTimeWindowSizeMs = DEFAULT_QUERY_TIME_WINDOW_SIZE_MS;
    private long confidenceWindowSizeMs = DEFAULT_CONFIDENCE_WINDOW_SIZE_MS;
    private RetryBackoff workerRetryBackoff = DEFAULT_WORKER_RETRY_BACKOFF;

    private CDCConsumerBuilder(Session session, RawChangeConsumerProvider consumer, Set<TableName> tables) {
        this.consumer = Preconditions.checkNotNull(consumer);
        Preconditions.checkNotNull(tables);
        Preconditions.checkArgument(!tables.isEmpty());
        this.tables = tables;
        this.session = Preconditions.checkNotNull(session);
    }

    public static CDCConsumerBuilder builder(Session session, RawChangeConsumerProvider consumer, Set<TableName> tables) {
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
