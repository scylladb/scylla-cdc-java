package com.scylladb.cdc.model.worker;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.cql.WorkerCQL;
import com.scylladb.cdc.model.RetryBackoff;
import com.scylladb.cdc.transport.WorkerTransport;

public final class Connectors {
    public final WorkerTransport transport;
    public final WorkerCQL cql;
    public final TaskAndRawChangeConsumer consumer;

    public final long queryTimeWindowSizeMs;
    public final long confidenceWindowSizeMs;

    public RetryBackoff workerRetryBackoff;

    public Connectors(WorkerTransport transport, WorkerCQL cql, TaskAndRawChangeConsumer consumer,
                      long queryTimeWindowSizeMs, long confidenceWindowSizeMs, RetryBackoff workerRetryBackoff) {
        this.transport = Preconditions.checkNotNull(transport);
        this.cql = Preconditions.checkNotNull(cql);
        this.consumer = Preconditions.checkNotNull(consumer);
        Preconditions.checkArgument(queryTimeWindowSizeMs > 0);
        this.queryTimeWindowSizeMs = queryTimeWindowSizeMs;
        Preconditions.checkArgument(confidenceWindowSizeMs > 0);
        this.confidenceWindowSizeMs = confidenceWindowSizeMs;
        this.workerRetryBackoff = Preconditions.checkNotNull(workerRetryBackoff);
    }
}
