package com.scylladb.cdc.model.worker;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.cql.WorkerCQL;
import com.scylladb.cdc.transport.WorkerTransport;

public final class Connectors {
    public final WorkerTransport transport;
    public final WorkerCQL cql;
    public final ChangesConsumer consumer;
    
    public Connectors(WorkerTransport transport, WorkerCQL cql, ChangesConsumer consumer) {
        this.transport = Preconditions.checkNotNull(transport);
        this.cql = Preconditions.checkNotNull(cql);
        this.consumer = Preconditions.checkNotNull(consumer);
    }
}
