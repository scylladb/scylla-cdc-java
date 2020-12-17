package com.scylladb.cdc.replicator.operations;

import com.datastax.driver.core.ConsistencyLevel;
import com.scylladb.cdc.model.worker.RawChange;

import java.util.concurrent.CompletableFuture;

public interface CdcOperationHandler {
    CompletableFuture<Void> handle(RawChange change, ConsistencyLevel cl);
}
