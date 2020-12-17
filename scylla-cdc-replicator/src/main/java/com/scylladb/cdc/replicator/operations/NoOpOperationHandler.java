package com.scylladb.cdc.replicator.operations;

import com.datastax.driver.core.ConsistencyLevel;
import com.scylladb.cdc.model.worker.RawChange;

import java.util.concurrent.CompletableFuture;

public class NoOpOperationHandler implements CdcOperationHandler {
    @Override
    public CompletableFuture<Void> handle(RawChange change, ConsistencyLevel cl) {
        // Ignore the change and immediately return completed future.
        return CompletableFuture.completedFuture(null);
    }
}
