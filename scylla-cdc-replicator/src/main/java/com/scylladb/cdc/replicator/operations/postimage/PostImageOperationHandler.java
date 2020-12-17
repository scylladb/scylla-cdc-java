package com.scylladb.cdc.replicator.operations.postimage;

import com.datastax.driver.core.ConsistencyLevel;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.replicator.operations.CdcOperationHandler;

import java.util.concurrent.CompletableFuture;

public class PostImageOperationHandler implements CdcOperationHandler {
    private final PostImageState state;

    public PostImageOperationHandler(PostImageState state) {
        this.state = state;
    }

    @Override
    public CompletableFuture<Void> handle(RawChange change, ConsistencyLevel cl) {
        return state.getLastOperationHandler(change).handle(change, cl);
    }
}
