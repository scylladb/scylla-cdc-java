package com.scylladb.cdc.replicator.operations.postimage;

import com.datastax.driver.core.ConsistencyLevel;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.replicator.operations.CdcOperationHandler;

import java.util.concurrent.CompletableFuture;

public class PostImageInsertUpdateOperationHandler implements CdcOperationHandler {
    private final PostImageState state;

    public PostImageInsertUpdateOperationHandler(PostImageState state) {
        this.state = state;
    }

    @Override
    public CompletableFuture<Void> handle(RawChange change, ConsistencyLevel cl) {
        if (change.getOperationType() == RawChange.OperationType.ROW_INSERT) {
            state.addInsertOperation(change);
        } else if (change.getOperationType() == RawChange.OperationType.ROW_UPDATE) {
            state.addUpdateOperation(change);
        } else {
            throw new IllegalStateException("Unsupported operation type in post-image handler: " + change.getOperationType());
        }
        return CompletableFuture.completedFuture(null);
    }
}
