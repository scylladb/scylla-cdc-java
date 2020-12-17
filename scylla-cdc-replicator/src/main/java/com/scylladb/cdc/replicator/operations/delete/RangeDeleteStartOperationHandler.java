package com.scylladb.cdc.replicator.operations.delete;

import com.datastax.driver.core.ConsistencyLevel;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.replicator.operations.CdcOperationHandler;

import java.util.concurrent.CompletableFuture;

public class RangeDeleteStartOperationHandler implements CdcOperationHandler {
    private final RangeDeleteState state;
    private final boolean inclusive;

    public RangeDeleteStartOperationHandler(RangeDeleteState rtState, boolean inclusive) {
        state = rtState;
        this.inclusive = inclusive;
    }

    @Override
    public CompletableFuture<Void> handle(RawChange change, ConsistencyLevel cl) {
        state.addStart(change, inclusive);
        return CompletableFuture.completedFuture(null);
    }

}
