package com.scylladb.cdc.replicator.operations.delete;

import com.datastax.driver.core.ConsistencyLevel;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.replicator.operations.CdcOperationHandler;

import java.util.concurrent.CompletableFuture;

public class RangeDeleteStartOperationHandler implements CdcOperationHandler {
    private final RangeDeleteState rangeDeleteState;
    private final boolean isInclusive;

    @Override
    public CompletableFuture<Void> handle(RawChange change, ConsistencyLevel consistencyLevel) {
        // We encountered a row range delete start, however
        // without range end we cannot perform this operation yet.
        // Therefore store it in RangeDeleteState from which
        // it will be accessed by a RangeDeleteEndOperationHandler
        // and only then the DELETE performed.
        rangeDeleteState.addStart(change, isInclusive);

        return CompletableFuture.completedFuture(null);
    }

    public RangeDeleteStartOperationHandler(RangeDeleteState rangeDeleteState, boolean isInclusive) {
        this.rangeDeleteState = rangeDeleteState;
        this.isInclusive = isInclusive;
    }
}
