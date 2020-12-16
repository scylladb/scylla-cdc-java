package com.scylladb.cdc.replicator.operations;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.scylladb.cdc.model.worker.RawChange;

public class RangeDeleteStartOperationHandler implements CdcOperationHandler {
    private final RangeDeleteState state;
    private final boolean inclusive;

    public RangeDeleteStartOperationHandler(RangeDeleteState rtState, boolean inclusive) {
        state = rtState;
        this.inclusive = inclusive;
    }

    @Override
    public Statement getStatement(RawChange c, ConsistencyLevel cl) {
        state.addStart(c, inclusive);
        return null;
    }

}
