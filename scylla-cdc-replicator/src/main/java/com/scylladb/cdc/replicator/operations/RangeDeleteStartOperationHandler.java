package com.scylladb.cdc.replicator.operations;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.replicator.Main;
import com.scylladb.cdc.replicator.ReplicatorConsumer;

public class RangeDeleteStartOperationHandler implements CdcOperationHandler {
    private final ReplicatorConsumer.RangeTombstoneState state;
    private final boolean inclusive;

    public RangeDeleteStartOperationHandler(ReplicatorConsumer.RangeTombstoneState rtState, boolean inclusive) {
        state = rtState;
        this.inclusive = inclusive;
    }

    @Override
    public Statement getStatement(RawChange c, ConsistencyLevel cl, Main.Mode m) {
        state.addStart(c, inclusive);
        return null;
    }

}
