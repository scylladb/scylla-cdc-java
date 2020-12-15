package com.scylladb.cdc.replicator.operations;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.replicator.Main;

public interface CdcOperationHandler {
    Statement getStatement(RawChange c, ConsistencyLevel cl, Main.Mode m);
}
