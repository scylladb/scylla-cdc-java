package com.scylladb.cdc.replicator.operations;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.scylladb.cdc.model.worker.RawChange;

public interface CdcOperationHandler {
    Statement getStatement(RawChange c, ConsistencyLevel cl);
}
