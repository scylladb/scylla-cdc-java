package com.scylladb.cdc.replicator.operations;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.scylladb.cdc.model.worker.RawChange;

import java.util.concurrent.CompletableFuture;

public abstract class ExecutingStatementHandler implements CdcOperationHandler {
    protected final Session session;

    public ExecutingStatementHandler(Session session) {
        this.session = session;
    }

    public abstract Statement getStatement(RawChange c, ConsistencyLevel cl);

    @Override
    public CompletableFuture<Void> handle(RawChange change, ConsistencyLevel cl) {
        Statement statement = getStatement(change, cl);
        return FutureUtils.convert(session.executeAsync(statement), "Handling operation: " + change.getOperationType());
    }
}
