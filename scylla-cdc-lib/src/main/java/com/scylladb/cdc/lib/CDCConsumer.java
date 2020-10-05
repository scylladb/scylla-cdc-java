package com.scylladb.cdc.lib;

import java.util.Set;

import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;
import com.scylladb.cdc.cql.MasterCQL;
import com.scylladb.cdc.cql.driver3.Driver3MasterCQL;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.worker.ChangeConsumer;

public final class CDCConsumer {

    private final LocalTransport transport;
    private final MasterCQL masterCQL;
    private final ThreadGroup cdcThreadGroup;
    private final Set<TableName> tables;
    private MasterThread master;

    public CDCConsumer(Session session, ChangeConsumer consumer, Set<TableName> tables, int workersCount) {
        cdcThreadGroup = new ThreadGroup("Scylla-CDC-Threads");
        Preconditions.checkNotNull(consumer);
        Preconditions.checkArgument(workersCount > 0);
        this.transport = new LocalTransport(cdcThreadGroup, session, workersCount, consumer);
        Preconditions.checkNotNull(tables);
        Preconditions.checkArgument(!tables.isEmpty());
        this.tables = tables;
        masterCQL = new Driver3MasterCQL(session);
    }

    public void start() {
        Preconditions.checkState(master == null);
        Preconditions.checkState(transport.isReadyToStart());
        master = new MasterThread(cdcThreadGroup, transport, masterCQL, tables);
        master.start();
    }

    public void stop() throws InterruptedException {
        master.finish();
        master = null;
        transport.stop();
    }

    public void reconfigure(int workersCount) throws InterruptedException {
        Preconditions.checkArgument(workersCount > 0);
        stop();
        transport.setWorkersCount(workersCount);
        start();
    }

}
