package com.scylladb.cdc.lib;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.cql.MasterCQL;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.master.Connectors;
import com.scylladb.cdc.model.master.Master;
import com.scylladb.cdc.transport.MasterTransport;

public final class MasterThread extends Thread {
    private static final long DEFAULT_SLEEP_BEFORE_FIRST_GENERATION_MS = TimeUnit.SECONDS.toMillis(10);
    private static final long DEFAULT_SLEEP_BEFORE_GENERATION_DONE_MS = TimeUnit.SECONDS.toMillis(30);
    private static final long DEFAULT_SLEEP_AFTER_EXCEPTION_MS = TimeUnit.SECONDS.toMillis(10);

    private final Master master;

    public MasterThread(ThreadGroup tg, MasterTransport transport, MasterCQL cql, Set<TableName> tables) {
        super(tg, "ScyllaCDCMaster");
        Preconditions.checkNotNull(transport);
        Preconditions.checkNotNull(cql);
        Preconditions.checkNotNull(tables);
        Preconditions.checkArgument(!tables.isEmpty());
        Connectors connectors = new Connectors(transport, cql, tables,
                DEFAULT_SLEEP_BEFORE_FIRST_GENERATION_MS, DEFAULT_SLEEP_BEFORE_GENERATION_DONE_MS, DEFAULT_SLEEP_AFTER_EXCEPTION_MS);
        this.master = new Master(connectors);
    }

    public void finish() throws InterruptedException {
        interrupt();
        join();
    }

    @Override
    public void run() {
        try {
            master.run();
        } catch (Throwable e) {
            System.err.println("Master thread failed: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }

}
