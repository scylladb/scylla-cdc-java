package com.scylladb.cdc.lib;

import java.util.Set;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.cql.MasterCQL;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.master.Master;
import com.scylladb.cdc.transport.MasterTransport;

public final class MasterThread extends Thread {

    private final Master master;

    public MasterThread(ThreadGroup tg, MasterTransport transport, MasterCQL cql, Set<TableName> tables) {
        super(tg, "ScyllaCDCMaster");
        Preconditions.checkNotNull(transport);
        Preconditions.checkNotNull(cql);
        Preconditions.checkNotNull(tables);
        Preconditions.checkArgument(!tables.isEmpty());
        this.master = new Master(transport, cql, tables);
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
