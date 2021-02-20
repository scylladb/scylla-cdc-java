package com.scylladb.cdc.model.master;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.cql.MasterCQL;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.transport.MasterTransport;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.fail;

public class MasterThread implements AutoCloseable {
    private static final long DEFAULT_SLEEP_BEFORE_FIRST_GENERATION_MS = 5;
    private static final long DEFAULT_SLEEP_BEFORE_GENERATION_DONE_MS = 5;
    private static final long DEFAULT_SLEEP_AFTER_EXCEPTION_MS = 5;

    private static final long THREAD_JOIN_TIMEOUT_MS = 3000;

    private final Thread masterThread;

    public MasterThread(Connectors connectors) {
        Preconditions.checkNotNull(connectors);
        Master master = new Master(connectors);
        this.masterThread = new Thread(master::run);
        this.masterThread.start();
    }

    public MasterThread(MasterTransport masterTransport, MasterCQL masterCQL, Set<TableName> tableNames) {
        this(new Connectors(masterTransport, masterCQL, tableNames,
                DEFAULT_SLEEP_BEFORE_FIRST_GENERATION_MS, DEFAULT_SLEEP_BEFORE_GENERATION_DONE_MS, DEFAULT_SLEEP_AFTER_EXCEPTION_MS));
    }

    @Override
    public void close() {
        if (this.masterThread != null) {
            this.masterThread.interrupt();
            try {
                this.masterThread.join(THREAD_JOIN_TIMEOUT_MS);
            } catch (InterruptedException e) {
                fail("Could not successfully join() master thread", e);
            } finally {
                if (this.masterThread.isAlive()) {
                    fail("Could not successfully close master thread");
                }
            }
        }
    }
}
