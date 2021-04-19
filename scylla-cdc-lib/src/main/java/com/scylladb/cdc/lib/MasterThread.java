package com.scylladb.cdc.lib;

import java.util.Optional;

import com.google.common.base.Preconditions;
import com.google.common.flogger.FluentLogger;
import com.scylladb.cdc.model.master.Master;
import com.scylladb.cdc.model.master.MasterConfiguration;

final class MasterThread extends Thread {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private final Master master;

    public MasterThread(ThreadGroup tg, MasterConfiguration masterConfiguration) {
        super(tg, "ScyllaCDCMaster");
        Preconditions.checkNotNull(masterConfiguration);
        this.master = new Master(masterConfiguration);
    }

    public Optional<Throwable> validate() {
        return this.master.validate();
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
            logger.atSevere().withCause(e).log("Master thread failed in run().");
        }
    }
}
