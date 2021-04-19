package com.scylladb.cdc.lib;

import java.util.Map;
import java.util.SortedSet;

import com.google.common.base.Preconditions;
import com.google.common.flogger.FluentLogger;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.worker.WorkerConfiguration;
import com.scylladb.cdc.model.worker.Worker;

class WorkerThread extends Thread {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private final Worker worker;
    private final Map<TaskId, SortedSet<StreamId>> tasks;

    public WorkerThread(ThreadGroup tg, int no, WorkerConfiguration workerConfiguration, Map<TaskId, SortedSet<StreamId>> tasks) {
        super(tg, "Scylla-CDC-Worker-Thread-" + no);
        worker = new Worker(workerConfiguration);
        this.tasks = Preconditions.checkNotNull(tasks);
    }

    @Override
    public void run() {
        try {
            worker.run(tasks);
        } catch (Throwable e) {
            logger.atSevere().withCause(e).log("Worker thread failed in run().");
        }
    }

}
