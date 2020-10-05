package com.scylladb.cdc.lib;

import java.util.Map;
import java.util.SortedSet;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.worker.Connectors;
import com.scylladb.cdc.model.worker.Worker;

public class WorkerThread extends Thread {

    private final Worker worker;
    private final Map<TaskId, SortedSet<StreamId>> tasks;

    public WorkerThread(ThreadGroup tg, int no, Connectors connectors, Map<TaskId, SortedSet<StreamId>> tasks) {
        super(tg, "Scylla-CDC-Worker-Thread-" + no);
        worker = new Worker(connectors);
        this.tasks = Preconditions.checkNotNull(tasks);
    }

    @Override
    public void run() {
        try {
            worker.run(tasks);
        } catch (Throwable e) {
            System.err.println("Master thread failed: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }

}
