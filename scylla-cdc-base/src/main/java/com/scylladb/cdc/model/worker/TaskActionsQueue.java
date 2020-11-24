package com.scylladb.cdc.model.worker;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

public final class TaskActionsQueue {
    /*
     * This is a thread-safe and non-blocking queue on purpose.
     *
     * It may be populated inside different threads that may not be allowed to
     * block.
     */
    private final ConcurrentLinkedQueue<TaskAction> queue;

    public TaskActionsQueue(Collection<TaskAction> initialActions) {
        queue = new ConcurrentLinkedQueue<>(initialActions);
    }

    /*
     * Takes a single action from the queue, performs this action and puts the
     * resulting action into the queue.
     */
    public void runNextAction() {
        TaskAction action = queue.poll();
        if (action != null) {
            // This queue::add may be executed in any thread
            action.run().thenAccept(queue::add);
        }
    }
}
