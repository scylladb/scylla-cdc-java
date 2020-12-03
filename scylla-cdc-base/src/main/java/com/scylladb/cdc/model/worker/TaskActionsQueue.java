package com.scylladb.cdc.model.worker;

import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public final class TaskActionsQueue {
    /*
     * Inside runNextAction() we wait this number of
     * milliseconds for next action to appear inside the queue.
     *
     * Doing this instead of blocking without timeout prevents
     * the caller of runNextAction() to block infinitely
     * and allows it to check every POLL_TIMEOUT_MS ms
     * if it should stop.
     */
    private static final int POLL_TIMEOUT_MS = 50;

    /*
     * Using a thread-safe queue on purpose.
     *
     * However, it may be populated inside different threads that
     * may not be allowed to block. (Why not allowed to block?
     * Are they allowed to wait?). Current solution for add() is
     * unfortunately blocking.
     *
     * runNextAction() is only ran inside one thread and is allowed
     * to block.
     */
    private final LinkedBlockingQueue<TaskAction> queue;

    public TaskActionsQueue(Collection<TaskAction> initialActions) {
        queue = new LinkedBlockingQueue<>(initialActions);
    }

    /*
     * Takes a single action from the queue, performs this action and puts the
     * resulting action into the queue.
     */
    public void runNextAction() throws InterruptedException {
        TaskAction action = queue.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        if (action != null) {
            // This queue::add may be executed in any thread
            // add() unfortunately is blocking
            action.run().thenAccept(queue::add);
        }
    }
}
