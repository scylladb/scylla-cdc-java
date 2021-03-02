package com.scylladb.cdc.model.worker;

import com.google.common.flogger.FluentLogger;

import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public final class TaskActionsQueue {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

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
     * Using a thread-safe queue on purpose, as
     * new TaskActions may be added from different
     * threads.
     *
     * runNextAction() is only ran inside one (Queue)
     * thread and is allowed to block.
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
            action.run().thenAccept(queue::add).exceptionally(ex -> {
                logger.atSevere().withCause(ex).log("Unhandled exception in TaskActionsQueue.");
                return null;
            });
        }
    }
}
