package com.scylladb.cdc.model.worker;

import com.google.common.flogger.FluentLogger;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

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

    private static final int JOIN_TIMEOUT_MS = 10 * 1000;
    private static final int JOIN_POLL_MS = 100;

    /*
     * Using a thread-safe queue on purpose, as
     * new TaskActions may be added from different
     * threads.
     *
     * runNextAction() is only ran inside one (Queue)
     * thread and is allowed to block.
     */
    private final LinkedBlockingQueue<TaskAction> queue;
    private final Set<CompletableFuture<Void>> runningFutures;

    public TaskActionsQueue(Collection<TaskAction> initialActions) {
        queue = new LinkedBlockingQueue<>(initialActions);
        runningFutures = Collections.newSetFromMap(new ConcurrentHashMap<>());
    }

    /*
     * Takes a single action from the queue, performs this action and puts the
     * resulting action into the queue.
     */
    public void runNextAction() throws InterruptedException {
        TaskAction action = queue.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        if (action != null) {
            CompletableFuture<Void> actionRunFuture = action.run().thenAccept(queue::add).exceptionally(ex -> {
                logger.atSevere().withCause(ex).log("Unhandled exception in TaskActionsQueue.");
                return null;
            });
            runningFutures.add(actionRunFuture);
            actionRunFuture.thenApply((ignored) -> {
                runningFutures.remove(actionRunFuture);
                return null;
            });
        }
    }

    public void join() {
        long joinStartTime = System.currentTimeMillis();
        long maximumJoinEndTime = joinStartTime + JOIN_TIMEOUT_MS;

        // Awaiting for runningFutures to get empty, meaning
        // all futures gracefully finished.
        while (System.currentTimeMillis() < maximumJoinEndTime) {
            if (runningFutures.isEmpty()) {
                break;
            }

            try {
                Thread.sleep(JOIN_POLL_MS);
            } catch (InterruptedException e) {
                // We have been interrupted. Exit immediately from
                // the while loop!
                break;
            }
        }

        // Some futures might have not finished...
        if (!runningFutures.isEmpty()) {
            logger.atWarning().log("Could not gracefully finish all started tasks in Worker's TaskActionsQueue " +
                    "(%d futures still left)", runningFutures.size());

            // Cancel them!
            for (CompletableFuture<Void> future : runningFutures) {
                future.cancel(true);
            }
        }
    }
}
