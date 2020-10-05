package com.scylladb.cdc.model.worker;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

public final class TaskActionsQueue {
    /*
     * This is a thread-safe and non-blocking queue on purpose.
     * 
     * It may be populated inside different threads that may not be allowed to
     * block.
     */
    private final ConcurrentLinkedQueue<TaskAction> queue = new ConcurrentLinkedQueue<>();

    /*
     * Asynchronously adds a result of a future into the queue.
     * 
     * This is done by adding a callback which takes the result of the future and
     * puts it into the queue.
     * 
     * IMPORTANT: The code makes no assumptions about the thread which will execute
     * the callback. It is possible that it will be a thread that should never block
     * so it is important that |actions| queue is non-blocking.
     */
    public void addWhenReady(CompletableFuture<TaskAction> future) {
        future.thenAccept(queue::add);
    }

    /*
     * Takes a single action from the queue, performs this action and puts the
     * resulting action into the queue.
     */
    public void runNextAction() {
        TaskAction action = queue.poll();
        if (action != null) {
            addWhenReady(action.run());
        }
    }
}
