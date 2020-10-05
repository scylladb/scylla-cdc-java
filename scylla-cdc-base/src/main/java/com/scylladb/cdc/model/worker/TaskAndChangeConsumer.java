package com.scylladb.cdc.model.worker;

import java.util.concurrent.CompletableFuture;

public interface TaskAndChangeConsumer {

    /*
     * No assumption should be made regarding the thread invoking this method
     */
    CompletableFuture<Task> consume(Task task, Change change);

}
