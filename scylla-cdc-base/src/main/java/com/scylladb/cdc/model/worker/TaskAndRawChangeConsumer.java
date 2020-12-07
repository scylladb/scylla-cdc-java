package com.scylladb.cdc.model.worker;

import java.util.concurrent.CompletableFuture;

public interface TaskAndRawChangeConsumer {

    /*
     * No assumption should be made regarding the thread invoking this method
     */
    CompletableFuture<Void> consume(Task task, RawChange change);

}
