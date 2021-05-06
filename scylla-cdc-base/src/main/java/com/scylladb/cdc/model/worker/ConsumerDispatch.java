package com.scylladb.cdc.model.worker;

import java.util.concurrent.CompletableFuture;

interface ConsumerDispatch {
    /*
     * No assumption should be made regarding the thread invoking this method
     */
    CompletableFuture<TaskState> consume(Task task, RawChange change, Task nextTask);

}
