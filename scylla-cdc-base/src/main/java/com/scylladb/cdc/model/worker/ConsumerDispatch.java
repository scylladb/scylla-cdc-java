package com.scylladb.cdc.model.worker;

import java.util.concurrent.CompletableFuture;

/**
 * Package private interface that comprises the actual callback signature for
 * {@link TaskAction} dispatch.
 *
 * Variants of this is created by {@link Consumer}, wrapping the user-facing
 * callback interfaces.
 *
 * @author calle
 *
 */
interface ConsumerDispatch {
    /*
     * No assumption should be made regarding the thread invoking this method
     */
    CompletableFuture<TaskState> consume(Task task, RawChange change, Task nextTask);

}
