package com.scylladb.cdc.model.worker;

import java.util.concurrent.CompletableFuture;

public interface RawChangeConsumer {

    /*
     * No assumption should be made regarding the thread invoking this method
     */
    CompletableFuture<Void> consume(RawChange change);

}
