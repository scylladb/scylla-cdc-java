package com.scylladb.cdc.model.worker;

import java.util.concurrent.CompletableFuture;

public interface BatchSequenceConsumer {
    /*
     * Unlike raw change consumption, this will buffer and package change rows
     * into full Event:s, i.e. the set of CDC rows comprising a single modification.
     * 
     * No assumption should be made regarding the thread invoking this method
     */
    CompletableFuture<Void> consume(BatchSequence event);
}
