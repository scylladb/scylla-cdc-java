package com.scylladb.cdc.model.worker;

import java.util.concurrent.CompletableFuture;

public interface ChangesConsumer {

    CompletableFuture<Void> consume(Change change);

}
