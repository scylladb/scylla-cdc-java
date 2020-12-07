package com.scylladb.cdc.model.worker;

import java.util.concurrent.CompletableFuture;

import com.google.common.base.Preconditions;

public class TaskAndRawChangeConsumerAdapter implements TaskAndRawChangeConsumer {

    private final RawChangeConsumer consumer;

    public TaskAndRawChangeConsumerAdapter(RawChangeConsumer consumer) {
        this.consumer = Preconditions.checkNotNull(consumer);
    }

    @Override
    public CompletableFuture<Task> consume(Task task, RawChange change) {
        return consumer.consume(change).thenApply(ignored -> task.updateState(change.getId()));
    }

}
