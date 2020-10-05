package com.scylladb.cdc.model.worker;

import java.util.concurrent.CompletableFuture;

import com.google.common.base.Preconditions;

public class TaskAndChangeConsumerAdapter implements TaskAndChangeConsumer {

    private final ChangeConsumer consumer;

    public TaskAndChangeConsumerAdapter(ChangeConsumer consumer) {
        this.consumer = Preconditions.checkNotNull(consumer);
    }

    @Override
    public CompletableFuture<Task> consume(Task task, Change change) {
        return consumer.consume(change).thenApply(ignored -> task.updateState(change.getId()));
    }

}
