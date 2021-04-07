package com.scylladb.cdc.model.worker;

import java.util.concurrent.CompletableFuture;

/**
 * Opaque wrapper around any of the available consumer interfaces
 * 
 * @author calle
 *
 */
public class Consumer {
    private final TaskAndRawChangeConsumer taskAndRawChangeConsumer;

    public Consumer(RawChangeConsumer rawChangeConsumer) {
        this(new TaskAndRawChangeConsumer() {
            @Override
            public CompletableFuture<Void> consume(Task task, RawChange change) {
                return rawChangeConsumer.consume(change);
            }
        });
    }

    public Consumer(TaskAndRawChangeConsumer taskAndRawChangeConsumer) {
        this.taskAndRawChangeConsumer = taskAndRawChangeConsumer;
    }

    public static Consumer forTaskAndRawChangeConsumer(TaskAndRawChangeConsumer c) {
        return new Consumer(c);
    }

    public static Consumer forRawChangeConsumer(RawChangeConsumer c) {
        return new Consumer(c);
    }

    TaskAndRawChangeConsumer getTaskAndRawChangeConsumer() {
        return taskAndRawChangeConsumer;
    }

}
