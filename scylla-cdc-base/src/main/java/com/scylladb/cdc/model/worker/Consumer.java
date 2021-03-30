package com.scylladb.cdc.model.worker;

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * Opaque wrapper around any of the available consumer interfaces
 *
 * @author calle
 *
 */
public class Consumer {
    private final ConsumerDispatch consumerDispatch;

    private Consumer(ConsumerDispatch consumerDispatch) {
        this.consumerDispatch = consumerDispatch;
    }

    public Consumer(RawChangeConsumer rawChangeConsumer) {
        this(new TaskAndRawChangeConsumer() {
            @Override
            public CompletableFuture<Void> consume(Task task, RawChange change) {
                return rawChangeConsumer.consume(change);
            }
        });
    }

    public Consumer(TaskAndRawChangeConsumer taskAndRawChangeConsumer) {
        this(new ConsumerDispatch() {
            @Override
            public CompletableFuture<TaskState> consume(Task task, RawChange change, Task nextTask) {
                return taskAndRawChangeConsumer.consume(task, change).thenCompose(v -> completedFuture(nextTask.state));
            }
        });
    }

    public static Consumer forTaskAndRawChangeConsumer(TaskAndRawChangeConsumer c) {
        return new Consumer(c);
    }

    public static Consumer syncTaskAndRawChangeConsumer(BiConsumer<Task, RawChange> c) {
        return new Consumer(new TaskAndRawChangeConsumer() {
            @Override
            public CompletableFuture<Void> consume(Task task, RawChange change) {
                c.accept(task, change);
                return CompletableFuture.completedFuture(null);
            }
        });
    }

    public static Consumer forRawChangeConsumer(RawChangeConsumer c) {
        return new Consumer(c);
    }

    public static Consumer syncRawChangeConsumer(java.util.function.Consumer<RawChange> c) {
        return new Consumer(new RawChangeConsumer() {
            @Override
            public CompletableFuture<Void> consume(RawChange change) {
                c.accept(change);
                return CompletableFuture.completedFuture(null);
            }
        });
    }

    ConsumerDispatch getConsumerDispatch() {
        return consumerDispatch;
    }
}
