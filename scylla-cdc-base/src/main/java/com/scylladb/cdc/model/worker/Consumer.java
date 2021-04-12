package com.scylladb.cdc.model.worker;

import static java.util.Collections.singletonList;
import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
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
            public CompletableFuture<TaskState> consume(Task task, RawChange change) {
                return taskAndRawChangeConsumer.consume(task, change).thenCompose(v -> completedFuture(task.state));
            }
        });
    }

    public Consumer(BatchSequenceConsumer batchSequenceConsumer) {
        this(new ConsumerDispatch() {
            private final ConcurrentHashMap<ChangeId, List<RawChange>> buffer = new ConcurrentHashMap<>();

            @Override
            public CompletableFuture<TaskState> consume(Task task, RawChange change) {
                if (change.isEndOfBatch()) {
                    List<RawChange> changes = buffer.remove(change.getId());
                    if (changes == null) {
                        changes = singletonList(change);
                    } else {
                        changes.add(change);
                    }
                    batchSequenceConsumer.consume(new BatchSequence(changes));
                    return completedFuture(task.state);
                }
                // different change id:s can come in different threads, but
                // every individual change id row set should be delivered sequentially,
                // because future chaining.
                buffer.computeIfAbsent(change.getId(), (id) -> new ArrayList<>()).add(change);
                return completedFuture(null);
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

    public static Consumer syncBatchSequenceConsumer(java.util.function.Consumer<BatchSequence> c) {
        return new Consumer(new BatchSequenceConsumer() {
            @Override
            public CompletableFuture<Void> consume(BatchSequence event) {
                c.accept(event);
                return CompletableFuture.completedFuture(null);
            }
        });
    }

    public static Consumer forBatchSequenceConsumer(BatchSequenceConsumer c) {
        return new Consumer(c);
    }

    ConsumerDispatch getConsumerDispatch() {
        return consumerDispatch;
    }

}
