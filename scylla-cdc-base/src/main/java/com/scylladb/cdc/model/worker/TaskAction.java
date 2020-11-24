package com.scylladb.cdc.model.worker;

import java.util.Date;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.cql.WorkerCQL.Reader;

public abstract class TaskAction {
    private static final ScheduledExecutorService internalExecutor = Executors
            .newSingleThreadScheduledExecutor(new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r);
                    t.setName("DelayingExecutorThread");
                    t.setDaemon(true);
                    return t;
                }
            });

    public abstract CompletableFuture<TaskAction> run();

    private static CompletableFuture<TaskAction> fetchOrWait(Connectors connectors, Task task,
            ReadNewWindowTaskAction action) {
        Date end = task.state.getWindowEndTimestamp().toDate();
        Date now = new Date();
        long toWait = end.getTime() - now.getTime() + TimeUnit.SECONDS.toMillis(30);
        if (toWait > 0) {
            CompletableFuture<TaskAction> result = new CompletableFuture<>();
            internalExecutor.schedule(() -> result.complete(action), toWait, TimeUnit.MILLISECONDS);
            return result;
        }
        return fetch(connectors, task);
    }

    public static TaskAction createFirstAction(Connectors connectors, Task task) {
        return new ReadNewWindowTaskAction(connectors, task);
    }

    private static CompletableFuture<TaskAction> fetch(Connectors connectors, Task task) {
        return connectors.cql.createReader(task)
                .thenApply(reader -> new ConsumeChangeTaskAction(connectors, task, reader));
    }

    private static final class ReadNewWindowTaskAction extends TaskAction {
        private final Connectors connectors;
        private final Task task;

        protected ReadNewWindowTaskAction(Connectors connectors, Task task) {
            this.connectors = Preconditions.checkNotNull(connectors);
            this.task = Preconditions.checkNotNull(task);
        }

        @Override
        public CompletableFuture<TaskAction> run() {
            return fetchOrWait(connectors, task, this);
        }

    }

    private static class ConsumeChangeTaskAction extends TaskAction {
        protected final Connectors connectors;
        protected final Task task;
        private final Reader reader;

        public ConsumeChangeTaskAction(Connectors connectors, Task task, Reader reader) {
            this.connectors = Preconditions.checkNotNull(connectors);
            this.task = Preconditions.checkNotNull(task);
            this.reader = Preconditions.checkNotNull(reader);
        }

        private CompletableFuture<TaskAction> consumeChange(Optional<Change> change) {
            if (change.isPresent()) {
                return connectors.consumer.consume(task, change.get())
                        .thenApply(updatedTask -> new UpdateStatusTaskAction(connectors, updatedTask, reader));
            } else {
                CompletableFuture<TaskAction> result = new CompletableFuture<>();
                result.complete(new MoveToNextWindowTaskAction(connectors, task));
                return result;
            }
        }

        @Override
        public CompletableFuture<TaskAction> run() {
            return reader.nextChange().thenCompose(this::consumeChange);
        }

    }

    private static final class UpdateStatusTaskAction extends ConsumeChangeTaskAction {

        public UpdateStatusTaskAction(Connectors connectors, Task task, Reader reader) {
            super(connectors, task, reader);
        }

        @Override
        public CompletableFuture<TaskAction> run() {
            connectors.transport.setState(task.id, task.state);
            return super.run();
        }

    }

    private static final class MoveToNextWindowTaskAction extends TaskAction {
        private final Connectors connectors;
        private final Task task;

        public MoveToNextWindowTaskAction(Connectors connectors, Task task) {
            this.connectors = Preconditions.checkNotNull(connectors);
            this.task = Preconditions.checkNotNull(task);
        }

        @Override
        public CompletableFuture<TaskAction> run() {
            TaskState newState = task.state.moveToNextWindow();
            connectors.transport.moveStateToNextWindow(task.id, newState);
            Task newTask = task.updateState(newState);
            return fetchOrWait(connectors, newTask, new ReadNewWindowTaskAction(connectors, newTask));
        }
    }
}
