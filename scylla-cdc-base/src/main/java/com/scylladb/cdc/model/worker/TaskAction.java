package com.scylladb.cdc.model.worker;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.cql.WorkerCQL.Reader;

public abstract class TaskAction {

    public static CompletableFuture<TaskAction> fetch(Connectors connectors, Task task) {
        return connectors.cql.createReader(task)
                .thenApply(reader -> new ConsumeChangeTaskAction(connectors, task, reader));
    }

    private static final class ConsumeChangeTaskAction extends TaskAction {

        public ConsumeChangeTaskAction(Connectors connectors, Task task, Reader reader) {
            super(connectors, task, reader);
        }

        @Override
        public CompletableFuture<TaskAction> run() {
            return consume();
        }

    }

    private static final class UpdateStatusTaskAction extends TaskAction {

        public UpdateStatusTaskAction(Connectors connectors, Task task, Reader reader) {
            super(connectors, task, reader);
        }

        @Override
        public CompletableFuture<TaskAction> run() {
            saveState();
            return consume();
        }

    }

    private static final class MoveToNextWindowTaskAction extends TaskAction {

        public MoveToNextWindowTaskAction(Connectors connectors, Task task, Reader reader) {
            super(connectors, task, reader);
        }

        @Override
        public CompletableFuture<TaskAction> run() {
            return moveToNextWindow();
        }
    }

    private final Connectors connectors;
    private final Task task;
    private final Reader reader;

    protected TaskAction(Connectors connectors, Task task, Reader reader) {
        this.connectors = Preconditions.checkNotNull(connectors);
        this.task = Preconditions.checkNotNull(task);
        this.reader = Preconditions.checkNotNull(reader);
    }

    public abstract CompletableFuture<TaskAction> run();

    private CompletableFuture<TaskAction> consumeChange(Optional<Change> change) {
        if (change.isPresent()) {
            return connectors.consumer.consume(task, change.get())
                    .thenApply(updatedTask -> new UpdateStatusTaskAction(connectors, updatedTask, reader));
        } else {
            CompletableFuture<TaskAction> result = new CompletableFuture<>();
            result.complete(new MoveToNextWindowTaskAction(connectors, task, reader));
            return result;
        }
    }

    protected CompletableFuture<TaskAction> consume() {
        return reader.nextChange().thenCompose(this::consumeChange);
    }

    protected void saveState() {
        connectors.transport.setState(task.id, task.state);
    }

    protected CompletableFuture<TaskAction> moveToNextWindow() {
        TaskState newState = task.state.moveToNextWindow();
        connectors.transport.moveStateToNextWindow(task.id, newState);
        return fetch(connectors, task.updateState(newState));
    }
}
