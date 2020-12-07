package com.scylladb.cdc.model.worker;

import java.util.Date;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.flogger.FluentLogger;
import com.scylladb.cdc.cql.WorkerCQL.Reader;

public abstract class TaskAction {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

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

    public static TaskAction createFirstAction(Connectors connectors, Task task) {
        return new ReadNewWindowTaskAction(connectors, task);
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
            CompletableFuture<Void> waitFuture = waitForWindow();
            CompletableFuture<Reader> readerFuture = waitFuture.thenCompose(w -> connectors.cql.createReader(task));
            return readerFuture.thenApply(reader -> (TaskAction) new UpdateStatusTaskAction(connectors, task, reader)).exceptionally(ex -> {
                // Exception occured while starting up the reader. Retry by starting
                // this TaskAction once again.
                
                // TODO - implement some backoff strategy.
                logger.atSevere().withCause(ex).log("Error while starting reading next window. Task: %s. Task state: %s. Will retry.", task.id, task.state);
                return new ReadNewWindowTaskAction(connectors, task);
            });
        }

        private CompletableFuture<Void> waitForWindow() {
            Date end = task.state.getWindowEndTimestamp().toDate();
            Date now = new Date();
            long toWait = end.getTime() - now.getTime() + TimeUnit.SECONDS.toMillis(30);
            if (toWait > 0) {
                CompletableFuture<Void> result = new CompletableFuture<>();
                internalExecutor.schedule(() -> result.complete(null), toWait, TimeUnit.MILLISECONDS);
                return result;
            }
            return CompletableFuture.completedFuture(null);
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

        private CompletableFuture<TaskAction> consumeChange(Optional<RawChange> change) {
            if (change.isPresent()) {
                Task updatedTask = task.updateState(change.get().getId());
                return connectors.consumer.consume(task, change.get())
                        .thenApply(q -> new UpdateStatusTaskAction(connectors, updatedTask, reader));
            } else {
                CompletableFuture<TaskAction> result = new CompletableFuture<>();
                result.complete(new MoveToNextWindowTaskAction(connectors, task));
                return result;
            }
        }

        @Override
        public CompletableFuture<TaskAction> run() {
            return reader.nextChange().thenCompose(this::consumeChange).exceptionally(ex -> {
                // Exception occured while reading the window, we will have to restart
                // ReadNewWindowTaskAction - read a window from state defined in task.

                // TODO - implement some backoff strategy.
                logger.atSevere().withCause(ex).log("Error while consuming change. Task: %s. Task state: %s. Will retry.", task.id, task.state);
                return new ReadNewWindowTaskAction(connectors, task);
            });
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
            return CompletableFuture.completedFuture(new ReadNewWindowTaskAction(connectors, newTask));
        }
    }
}
