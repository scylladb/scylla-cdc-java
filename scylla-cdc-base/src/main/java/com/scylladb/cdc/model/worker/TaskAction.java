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
import com.scylladb.cdc.model.FutureUtils;

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

    private static CompletableFuture<Void> sleepOnExecutor(long numberOfMilliseconds) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        internalExecutor.schedule(() -> result.complete(null), numberOfMilliseconds, TimeUnit.MILLISECONDS);
        return result;
    }

    public abstract CompletableFuture<TaskAction> run();

    public static TaskAction createFirstAction(WorkerConfiguration workerConfiguration, Task task) {
        return new ReadNewWindowTaskAction(workerConfiguration, task, 0);
    }

    private static final class ReadNewWindowTaskAction extends TaskAction {
        private final WorkerConfiguration workerConfiguration;
        private final Task task;
        private final int tryAttempt;

        protected ReadNewWindowTaskAction(WorkerConfiguration workerConfiguration, Task task, int tryAttempt) {
            this.workerConfiguration = Preconditions.checkNotNull(workerConfiguration);
            this.task = Preconditions.checkNotNull(task);
            Preconditions.checkArgument(tryAttempt >= 0);
            this.tryAttempt = tryAttempt;
        }

        @Override
        public CompletableFuture<TaskAction> run() {
            CompletableFuture<Void> waitFuture = waitForWindow();
            CompletableFuture<Reader> readerFuture = waitFuture.thenCompose(w -> workerConfiguration.cql.createReader(task));
            CompletableFuture<TaskAction> taskActionFuture = readerFuture
                    .thenApply(reader -> new UpdateStatusTaskAction(workerConfiguration, task, reader, tryAttempt));
            return FutureUtils.thenComposeExceptionally(taskActionFuture, ex -> {
                // Exception occured while starting up the reader. Retry by starting
                // this TaskAction once again.
                long backoffTime = workerConfiguration.workerRetryBackoff.getRetryBackoffTimeMs(tryAttempt);
                logger.atSevere().withCause(ex).log("Error while starting reading next window. Task: %s. " +
                        "Task state: %s. Will retry after backoff (%d ms).", task.id, task.state, backoffTime);
                return TaskAction.sleepOnExecutor(backoffTime).thenApply(t -> new ReadNewWindowTaskAction(workerConfiguration, task, tryAttempt + 1));
            });
        }

        private CompletableFuture<Void> waitForWindow() {
            Date end = task.state.getWindowEndTimestamp().toDate();
            Date now = new Date();
            long toWait = end.getTime() - now.getTime() + workerConfiguration.confidenceWindowSizeMs;
            if (toWait > 0) {
                return sleepOnExecutor(toWait);
            }
            return CompletableFuture.completedFuture(null);
        }
    }

    private static class ConsumeChangeTaskAction extends TaskAction {
        protected final WorkerConfiguration workerConfiguration;
        protected final Task task;
        private final Reader reader;
        private final int tryAttempt;

        public ConsumeChangeTaskAction(WorkerConfiguration workerConfiguration, Task task, Reader reader, int tryAttempt) {
            this.workerConfiguration = Preconditions.checkNotNull(workerConfiguration);
            this.task = Preconditions.checkNotNull(task);
            this.reader = Preconditions.checkNotNull(reader);
            Preconditions.checkArgument(tryAttempt >= 0);
            this.tryAttempt = tryAttempt;
        }

        private CompletableFuture<TaskAction> consumeChange(Optional<RawChange> change) {
            if (change.isPresent()) {
                Task updatedTask = task.updateState(change.get().getId());
                return workerConfiguration.consumer.consume(task, change.get())
                        .thenApply(q -> new UpdateStatusTaskAction(workerConfiguration, updatedTask, reader, tryAttempt));
            } else {
                if (tryAttempt > 0) {
                    logger.atWarning().log("Successfully finished reading a window after %d tries. Task: %s. " +
                            "Task state: %s.", tryAttempt, task.id, task.state);
                }

                CompletableFuture<TaskAction> result = new CompletableFuture<>();
                result.complete(new MoveToNextWindowTaskAction(workerConfiguration, task));
                return result;
            }
        }

        @Override
        public CompletableFuture<TaskAction> run() {
            CompletableFuture<TaskAction> taskActionFuture = reader.nextChange().thenCompose(this::consumeChange);
            return FutureUtils.thenComposeExceptionally(taskActionFuture, ex -> {
                // Exception occured while reading the window, we will have to restart
                // ReadNewWindowTaskAction - read a window from state defined in task.
                long backoffTime = workerConfiguration.workerRetryBackoff.getRetryBackoffTimeMs(tryAttempt);
                logger.atSevere().withCause(ex).log("Error while consuming change. Task: %s. " +
                        "Task state: %s. Will retry after backoff (%d ms).", task.id, task.state, backoffTime);
                return TaskAction.sleepOnExecutor(backoffTime).thenApply(t -> new ReadNewWindowTaskAction(workerConfiguration, task, tryAttempt + 1));
            });
        }
    }

    private static final class UpdateStatusTaskAction extends ConsumeChangeTaskAction {

        public UpdateStatusTaskAction(WorkerConfiguration workerConfiguration, Task task, Reader reader, int tryAttempt) {
            super(workerConfiguration, task, reader, tryAttempt);
        }

        @Override
        public CompletableFuture<TaskAction> run() {
            workerConfiguration.transport.setState(task.id, task.state);
            return super.run();
        }

    }

    private static final class MoveToNextWindowTaskAction extends TaskAction {
        private final WorkerConfiguration workerConfiguration;
        private final Task task;

        public MoveToNextWindowTaskAction(WorkerConfiguration workerConfiguration, Task task) {
            this.workerConfiguration = Preconditions.checkNotNull(workerConfiguration);
            this.task = Preconditions.checkNotNull(task);
        }

        @Override
        public CompletableFuture<TaskAction> run() {
            TaskState newState = task.state.moveToNextWindow(workerConfiguration.queryTimeWindowSizeMs);
            workerConfiguration.transport.moveStateToNextWindow(task.id, newState);
            Task newTask = task.updateState(newState);
            return CompletableFuture.completedFuture(new ReadNewWindowTaskAction(workerConfiguration, newTask, 0));
        }
    }
}
