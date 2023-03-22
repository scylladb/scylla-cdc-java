package com.scylladb.cdc.model.worker;

import java.util.Date;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.flogger.FluentLogger;
import com.scylladb.cdc.cql.WorkerCQL.Reader;
import com.scylladb.cdc.model.FutureUtils;

abstract class TaskAction {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
    protected final WorkerConfiguration workerConfiguration;
    protected final Task task;

    protected TaskAction(WorkerConfiguration workerConfiguration, Task task) {
        this.workerConfiguration = Preconditions.checkNotNull(workerConfiguration);
        this.task = Preconditions.checkNotNull(task);
    }

    protected CompletableFuture<Void> delay(long millis) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        this.workerConfiguration.getExecutorService().schedule(() -> {
            result.complete(null);
        }, millis, TimeUnit.MILLISECONDS);
        return result;
    }

    public abstract CompletableFuture<TaskAction> run();

    public static TaskAction createFirstAction(WorkerConfiguration workerConfiguration, Task task) {
        return new ReadNewWindowTaskAction(workerConfiguration, task, 0);
    }

    private static final class ReadNewWindowTaskAction extends TaskAction {
        private final int tryAttempt;

        protected ReadNewWindowTaskAction(WorkerConfiguration workerConfiguration, Task task, int tryAttempt) {
            super(workerConfiguration, task);
            Preconditions.checkArgument(tryAttempt >= 0);
            this.tryAttempt = tryAttempt;
        }

        private CompletableFuture<TaskAction> onException(Throwable ex) {
            // Exception occured while starting up the reader. Retry by starting
            // this TaskAction once again.
            long backoffTime = workerConfiguration.workerRetryBackoff.getRetryBackoffTimeMs(tryAttempt);
            logger.atSevere().withCause(ex).log("Error while starting reading next window. Task: %s. " +
                    "Task state: %s. Will retry after backoff (%d ms).", task.id, task.state, backoffTime);
            return delay(backoffTime)
                    .thenApply(t -> new ReadNewWindowTaskAction(workerConfiguration, task, tryAttempt + 1));
        }

        @Override
        public CompletableFuture<TaskAction> run() {
            // Wait future might end prematurely - when transport
            // requested stop. That could mean we create a reader
            // for a window intersecting with confidence window.
            // (reading too fresh data).
            //
            // Fortunately, to consume a change we queue
            // ReadNewWindowTaskAction, but as transport requested
            // stop, new TaskActions are not started and changes
            // from that "incorrect" window will not be consumed.
            try {
                CompletableFuture<Void> waitFuture = waitForWindow();

                CompletableFuture<Reader> readerFuture = waitFuture
                        .thenCompose(w -> workerConfiguration.cql.createReader(task));
                CompletableFuture<TaskAction> taskActionFuture = readerFuture.thenApply(
                        reader -> new ReadChangeTaskAction(workerConfiguration, task, reader, tryAttempt, task.state));
                return FutureUtils.thenComposeExceptionally(taskActionFuture, this::onException);
            } catch (Throwable ex) {
                return onException(ex);
            }
        }

        private CompletableFuture<Void> waitForWindow() {
            Date end = task.state.getWindowEndTimestamp().toDate();
            Date now = Date.from(workerConfiguration.getClock().instant());
            long toWait = end.getTime() - now.getTime() + workerConfiguration.confidenceWindowSizeMs;
            toWait = Long.max(toWait, workerConfiguration.minimalWaitForWindowMs);
            if (toWait > 0) {
                return delay(toWait);
            }
            return CompletableFuture.completedFuture(null);
        }
    }

    private static class ReadChangeTaskAction extends TaskAction {
        private final Reader reader;
        private final int tryAttempt;
        private final TaskState newState;

        public ReadChangeTaskAction(WorkerConfiguration workerConfiguration, Task task, Reader reader, int tryAttempt, TaskState newState) {
            super(workerConfiguration, task);
            this.reader = Preconditions.checkNotNull(reader);
            Preconditions.checkArgument(tryAttempt >= 0);
            this.tryAttempt = tryAttempt;
            this.newState = newState;
        }

        private CompletableFuture<TaskAction> onException(Throwable ex) {
            // Exception occured while reading the window, we will have to restart
            // ReadNewWindowTaskAction - read a window from state defined in task.
            long backoffTime = workerConfiguration.workerRetryBackoff.getRetryBackoffTimeMs(tryAttempt);
            logger.atSevere().withCause(ex).log("Error while reading a CDC change. Task: %s. " +
                    "Task state: %s. Will retry after backoff (%d ms).", task.id, task.state, backoffTime);
            return delay(backoffTime)
                    .thenApply(t -> new ReadNewWindowTaskAction(workerConfiguration, task, tryAttempt + 1));
        }

        @Override
        public CompletableFuture<TaskAction> run() {
            if (newState != null) {
                workerConfiguration.transport.setState(task.id, newState);
            }
            try {
                CompletableFuture<TaskAction> taskActionFuture = reader.nextChange().
                        thenApply(change -> new ConsumeChangeTaskAction(workerConfiguration, task, reader, change, tryAttempt));
                return FutureUtils.thenComposeExceptionally(taskActionFuture, this::onException);
            } catch (Throwable ex) {
                return onException(ex);
            }
        }
    }

    private static final class ConsumeChangeTaskAction extends TaskAction {
        private final Reader reader;
        private final Optional<RawChange> change;
        private final int tryAttempt;

        public ConsumeChangeTaskAction(WorkerConfiguration workerConfiguration, Task task, Reader reader, Optional<RawChange> change, int tryAttempt) {
            super(workerConfiguration, task);
            this.reader = Preconditions.checkNotNull(reader);
            this.change = Preconditions.checkNotNull(change);
            Preconditions.checkArgument(tryAttempt >= 0);
            this.tryAttempt = tryAttempt;
        }

        private CompletableFuture<TaskAction> onException(Throwable ex) {
            // Exception occured while consuming the change, we will have to restart
            // ReadNewWindowTaskAction - read a window from state defined in task.
            long backoffTime = workerConfiguration.workerRetryBackoff.getRetryBackoffTimeMs(tryAttempt);
            logger.atSevere().withCause(ex).log("Error while executing consume() method provided to the library. Task: %s. " +
                    "Task state: %s. Will retry after backoff (%d ms).", task.id, task.state, backoffTime);
            return delay(backoffTime)
                    .thenApply(t -> new ReadNewWindowTaskAction(workerConfiguration, task, tryAttempt + 1));
        }

        @Override
        public CompletableFuture<TaskAction> run() {
            if (change.isPresent()) {
                Task updatedTask = task.updateState(change.get().getId());

                try {
                    CompletableFuture<TaskAction> taskActionFuture = workerConfiguration.consumer.getConsumerDispatch().consume(task, change.get(), updatedTask)
                            .thenApply(newState -> new ReadChangeTaskAction(workerConfiguration, updatedTask, reader, tryAttempt, newState));

                    return FutureUtils.thenComposeExceptionally(taskActionFuture, this::onException);
                } catch (Throwable ex) {
                    return onException(ex);
                }
            } else {
                if (tryAttempt > 0) {
                    logger.atWarning().log("Successfully finished reading a window after %d tries. Task: %s. " +
                            "Task state: %s.", tryAttempt, task.id, task.state);
                }

                return CompletableFuture.completedFuture(new MoveToNextWindowTaskAction(workerConfiguration, task));
            }
        }
    }

    private static final class MoveToNextWindowTaskAction extends TaskAction {
        public MoveToNextWindowTaskAction(WorkerConfiguration workerConfiguration, Task task) {
            super(workerConfiguration, task);
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
