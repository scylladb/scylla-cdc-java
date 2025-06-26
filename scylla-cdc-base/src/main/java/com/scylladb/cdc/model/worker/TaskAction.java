package com.scylladb.cdc.model.worker;

import java.util.Date;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.flogger.FluentLogger;
import com.scylladb.cdc.cql.WorkerCQL.Reader;
import com.scylladb.cdc.model.FutureUtils;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.Timestamp;

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
            Throwable cause = ex;
            if (ex instanceof java.util.concurrent.CompletionException && ex.getCause() != null) {
                cause = ex.getCause();
            }

            // Special handling for ClosedTimeFoundException - re-read the window with the updated task.
            // there is no need for delay in this case.
            if (cause instanceof ClosedTimeFoundException) {
                ClosedTimeFoundException ctfe = (ClosedTimeFoundException) cause;
                Task updatedTask = ctfe.getUpdatedTask();

                return CompletableFuture.completedFuture(
                        new ReadNewWindowTaskAction(workerConfiguration, updatedTask, 0));
            }

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
                CompletableFuture<Optional<RawChange>> changeCompletableFuture = reader.nextChange();
                CompletableFuture<TaskAction> taskActionFuture = changeCompletableFuture.thenCompose(changeOpt -> {
                    if (changeOpt.isPresent()) {
                        RawChange change = changeOpt.get();

                        // If we discover a closed_time while reading, we need to abort and retry after
                        // updating the task's end timestamp. The correctness relies on us not consuming
                        // changes with a timestamp greater than the closed_time when closed_time is set.
                        Optional<Date> closedTimeOpt = change.getClosedTime();
                        if (closedTimeOpt.isPresent() && !task.state.getEndTimestamp().isPresent()) {
                            // We found a closed_time, let's update the task's end timestamp
                            Date closedTime = closedTimeOpt.get();
                            logger.atInfo().log("Found closed_time %s in change from stream %s, updating task %s end timestamp.",
                                    closedTime, change.getId().getStreamId(), task.id);

                            // Update the task with the new end timestamp
                            Timestamp endTimestamp = new Timestamp(closedTime);
                            Task updatedTask = task.updateState(task.state.withEndTimestamp(endTimestamp));

                            CompletableFuture<TaskAction> result = new CompletableFuture<>();
                            result.completeExceptionally(new ClosedTimeFoundException(updatedTask));
                            return result;
                        }
                    }

                    return CompletableFuture.completedFuture(
                            new ConsumeChangeTaskAction(workerConfiguration, task, reader, changeOpt, tryAttempt));
                });

                return FutureUtils.thenComposeExceptionally(taskActionFuture, this::onException);
            } catch (Throwable ex) {
                return onException(ex);
            }
        }
    }

    /**
     * A special exception used to signal that we found a closed_time in a change
     * and need to update the task's end timestamp.
     */
    private static class ClosedTimeFoundException extends RuntimeException {
        private final Task updatedTask;

        public ClosedTimeFoundException(Task updatedTask) {
            super("Found closed_time in change, updating task end timestamp");
            this.updatedTask = updatedTask;
        }

        public Task getUpdatedTask() {
            return updatedTask;
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
                Task updatedTask;
                if(change.get().isEndOfBatch()) {
                    updatedTask = task.updateState(change.get().getId());
                } else {
                    updatedTask = task;
                }

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
            // Check for end timestamp updates before moving to next window
            Task taskToUse = task;

            // Use Optional-based method for getting table end timestamp
            TableName tableName = task.id.getTable();
            Optional<Timestamp> endTimestampOpt = workerConfiguration.transport.getTableEndTimestamp(tableName);

            if (endTimestampOpt.isPresent() && !task.state.getEndTimestamp().isPresent()) {
                Timestamp endTimestamp = endTimestampOpt.get();

                logger.atInfo().log("Found end timestamp %s for table %s, updating task %s",
                        endTimestamp, tableName, task.id);

                TaskState newState = task.state.withEndTimestamp(endTimestamp);
                taskToUse = task.updateState(newState);
            }

            TaskState newState = taskToUse.state.moveToNextWindow(workerConfiguration.queryTimeWindowSizeMs);
            workerConfiguration.transport.moveStateToNextWindow(taskToUse.id, newState);
            Task newTask = taskToUse.updateState(newState);

            // Check if we've reached the end timestamp - if so, we're done with this task
            if (newTask.hasReachedEnd()) {
                logger.atInfo().log("Task %s has reached its end timestamp %s, stopping.",
                        newTask.id, newTask.state.getEndTimestamp().get());
                return CompletableFuture.completedFuture(null);
            }

            return CompletableFuture.completedFuture(new ReadNewWindowTaskAction(workerConfiguration, newTask, 0));
        }
    }
}
