package com.scylladb.cdc.model.worker;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.google.common.flogger.FluentLogger;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;

public final class Worker {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private final WorkerConfiguration workerConfiguration;
    private volatile boolean shouldStop = false;

    public Worker(WorkerConfiguration workerConfiguration) {
        this.workerConfiguration = Preconditions.checkNotNull(workerConfiguration);
    }

    /*
     * Get a generation ID of given set of streams. It is assumed that all streams
     * in a set belong to the same generation. The set is assumed to be non-empty.
     */
    private static GenerationId getGenerationIdOfStreams(Map<TaskId, SortedSet<StreamId>> groupedStreams) {
        return groupedStreams.entrySet().stream().map(e -> e.getKey().getGenerationId()).findAny().get();
    }

    /*
     * Return an initial task state for given set of streams. Such an initial state
     * is used when the task has not been run before.
     *
     * All streams are assumed to belong to the same generation and the initial
     * state is build based on the ID of this generation.
     */
    private static TaskState getInitialStateForStreams(Map<TaskId, SortedSet<StreamId>> groupedStreams,
                                                       long windowSizeMs) {
        return TaskState.createInitialFor(getGenerationIdOfStreams(groupedStreams), windowSizeMs);
    }

    /*
     * For each pair task id -> task's streams, generates a Task with a state.
     *
     * The state is either taken from the Transport if this task was executed before
     * and is now restarted or is created from scratch if the task hasn't executed
     * successfully before.
     *
     * Additionally, the state is trimmed according to the table's TTL value.
     */
    private Stream<Task> createTasksWithState(Map<TaskId, SortedSet<StreamId>> groupedStreams) throws ExecutionException, InterruptedException {
        Map<TaskId, TaskState> states = workerConfiguration.transport.getTaskStates(groupedStreams.keySet());
        TaskState initialState = getInitialStateForStreams(groupedStreams, workerConfiguration.queryTimeWindowSizeMs);

        Set<TableName> tableNames = groupedStreams.keySet().stream().map(TaskId::getTable).collect(Collectors.toSet());
        Date now = Date.from(workerConfiguration.getClock().instant());

        // The furthest point in time where there might be
        // a CDC change, given table's TTL.
        Map<TableName, Timestamp> minimumWindowStarts = new HashMap<>();

        for (TableName tableName : tableNames) {
            Optional<Long> ttl = workerConfiguration.cql.fetchTableTTL(tableName).get();
            Date minimumWindowStart = new Date(0);
            if (ttl.isPresent()) {
                minimumWindowStart = new Date(now.getTime() - 1000L * ttl.get()); // TTL is in seconds, getTime() in milliseconds
            }
            minimumWindowStarts.put(tableName, new Timestamp(minimumWindowStart));
        }

        return groupedStreams.entrySet().stream().map(taskStreams -> {
            TaskId id = taskStreams.getKey();
            SortedSet<StreamId> streams = taskStreams.getValue();
            TaskState state = states.getOrDefault(id, initialState);
            state = state.trimTaskState(minimumWindowStarts.get(id.getTable()), workerConfiguration.queryTimeWindowSizeMs);
            return new Task(id, streams, state);
        });
    }

    /*
     * Creates initial actions for every group of streams (task).
     *
     * This includes fetching saved state of each task or creating a new initial
     * state for tasks that haven't run successfully before.
     */
    private Collection<TaskAction> queueFirstActionForEachTask(Map<TaskId, SortedSet<StreamId>> groupedStreams)
            throws ExecutionException, InterruptedException {
        return createTasksWithState(groupedStreams).map(task -> TaskAction.createFirstAction(workerConfiguration, task))
                .collect(Collectors.toSet());
    }

    private boolean shouldStop() {
        return shouldStop;
    }

    private ScheduledExecutorService getExecutorService() {
        return workerConfiguration.getExecutorService();
    }

    private Callable<Object> makeCallable(TaskAction a) {
        return () -> a.run().handle((na, ex) -> {
            if (ex != null) {
                logger.atSevere().withCause(ex).log("Unhandled exception in Worker.");
            } else if (!shouldStop()) {
                getExecutorService().submit(makeCallable(na));
            }
            return null;
        });
    }

    /*
     * Loops until Master sends a stop request using Transport.
     *
     * At each iteration, runs a single action from |actions| queue if any
     * available.
     */
    @SuppressWarnings("deprecation")
    private void performActionsUntilStopRequested(Collection<TaskAction> actions) {
        if (shouldStop()) {
            return;
        }

        ScheduledExecutorService executorService = getExecutorService();
        try {
            executorService.invokeAll(actions.stream().map(a -> makeCallable(a)).collect(Collectors.toSet()));
            do {
                // pretty short poll, to allow for reasonably fast switchover
                // iff using "polled" shutdown (WorkerTransport::shouldStop).
                executorService.awaitTermination(50, MILLISECONDS);
                if (workerConfiguration.transport.shouldStop()) {
                    stop();
                }
            } while (!shouldStop() && !executorService.isTerminated());
        } catch (InterruptedException e) {
            logger.atWarning().log("Worker interrupted");
        }
    }

    public void stop() {
        shouldStop = true;
        getExecutorService().shutdown();
    }

    /*
     * Fetches changes from given streams grouped into tasks.
     *
     * The assumptions are: 1. There is at least one task 2. Each task has at least
     * a single stream to fetch 3. All tasks belong to the same generation
     */
    public void run(Map<TaskId, SortedSet<StreamId>> groupedStreams) throws InterruptedException, ExecutionException {
        Preconditions.checkNotNull(groupedStreams);
        Preconditions.checkArgument(!groupedStreams.isEmpty(), "No tasks");
        Preconditions.checkArgument(groupedStreams.entrySet().stream().noneMatch(e -> e.getValue().isEmpty()),
                "Task with no streams");
        Preconditions.checkArgument(
                groupedStreams.keySet().stream().map(TaskId::getGenerationId).distinct().count() == 1,
                "Tasks from different generations");

        workerConfiguration.cql.prepare(groupedStreams.keySet().stream().map(TaskId::getTable).collect(Collectors.toSet()));
        Collection<TaskAction> actions = queueFirstActionForEachTask(groupedStreams);
        performActionsUntilStopRequested(actions);
    }
}
