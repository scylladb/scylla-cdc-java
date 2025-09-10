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
import com.scylladb.cdc.transport.GroupedTasks;

public final class Worker {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private final WorkerConfiguration workerConfiguration;
    private volatile boolean shouldStop = false;

    public Worker(WorkerConfiguration workerConfiguration) {
        this.workerConfiguration = Preconditions.checkNotNull(workerConfiguration);
    }

    /*
     * Return an initial task state for given set of streams. Such an initial state
     * is used when the task has not been run before.
     *
     * All streams are assumed to belong to the same generation and the initial
     * state is build based on the ID of this generation.
     */
    private static TaskState getInitialStateForStreams(GroupedTasks workerTasks, long windowSizeMs) {
        return TaskState.createInitialFor(workerTasks.getGenerationId(), windowSizeMs);
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
    private Stream<Task> createTasksWithState(GroupedTasks workerTasks) throws ExecutionException, InterruptedException {
        Map<TaskId, SortedSet<StreamId>> taskMap = workerTasks.getTasks();
        Map<TaskId, TaskState> states = workerConfiguration.transport.getTaskStates(taskMap.keySet());
        TaskState initialState = getInitialStateForStreams(workerTasks, workerConfiguration.queryTimeWindowSizeMs);

        Set<TableName> tableNames = taskMap.keySet().stream().map(TaskId::getTable).collect(Collectors.toSet());
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

        return taskMap.entrySet().stream().map(taskStreams -> {
            TaskId id = taskStreams.getKey();
            SortedSet<StreamId> streams = taskStreams.getValue();
            TaskState state = states.getOrDefault(id, initialState);
            state = state.trimTaskState(minimumWindowStarts.get(id.getTable()), workerConfiguration.queryTimeWindowSizeMs);

            // set the task state in the transport before it is queued.
            // this is necessary because the existence of state in the transport is used to indicate
            // whether a task is active or should be aborted. if the task is executed and sees there
            // is no state in transport then it will abort itself.
            workerConfiguration.transport.setState(id, state);

            return new Task(id, streams, state);
        });
    }

    /*
     * Creates initial actions for every group of streams (task).
     *
     * This includes fetching saved state of each task or creating a new initial
     * state for tasks that haven't run successfully before.
     */
    private Collection<TaskAction> queueFirstActionForEachTask(GroupedTasks workerTasks)
            throws ExecutionException, InterruptedException {
        return createTasksWithState(workerTasks).map(task -> TaskAction.createFirstAction(workerConfiguration, task))
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
            } else if (na != null && !shouldStop()) {
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
    public void run(GroupedTasks workerTasks) throws InterruptedException, ExecutionException {
        Preconditions.checkNotNull(workerTasks, "Worker tasks cannot be null");
        Map<TaskId, SortedSet<StreamId>> taskMap = workerTasks.getTasks();
        Preconditions.checkArgument(!taskMap.isEmpty(), "No tasks");
        Preconditions.checkArgument(taskMap.entrySet().stream().noneMatch(e -> e.getValue().isEmpty()),
                "Task with no streams");

        workerConfiguration.cql.prepare(taskMap.keySet().stream().map(TaskId::getTable).collect(Collectors.toSet()));
        Collection<TaskAction> actions = queueFirstActionForEachTask(workerTasks);
        performActionsUntilStopRequested(actions);
    }

    /**
     * Adds new tasks dynamically to the running worker.
     *
     * @param workerTasks the tasks to add
     * @throws ExecutionException if there's an error preparing the task
     * @throws InterruptedException if the thread is interrupted
     */
    public void addTasks(GroupedTasks workerTasks) throws ExecutionException, InterruptedException {
        Map<TaskId, SortedSet<StreamId>> newTasks = workerTasks.getTasks();

        if (shouldStop) {
            throw new IllegalStateException("Cannot add tasks to a stopped worker");
        }

        if (newTasks.isEmpty()) {
            return;
        }

        // Prepare any new tables
        Set<TableName> tables = newTasks.keySet().stream()
                .map(TaskId::getTable)
                .collect(Collectors.toSet());

        workerConfiguration.cql.prepare(tables);

        // Create and submit actions for the new tasks
        Collection<TaskAction> newActions = queueFirstActionForEachTask(workerTasks);

        ScheduledExecutorService executorService = getExecutorService();
        executorService.invokeAll(newActions.stream()
                .map(this::makeCallable)
                .collect(Collectors.toSet()));
    }
}
