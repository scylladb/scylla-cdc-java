package com.scylladb.cdc.model.worker;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.flogger.FluentLogger;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.VNodeId;
import com.scylladb.cdc.transport.GroupedTasks;
import com.scylladb.cdc.transport.WorkerTransport;

public final class Worker {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private static final class TaskStateMigration {
        private final Set<TaskId> legacyTasks;
        private final Set<TaskId> restoredTasks;

        private TaskStateMigration(Set<TaskId> legacyTasks, Set<TaskId> restoredTasks) {
            this.legacyTasks = Collections.unmodifiableSet(new HashSet<>(legacyTasks));
            this.restoredTasks = Collections.unmodifiableSet(new HashSet<>(restoredTasks));
        }
    }

    private static final class LoadedTaskStates {
        private final Map<TaskId, TaskState> states;
        private final TaskStateMigration migration;

        private LoadedTaskStates(Map<TaskId, TaskState> states, TaskStateMigration migration) {
            this.states = states;
            this.migration = migration;
        }
    }

    private static final class TabletMigrationPlan {
        private final Set<TaskId> assignedTasks;
        private final Map<TaskId, Set<TaskId>> replacementsByLegacyTask;

        private TabletMigrationPlan(Set<TaskId> assignedTasks,
                                    Map<TaskId, Set<TaskId>> replacementsByLegacyTask) {
            this.assignedTasks = assignedTasks;
            this.replacementsByLegacyTask = replacementsByLegacyTask;
        }

        private static TabletMigrationPlan create(
                Map<TaskId, SortedSet<StreamId>> taskMap) {
            Map<TableName, List<Map.Entry<TaskId, SortedSet<StreamId>>>> tasksByTable =
                    groupTasksByTable(taskMap);
            validateTabletLayout(tasksByTable.values());

            Map<TaskId, Set<TaskId>> replacementsByLegacyTask = new HashMap<>();
            for (List<Map.Entry<TaskId, SortedSet<StreamId>>> tableTasks
                    : tasksByTable.values()) {
                if (!containsTabletTask(tableTasks)) {
                    continue;
                }

                TaskId exampleTask = tableTasks.get(0).getKey();
                TaskId legacyTask = new TaskId(exampleTask.getGenerationId(), new VNodeId(0),
                        exampleTask.getTable());
                Set<TaskId> replacementTasks = tableTasks.stream()
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toSet());
                replacementsByLegacyTask.put(legacyTask, replacementTasks);
            }
            return new TabletMigrationPlan(
                    new HashSet<>(taskMap.keySet()), replacementsByLegacyTask);
        }

        private static Map<TableName, List<Map.Entry<TaskId, SortedSet<StreamId>>>>
                groupTasksByTable(Map<TaskId, SortedSet<StreamId>> taskMap) {
            Map<TableName, List<Map.Entry<TaskId, SortedSet<StreamId>>>> tasksByTable =
                    new HashMap<>();
            taskMap.entrySet().forEach(entry -> tasksByTable
                    .computeIfAbsent(entry.getKey().getTable(), ignored -> new ArrayList<>())
                    .add(entry));
            return tasksByTable;
        }

        private static void validateTabletLayout(
                Map<TaskId, SortedSet<StreamId>> taskMap) {
            validateTabletLayout(groupTasksByTable(taskMap).values());
        }

        private static void validateTabletLayout(
                Collection<List<Map.Entry<TaskId, SortedSet<StreamId>>>> tasksByTable) {
            for (List<Map.Entry<TaskId, SortedSet<StreamId>>> tableTasks
                    : tasksByTable) {
                if (!containsTabletTask(tableTasks)) {
                    continue;
                }

                Preconditions.checkArgument(tableTasks.stream().allMatch(entry ->
                                entry.getKey().isTabletStreamTask()
                                        && entry.getValue().size() == 1),
                        "Worker task assignment rejected: tasks for table %s mix tablet and vnode "
                                + "layouts or contain a non-singleton tablet task. No tasks in "
                                + "this assignment will start.",
                        tableTasks.get(0).getKey().getTable());
            }
        }

        private static boolean containsTabletTask(
                List<Map.Entry<TaskId, SortedSet<StreamId>>> tableTasks) {
            return tableTasks.stream()
                    .anyMatch(entry -> entry.getKey().isTabletStreamTask());
        }

        private Set<TaskId> legacyTaskIds() {
            return Collections.unmodifiableSet(
                    new HashSet<>(replacementsByLegacyTask.keySet()));
        }

        private Set<TaskId> replacementTaskIds(TaskId legacyTask) {
            return replacementsByLegacyTask.get(legacyTask);
        }

        /**
         * Fans a checkpoint created by the legacy single-task tablet grouping out to missing
         * per-stream task states.
         *
         * <p>The returned IDs let task preparation normalize only the states restored here, after
         * applying TTL trimming. Existing per-stream states remain authoritative.
         *
         * @return task IDs whose missing states were restored from legacy checkpoints
         */
        private Set<TaskId> restoreMissingReplacementStates(
                Map<TaskId, TaskState> loadedStates) {
            Set<TaskId> restoredTasks = new HashSet<>();
            for (Map.Entry<TaskId, Set<TaskId>> replacements
                    : replacementsByLegacyTask.entrySet()) {
                TaskState legacyState = loadedStates.get(replacements.getKey());
                if (legacyState == null) {
                    continue;
                }
                for (TaskId replacementTask : replacements.getValue()) {
                    if (loadedStates.putIfAbsent(replacementTask, legacyState) == null) {
                        restoredTasks.add(replacementTask);
                    }
                }
            }
            loadedStates.keySet().retainAll(assignedTasks);
            return restoredTasks;
        }

        private static TaskState normalizeStateForReplacementStream(TaskState legacyState,
                                                                    StreamId replacementStream,
                                                                    long queryTimeWindowSizeMs) {
            Optional<ChangeId> checkpoint = legacyState.getLastConsumedChangeId();
            if (!checkpoint.isPresent()) {
                return legacyState;
            }

            int comparison = replacementStream.compareTo(checkpoint.get().getStreamId());
            if (comparison < 0) {
                return legacyState.moveToNextWindow(queryTimeWindowSizeMs);
            }
            if (comparison == 0) {
                return legacyState;
            }
            return new TaskState(legacyState.getWindowStartTimestamp(),
                    legacyState.getWindowEndTimestamp(), Optional.empty());
        }
    }

    private static final class PreparedTasks {
        private final Collection<Task> tasks;
        private final TaskStateMigration migration;

        private PreparedTasks(Collection<Task> tasks, TaskStateMigration migration) {
            this.tasks = tasks;
            this.migration = migration;
        }
    }

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
    private PreparedTasks prepareTasksWithState(GroupedTasks workerTasks)
            throws ExecutionException, InterruptedException {
        Map<TaskId, SortedSet<StreamId>> taskMap = workerTasks.getTasks();
        LoadedTaskStates loaded = loadTaskStatesForPreparation(workerConfiguration.transport, taskMap);
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

        Collection<Task> tasks = taskMap.entrySet().stream().map(taskStreams -> {
            TaskId id = taskStreams.getKey();
            SortedSet<StreamId> streams = taskStreams.getValue();
            TaskState state = loaded.states.getOrDefault(id, initialState);
            state = state.trimTaskState(minimumWindowStarts.get(id.getTable()), workerConfiguration.queryTimeWindowSizeMs);
            if (loaded.migration.restoredTasks.contains(id)) {
                // The legacy task processed streams in sorted order. Normalize its checkpoint so
                // a singleton task never receives a cursor for another stream: earlier streams
                // advance past this window, the checkpoint stream resumes within it, and later
                // streams read the whole window.
                state = TabletMigrationPlan.normalizeStateForReplacementStream(
                        state, streams.first(), workerConfiguration.queryTimeWindowSizeMs);
            }
            return new Task(id, streams, state);
        }).collect(Collectors.toList());
        return new PreparedTasks(tasks, loaded.migration);
    }

    private static LoadedTaskStates loadTaskStatesForPreparation(
            WorkerTransport transport, Map<TaskId, SortedSet<StreamId>> taskMap) {
        TabletMigrationPlan migrationPlan = TabletMigrationPlan.create(taskMap);
        Map<TaskId, TaskState> loadedStates = new HashMap<>(
                transport.getTaskStates(taskMap.keySet()));
        loadedStates.keySet().retainAll(taskMap.keySet());
        Set<TaskId> missingReplacementTasks = new HashSet<>(taskMap.keySet());
        missingReplacementTasks.removeAll(loadedStates.keySet());

        Set<TaskId> legacyTaskIds = migrationPlan.legacyTaskIds();
        Set<TaskId> returnedLegacyTaskIds = new HashSet<>();
        if (!legacyTaskIds.isEmpty()) {
            Map<TaskId, TaskState> migrationStates =
                    transport.getTaskStatesForMigration(legacyTaskIds);
            legacyTaskIds.forEach(taskId -> {
                TaskState state = migrationStates.get(taskId);
                if (state != null) {
                    loadedStates.put(taskId, state);
                    returnedLegacyTaskIds.add(taskId);
                }
            });
            legacyTaskIds.stream()
                    .filter(taskId -> !returnedLegacyTaskIds.contains(taskId))
                    .forEach(taskId -> {
                boolean hasMissingReplacementState = migrationPlan
                        .replacementTaskIds(taskId).stream()
                        .anyMatch(missingReplacementTasks::contains);
                if (hasMissingReplacementState) {
                    logger.atInfo().log(
                            "No legacy tablet checkpoints found for missing replacement tasks: %s",
                            taskId);
                } else {
                    logger.atFine().log("No legacy tablet checkpoint remains: %s", taskId);
                }
            });
        }
        Set<TaskId> restoredTasks = migrationPlan.restoreMissingReplacementStates(loadedStates);
        return new LoadedTaskStates(loadedStates,
                new TaskStateMigration(returnedLegacyTaskIds, restoredTasks));
    }

    /*
     * Creates initial actions for every group of streams (task).
     *
     * This includes fetching saved state of each task or creating a new initial
     * state for tasks that haven't run successfully before.
     */
    private Collection<TaskAction> createFirstActions(PreparedTasks preparedTasks) {
        return preparedTasks.tasks.stream()
                .map(task -> TaskAction.createFirstAction(workerConfiguration, task))
                .collect(Collectors.toSet());
    }

    private void persistPreparedTasks(Collection<PreparedTasks> preparedTaskGroups) {
        // State must exist in the transport before a task is queued: running tasks use its
        // existence to determine whether they are still active. Prepare every group first so a
        // later preparation failure cannot leave an earlier group registered but not running.
        preparedTaskGroups.stream()
                .flatMap(prepared -> prepared.tasks.stream())
                .forEach(task -> workerConfiguration.transport.setState(task.id, task.state));

        Set<TaskId> migratedLegacyTasks = preparedTaskGroups.stream()
                .flatMap(prepared -> prepared.migration.legacyTasks.stream())
                .collect(Collectors.toSet());
        if (!migratedLegacyTasks.isEmpty()) {
            int restoredTaskCount = preparedTaskGroups.stream()
                    .mapToInt(prepared -> prepared.migration.restoredTasks.size())
                    .sum();
            workerConfiguration.transport.completeTaskStateMigration(
                    Collections.unmodifiableSet(migratedLegacyTasks));
            if (restoredTaskCount > 0) {
                logger.atInfo().log(
                        "Initialized %d replacement task states from legacy tablet checkpoints: %s",
                        restoredTaskCount, migratedLegacyTasks);
            } else {
                logger.atFine().log(
                        "Legacy tablet checkpoints found, but all replacement states already "
                                + "existed: %s",
                        migratedLegacyTasks);
            }
        }
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

    /** Fetches changes from tasks belonging to one generation. */
    public void run(GroupedTasks workerTasks) throws InterruptedException, ExecutionException {
        Preconditions.checkNotNull(workerTasks, "Worker tasks cannot be null");
        runTaskGroups(Collections.singleton(workerTasks));
    }

    /**
     * Fetches changes from one or more generation-homogeneous task groups.
     *
     * <p>This entry point allows a distributed transport worker to own tasks from multiple CDC
     * generations while retaining the generation boundary of each {@link GroupedTasks} instance.
     * A task ID may occur in only one group, and all tasks for a table must belong to the same
     * generation. Without an end timestamp, a worker cannot safely run old and current generations
     * of one table at the same time.
     */
    public void runTaskGroups(Collection<GroupedTasks> workerTaskGroups)
            throws InterruptedException, ExecutionException {
        Preconditions.checkNotNull(workerTaskGroups, "Worker task groups cannot be null");

        Map<TaskId, SortedSet<StreamId>> allTasks = new HashMap<>();
        Map<TableName, GenerationId> generationByTable = new HashMap<>();
        List<GroupedTasks> nonEmptyTaskGroups = new ArrayList<>();

        for (GroupedTasks workerTasks : workerTaskGroups) {
            Preconditions.checkNotNull(workerTasks, "Worker task group cannot be null");
            Map<TaskId, SortedSet<StreamId>> taskMap = workerTasks.getTasks();
            if (taskMap.isEmpty()) {
                logEmptyTaskGroup(workerTasks);
                continue;
            }

            taskMap.forEach((taskId, streams) -> {
                Preconditions.checkArgument(!allTasks.containsKey(taskId),
                        "Duplicate task ID across groups: %s", taskId);
                allTasks.put(taskId, streams);
                TableName table = taskId.getTable();
                GenerationId previousGeneration = generationByTable.putIfAbsent(
                        table, taskId.getGenerationId());
                Preconditions.checkArgument(previousGeneration == null
                                || previousGeneration.equals(taskId.getGenerationId()),
                        "Tasks for table %s span multiple generations: %s and %s",
                        table, previousGeneration, taskId.getGenerationId());
                if (streams.isEmpty()) {
                    logger.atWarning().log("Task %s has no streams assigned to it.", taskId);
                }
            });
            nonEmptyTaskGroups.add(workerTasks);
        }

        if (nonEmptyTaskGroups.isEmpty()) {
            return;
        }

        // Validate per-table layout across group boundaries before preparing or persisting any
        // task. This prevents a distributed assignment from presenting the legacy task in one
        // group and its per-stream replacements in another.
        TabletMigrationPlan.validateTabletLayout(allTasks);

        workerConfiguration.cql.prepare(generationByTable.keySet());
        Collection<PreparedTasks> preparedTaskGroups = new ArrayList<>();
        for (GroupedTasks workerTasks : nonEmptyTaskGroups) {
            preparedTaskGroups.add(prepareTasksWithState(workerTasks));
        }
        persistPreparedTasks(preparedTaskGroups);

        Collection<TaskAction> actions = new HashSet<>();
        for (PreparedTasks preparedTasks : preparedTaskGroups) {
            actions.addAll(createFirstActions(preparedTasks));
        }
        performActionsUntilStopRequested(actions);
    }

    private static void logEmptyTaskGroup(GroupedTasks workerTasks) {
        logger.atSevere().log(String.format("Worker was given an empty set of tasks to run (Generation %s). " +
                "Check the integrity of your cluster and system CDC tables. (For vnodes model check " +
                "cdc_streams_descriptions_v2 and cdc_generation_timestamp within system_distributed " +
                "keyspace. For tablets check cdc_timestamps and cdc_streams within system keyspace).",
                workerTasks.getGenerationId()));
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
        PreparedTasks preparedTasks = prepareTasksWithState(workerTasks);
        persistPreparedTasks(Collections.singleton(preparedTasks));
        Collection<TaskAction> newActions = createFirstActions(preparedTasks);

        ScheduledExecutorService executorService = getExecutorService();
        executorService.invokeAll(newActions.stream()
                .map(this::makeCallable)
                .collect(Collectors.toSet()));
    }
}
