package com.scylladb.cdc.model.worker;

import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TaskId;

public final class Worker {

    private final Connectors connectors;

    public Worker(Connectors connectors) {
        this.connectors = Preconditions.checkNotNull(connectors);
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
    private static TaskState getInitialStateForStreams(Map<TaskId, SortedSet<StreamId>> groupedStreams) {
        return TaskState.createInitialFor(getGenerationIdOfStreams(groupedStreams));
    }

    /*
     * For each pair task id -> task's streams, generates a Task with a state.
     *
     * The state is either taken from the Transport if this task was executed before
     * and is now restarted or is created from scratch if the task hasn't executed
     * successfully before.
     */
    private Stream<Task> createTasksWithState(Map<TaskId, SortedSet<StreamId>> groupedStreams) {
        Map<TaskId, TaskState> states = connectors.transport.getTaskStates(groupedStreams.keySet());
        TaskState initialState = getInitialStateForStreams(groupedStreams);
        return groupedStreams.entrySet().stream().map(taskStreams -> {
            TaskId id = taskStreams.getKey();
            SortedSet<StreamId> streams = taskStreams.getValue();
            TaskState state = states.getOrDefault(id, initialState);
            return new Task(id, streams, state);
        });
    }

    /*
     * Creates initial actions for every group of streams (task).
     *
     * This includes fetching saved state of each task or creating a new initial
     * state for tasks that haven't run successfully before.
     */
    private TaskActionsQueue queueFirstActionForEachTask(Map<TaskId, SortedSet<StreamId>> groupedStreams) {
        return new TaskActionsQueue(createTasksWithState(groupedStreams)
                .map(task -> TaskAction.createFirstAction(connectors, task)).collect(Collectors.toSet()));
    }

    /*
     * Loops until Master sends a stop request using Transport.
     *
     * At each iteration, runs a single action from |actions| queue if any
     * available.
     */
    private void performActionsUntilStopRequested(TaskActionsQueue actions) {
        while (!connectors.transport.shouldStop()) {
            actions.runNextAction();
        }
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

        connectors.cql.prepare(groupedStreams.keySet().stream().map(TaskId::getTable).collect(Collectors.toSet()));
        TaskActionsQueue actions = queueFirstActionForEachTask(groupedStreams);
        performActionsUntilStopRequested(actions);
    }
}
