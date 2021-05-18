package com.scylladb.cdc.transport;

import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.worker.TaskState;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

public class MockWorkerTransport implements WorkerTransport {
    private Map<TaskId, TaskState> taskStates = new ConcurrentHashMap<>();

    private List<AbstractMap.SimpleEntry<TaskId, TaskState>> setStatesInvocations = new CopyOnWriteArrayList<>();
    private List<AbstractMap.SimpleEntry<TaskId, TaskState>> moveStateToNextWindowInvocations = new CopyOnWriteArrayList<>();

    @Override
    public Map<TaskId, TaskState> getTaskStates(Set<TaskId> tasks) {
        return Collections.unmodifiableMap(taskStates);
    }

    @Override
    public void setState(TaskId task, TaskState newState) {
        taskStates.put(task, newState);
        setStatesInvocations.add(new AbstractMap.SimpleEntry<>(task, newState));
    }

    @Override
    public void moveStateToNextWindow(TaskId task, TaskState newState) {
        taskStates.put(task, newState);
        moveStateToNextWindowInvocations.add(new AbstractMap.SimpleEntry<>(task, newState));
    }

    public List<TaskState> getSetStateInvocations(TaskId taskId) {
        return setStatesInvocations.stream().filter(t -> t.getKey().equals(taskId))
                .map(AbstractMap.SimpleEntry::getValue).collect(Collectors.toList());
    }

    public List<TaskState> getMoveStateToNextWindowInvocations(TaskId taskId) {
        return moveStateToNextWindowInvocations.stream().filter(t -> t.getKey().equals(taskId))
                .map(AbstractMap.SimpleEntry::getValue).collect(Collectors.toList());
    }
}
