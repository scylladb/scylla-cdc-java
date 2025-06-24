package com.scylladb.cdc.transport;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;

import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;

public interface MasterTransport {
    // Vnode-based CDC methods
    Optional<GenerationId> getCurrentGenerationId();
    boolean areTasksFullyConsumedUntil(Set<TaskId> tasks, Timestamp until);
    void configureWorkers(GroupedTasks workerTasks) throws InterruptedException;

    // Tablets-based CDC methods
    Optional<GenerationId> getCurrentGenerationId(TableName tableName);
    void configureWorkers(TableName tableName, GroupedTasks workerTasks) throws InterruptedException;
}
