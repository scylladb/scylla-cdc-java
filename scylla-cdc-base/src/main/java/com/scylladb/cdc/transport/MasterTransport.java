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

    /**
     * Configures workers like {@link #configureWorkers(GroupedTasks)} and also discards the state of
     * {@code superseded}: the tasks of the generation {@link #getCurrentGenerationId()} returned at
     * startup, when the master found it done and moved past it. They are empty otherwise. The
     * transport may not know these tasks, for example after a restart. A transport that keeps
     * task state must discard it only after persisting the new generation; otherwise a failure in
     * between leaves the stored generation without its state, and it is read again from its start.
     * The default ignores {@code superseded}.
     *
     * @param workerTasks the tasks of the generation to consume
     * @param superseded  the tasks whose state to discard; may be empty
     * @throws InterruptedException if interrupted while configuring workers
     */
    default void configureWorkers(GroupedTasks workerTasks, Set<TaskId> superseded)
            throws InterruptedException {
        configureWorkers(workerTasks);
    }

    // Tablets-based CDC methods
    Optional<GenerationId> getCurrentGenerationId(TableName tableName);
    void configureWorkers(TableName tableName, GroupedTasks workerTasks) throws InterruptedException;

    /**
     * Tablet counterpart of {@link #configureWorkers(GroupedTasks, Set)} for one table, where
     * {@code superseded} belong to the generation {@link #getCurrentGenerationId(TableName)} returned.
     *
     * @param tableName   the table whose workers to configure
     * @param workerTasks the table's tasks of the generation to consume
     * @param superseded  the tasks whose state to discard; may be empty
     * @throws InterruptedException if interrupted while configuring workers
     */
    default void configureWorkers(TableName tableName, GroupedTasks workerTasks, Set<TaskId> superseded)
            throws InterruptedException {
        configureWorkers(tableName, workerTasks);
    }

    void stopWorkers() throws InterruptedException;

}
