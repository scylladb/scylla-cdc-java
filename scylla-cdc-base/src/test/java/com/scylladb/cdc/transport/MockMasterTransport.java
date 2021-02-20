package com.scylladb.cdc.transport;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.master.GenerationMetadata;
import org.awaitility.core.ConditionFactory;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicInteger;

public class MockMasterTransport implements MasterTransport {
    private volatile Timestamp currentFullyConsumedTimestamp = new Timestamp(new Date(0));
    private volatile Optional<GenerationId> currentGenerationId = Optional.empty();
    private final List<Map<TaskId, SortedSet<StreamId>>> configureWorkersInvocations = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger areTasksFullyConsumedUntilCount = new AtomicInteger(0);

    public void setCurrentFullyConsumedTimestamp(Timestamp newTimestamp) {
        currentFullyConsumedTimestamp = Preconditions.checkNotNull(newTimestamp);
    }

    public void setGenerationFullyConsumed(GenerationMetadata generation) {
        Preconditions.checkNotNull(generation);

        // Set currentFullyConsumedTimestamp well past generation end.
        currentFullyConsumedTimestamp = generation.getEnd().get().plus(1, ChronoUnit.MINUTES);
    }

    public void setCurrentGenerationId(Optional<GenerationId> newGenerationId) {
        currentGenerationId = Preconditions.checkNotNull(newGenerationId);
    }

    public Map<TaskId, SortedSet<StreamId>> getConfigureWorkersInvocation(int index) {
        if (index >= configureWorkersInvocations.size()) {
            return null;
        }
        return configureWorkersInvocations.get(index);
    }

    public int getConfigureWorkersInvocationsCount() {
        return configureWorkersInvocations.size();
    }

    public int getAreTasksFullyConsumedUntilCount() {
        return areTasksFullyConsumedUntilCount.get();
    }

    public ConfigureWorkersTracker tracker(ConditionFactory await) {
        return new ConfigureWorkersTracker(this, await);
    }

    @Override
    public Optional<GenerationId> getCurrentGenerationId() {
        return currentGenerationId;
    }

    @Override
    public boolean areTasksFullyConsumedUntil(Set<TaskId> tasks, Timestamp until) {
        areTasksFullyConsumedUntilCount.incrementAndGet();
        return until.compareTo(currentFullyConsumedTimestamp) < 0;
    }

    @Override
    public void configureWorkers(Map<TaskId, SortedSet<StreamId>> workerConfigurations) {
        configureWorkersInvocations.add(workerConfigurations);
    }
}
