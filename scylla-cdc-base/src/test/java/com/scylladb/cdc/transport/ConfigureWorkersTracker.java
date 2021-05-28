package com.scylladb.cdc.transport;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.VNodeId;
import com.scylladb.cdc.model.master.GenerationMetadata;
import com.scylladb.cdc.model.master.MockGenerationMetadata;
import org.awaitility.core.ConditionFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConfigureWorkersTracker {
    private final MockMasterTransport masterTransport;
    private final ConditionFactory await;
    private int currentGenerationIndex;

    public ConfigureWorkersTracker(MockMasterTransport masterTransport, ConditionFactory await) {
        this.masterTransport = Preconditions.checkNotNull(masterTransport);
        this.await = Preconditions.checkNotNull(await);
        this.currentGenerationIndex = 0;
    }

    public void awaitConfigureWorkers(Map<TaskId, SortedSet<StreamId>> desiredConfigureWorkers) {
        await.until(() -> desiredConfigureWorkers.equals(masterTransport.getConfigureWorkersInvocation(currentGenerationIndex)));
        currentGenerationIndex++;
    }

    public void awaitConfigureWorkers(GenerationMetadata generationMetadata, Set<TableName> tableNames) {
        awaitConfigureWorkers(MockGenerationMetadata.generationMetadataToTaskMap(generationMetadata, tableNames));
    }

    public void checkNoAdditionalConfigureWorkers() {
        int invocationCount = masterTransport.getConfigureWorkersInvocationsCount();
        assertEquals(invocationCount, currentGenerationIndex);
    }
}
