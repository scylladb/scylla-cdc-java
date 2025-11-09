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
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConfigureWorkersTracker {
    private final MockMasterTransport masterTransport;
    private final ConditionFactory await;
    private int currentGenerationIndex;

    // Track current index per table
    private final Map<TableName, Integer> currentGenerationIndexPerTable = new ConcurrentHashMap<>();

    public ConfigureWorkersTracker(MockMasterTransport masterTransport, ConditionFactory await) {
        this.masterTransport = Preconditions.checkNotNull(masterTransport);
        this.await = Preconditions.checkNotNull(await);
        this.currentGenerationIndex = 0;
    }

    public void awaitConfigureWorkers(Map<TaskId, SortedSet<StreamId>> desiredConfigureWorkers) {
        await.until(() -> desiredConfigureWorkers.equals(masterTransport.getConfigureWorkersInvocation(currentGenerationIndex)));
        currentGenerationIndex++;
    }

    public void awaitConfigureWorkers(TableName tableName, Map<TaskId, SortedSet<StreamId>> desiredConfigureWorkers) {
        int index = currentGenerationIndexPerTable.getOrDefault(tableName, 0);
        await.until(() -> {
            Map<TaskId, SortedSet<StreamId>> config = masterTransport.getConfigureWorkersInvocation(tableName, index);
            if (config == null) return false;

            // Filter the configuration to only contain tasks for this table
            Map<TaskId, SortedSet<StreamId>> filteredConfig = config.entrySet().stream()
                    .filter(entry -> entry.getKey().getTable().equals(tableName))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            return desiredConfigureWorkers.equals(filteredConfig);
        });
        currentGenerationIndexPerTable.put(tableName, index + 1);
    }

    public void awaitConfigureWorkers(GenerationMetadata generationMetadata, Set<TableName> tableNames) {
        awaitConfigureWorkers(MockGenerationMetadata.generationMetadataToTaskMap(generationMetadata, tableNames));
    }

    public void awaitConfigureWorkers(TableName tableName, GenerationMetadata generationMetadata) {
        Map<TaskId, SortedSet<StreamId>> taskMap = MockGenerationMetadata.generationMetadataToTaskMap(
                generationMetadata, Set.of(tableName));
        awaitConfigureWorkers(tableName, taskMap);
    }

    public void checkNoAdditionalConfigureWorkers() {
        int invocationCount = masterTransport.getConfigureWorkersInvocationsCount();
        assertEquals(invocationCount, currentGenerationIndex);
    }

    public void checkNoAdditionalConfigureWorkers(TableName tableName) {
        int invocationCount = masterTransport.getConfigureWorkersInvocationsCount(tableName);
        int expectedCount = currentGenerationIndexPerTable.getOrDefault(tableName, 0);
        assertEquals(expectedCount, invocationCount,
                "Expected " + expectedCount + " configurations for table " + tableName +
                " but found " + invocationCount);
    }
}
