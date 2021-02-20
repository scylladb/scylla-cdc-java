package com.scylladb.cdc.transport;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.VNodeId;
import com.scylladb.cdc.model.master.GenerationMetadata;
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
        Map<TaskId, SortedSet<StreamId>> expectedConfigureWorkers = new HashMap<>();
        List<VNodeId> vNodes = generationMetadata.getStreams().stream()
                .map(StreamId::getVNodeId).distinct().collect(Collectors.toList());

        // FIXME - quadratic complexity (|vNodes|^2)
        for (TableName tableName : tableNames) {
            for (VNodeId vNodeId : vNodes) {
                TaskId taskId = new TaskId(generationMetadata.getId(), vNodeId, tableName);
                SortedSet<StreamId> streamIds = generationMetadata.getStreams().stream()
                        .filter(s -> s.getVNodeId().equals(vNodeId)).collect(Collectors.toCollection(TreeSet::new));
                expectedConfigureWorkers.put(taskId, streamIds);
            }
        }

        awaitConfigureWorkers(expectedConfigureWorkers);
    }

    public void checkNoAdditionalConfigureWorkers() {
        int invocationCount = masterTransport.getConfigureWorkersInvocationsCount();
        assertEquals(invocationCount, currentGenerationIndex);
    }
}
