package com.scylladb.cdc.model.master;

import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.VNodeId;
import com.scylladb.cdc.model.Timestamp;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

public class MockGenerationMetadata {
    public static GenerationMetadata mockGenerationMetadata(Timestamp start, Optional<Timestamp> end, int vnodeCount) {
        return mockGenerationMetadata(start, end, vnodeCount, 4);
    }

    public static GenerationMetadata mockGenerationMetadata(Timestamp start, Optional<Timestamp> end,
                                                            int vnodeCount, int streamsPerVnode) {
        // Random with deterministic seed
        Random random = new Random(start.toDate().getTime());

        SortedSet<StreamId> streams = new TreeSet<>();
        for (int vnode = 0; vnode < vnodeCount; vnode++) {
            for (int stream = 0; stream < streamsPerVnode; stream++) {
                long upperDword = random.nextLong(); // <token:64>
                long lowerDword = random.nextLong(); // <random:38>
                lowerDword = lowerDword << 22;
                lowerDword |= vnode; // <index:22>
                lowerDword = lowerDword << 4;
                lowerDword |= 1; // <version:4>

                ByteBuffer buffer = ByteBuffer.allocate(16);
                buffer.putLong(upperDword);
                buffer.putLong(lowerDword);
                buffer.position(0);

                StreamId streamId = new StreamId(buffer);
                streams.add(streamId);
            }
        }
        return new GenerationMetadata(start, end, streams);
    }

    public static Map<TaskId, SortedSet<StreamId>> generationMetadataToTaskMap(
            GenerationMetadata generationMetadata, Set<TableName> tableNames) {
        Map<TaskId, SortedSet<StreamId>> generationMetadataMap = new HashMap<>();
        List<VNodeId> vNodes = generationMetadata.getStreams().stream()
                .map(StreamId::getVNodeId).distinct().collect(Collectors.toList());

        // FIXME - quadratic complexity (|vNodes|^2)
        for (TableName tableName : tableNames) {
            for (VNodeId vNodeId : vNodes) {
                TaskId taskId = new TaskId(generationMetadata.getId(), vNodeId, tableName);
                SortedSet<StreamId> streamIds = generationMetadata.getStreams().stream()
                        .filter(s -> s.getVNodeId().equals(vNodeId)).collect(Collectors.toCollection(TreeSet::new));
                generationMetadataMap.put(taskId, streamIds);
            }
        }
        return generationMetadataMap;
    }
}
