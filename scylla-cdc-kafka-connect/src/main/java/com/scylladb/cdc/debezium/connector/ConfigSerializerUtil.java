package com.scylladb.cdc.debezium.connector;

import com.datastax.driver.core.utils.Bytes;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.VNodeId;
import org.apache.commons.lang3.tuple.Pair;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class ConfigSerializerUtil {
    private static final String FIELD_DELIMITER = ";";
    private static final String STREAM_ID_DELIMITER = ",";
    private static final String CLUSTER_IP_DELIMITER = ",";
    private static final String TABLE_NAMES_DELIMITER = ",";

    /*
     * Serializes Task (Worker) config into String.
     *
     * Format:
     * "generationStart;vNodeIndex;keyspace;table;streamId1,streamId2,streamId3,..."
     */
    public static String serializeTaskConfig(TaskId taskId, SortedSet<StreamId> streamIds) {
        String generationStartEpoch = Long.toString(taskId.getGenerationId().getGenerationStart().toDate().getTime());
        String vNodeIndex = Integer.toString(taskId.getvNodeId().getIndex());
        String keyspace = taskId.getTable().keyspace;
        String table = taskId.getTable().name;
        String delimitedStreamIds = streamIds.stream().map(StreamId::getValue)
                .map(Bytes::toHexString).collect(Collectors.joining(STREAM_ID_DELIMITER));
        return String.join(FIELD_DELIMITER, generationStartEpoch, vNodeIndex, keyspace, table, delimitedStreamIds);
    }

    // TODO - introduce a new type for Pair<TaskId, SortedSet<StreamId>>?
    public static Pair<TaskId, SortedSet<StreamId>> deserializeTaskConfig(String serialized) {
        String[] fields = serialized.split(FIELD_DELIMITER);

        GenerationId generationId = new GenerationId(new Timestamp(new Date(Long.parseLong(fields[0]))));
        VNodeId vNodeId = new VNodeId(Integer.parseInt(fields[1]));
        TableName table = new TableName(fields[2], fields[3]);
        TaskId taskId = new TaskId(generationId, vNodeId, table);

        SortedSet<StreamId> streamIds = Arrays.stream(fields[4].split(STREAM_ID_DELIMITER))
                .map(Bytes::fromHexString).map(StreamId::new).collect(Collectors.toCollection(TreeSet<StreamId>::new));

        return Pair.of(taskId, streamIds);
    }

    /*
     * Deserializes a list of IP addresses, provided as a comma-separated list of pairs <IP>:<PORT>.
     */
    public static List<InetSocketAddress> deserializeClusterIpAddresses(String serialized) {
        String[] fields = serialized.split(CLUSTER_IP_DELIMITER);
        return Arrays.stream(fields).map(ConfigSerializerUtil::deserializeClusterIpAddress).collect(Collectors.toList());
    }

    private static InetSocketAddress deserializeClusterIpAddress(String serialized) {
        String[] hostPort = serialized.split(":");

        // TODO - throw user-friendly error and do proper validation
        assert hostPort.length == 2;

        return new InetSocketAddress(hostPort[0], Integer.parseInt(hostPort[1]));
    }

    /*
     * Deserializes a list of table names, provided as a comma-separated list of <keyspace name>.<table name>.
     */
    public static Set<TableName> deserializeTableNames(String serialized) {
        String[] tables = serialized.split(TABLE_NAMES_DELIMITER);

        return Arrays.stream(tables).map(t -> {
           String[] keyspaceName = t.split("\\.");

            // TODO - throw user-friendly error and do proper validation
           assert keyspaceName.length == 2;

            return new TableName(keyspaceName[0], keyspaceName[1]);
        }).collect(Collectors.toSet());
    }
}
