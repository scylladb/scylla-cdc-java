package com.scylladb.cdc.debezium.connector;

import com.datastax.driver.core.utils.Bytes;
import com.datastax.driver.core.utils.UUIDs;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.worker.TaskState;
import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.util.Collect;

import java.time.Instant;
import java.util.*;

public class SourceInfo extends BaseSourceInfo {

    public static final String KEYSPACE_NAME = "keyspace_name";
    public static final String TABLE_NAME = "table_name";
    public static final String VNODE_ID = "vnode_id";
    public static final String GENERATION_START = "generation_start";

    public static final String WINDOW_START = "window_start";
    public static final String WINDOW_END = "window_end";
    public static final String CHANGE_ID_STREAM_ID = "change_id_stream_id";
    public static final String CHANGE_ID_TIME = "change_id_time";

    private TableName lastTableName;
    private Instant lastTimestamp = new Date(0).toInstant();

    private final Map<TaskId, TaskState> taskStates = new HashMap<>();

    protected SourceInfo(ScyllaConnectorConfig connectorConfig) {
        super(connectorConfig);
    }

    public Map<String, String> partition(TaskId taskId) {
        return Collect.hashMapOf(KEYSPACE_NAME, taskId.getTable().keyspace,
                TABLE_NAME, taskId.getTable().name, VNODE_ID, Long.toString(taskId.getvNodeId().getIndex()),
                GENERATION_START, Long.toString(taskId.getGenerationId().getGenerationStart().toDate().getTime()));
    }

    public Map<String, String> offset(TaskId taskId) {
        TaskState position = getTaskState(taskId);
        Map<String, String> result = Collect.hashMapOf(WINDOW_START, position.getWindowStart().toString(),
                WINDOW_END, position.getWindowEnd().toString());
        position.getLastConsumedChangeId().ifPresent(changeId -> {
            result.putAll(Collect.hashMapOf(CHANGE_ID_STREAM_ID, Bytes.toHexString(changeId.getStreamId().getValue()),
                    CHANGE_ID_TIME, changeId.getChangeTime().getUUID().toString()));
        });
        return result;
    }

    public TaskState getTaskState(TaskId taskId) {
        return taskStates.get(taskId);
    }

    public void setTaskState(TaskId taskId, TaskState taskState) {
        taskStates.put(taskId, taskState);
        lastTableName = taskId.getTable();
        // TODO - set the timestamp based on cdc$time column, not window
        lastTimestamp = new Date(UUIDs.unixTimestamp(taskState.getWindowStart())).toInstant();
    }

    public void dataChangeEvent(TaskId taskId, TaskState taskState) {
        setTaskState(taskId, taskState);
    }

    public TableName getTableName() {
        return lastTableName;
    }

    @Override
    protected Instant timestamp() {
        return lastTimestamp;
    }

    @Override
    protected String database() {
        // TODO - database() is required by Debezium. Currently returning the keyspace name.
        return lastTableName.keyspace;
    }
}
