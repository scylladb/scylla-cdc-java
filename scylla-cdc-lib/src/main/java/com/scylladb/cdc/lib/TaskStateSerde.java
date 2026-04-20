package com.scylladb.cdc.lib;

import com.google.common.base.Preconditions;
import com.google.common.io.BaseEncoding;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.VNodeId;
import com.scylladb.cdc.model.worker.ChangeId;
import com.scylladb.cdc.model.worker.ChangeTime;
import com.scylladb.cdc.model.worker.TaskState;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Serialization and deserialization helpers for CDC model types.
 *
 * <p>These utilities are intended for use by {@link CDCStateStore} implementations that need to
 * convert {@link TaskId}, {@link TaskState}, and {@link GenerationId} to and from a storable
 * format (strings, maps, byte arrays, etc.).
 *
 * <p>All serialization formats are stable across library versions.
 *
 * <h2>TaskId key format</h2>
 * <pre>{@code <keyspace>.<table>:<generationStartMs>:<vnodeIndex>}</pre>
 * Example: {@code my_ks.my_table:1700000000000:42}
 *
 * <h2>TaskState map keys</h2>
 * <ul>
 *   <li>{@code window_start} — epoch milliseconds of the window start (long as string)
 *   <li>{@code window_end} — epoch milliseconds of the window end (long as string)
 *   <li>{@code change_id_stream} — hex-encoded 16-byte StreamId (present only when
 *       lastConsumedChangeId is set)
 *   <li>{@code change_id_time} — UUID string of ChangeTime (present only when
 *       lastConsumedChangeId is set)
 * </ul>
 *
 * <h2>GenerationId format</h2>
 * Serialized as epoch milliseconds (long as string).
 */
public final class TaskStateSerde {

    static final String TASK_STATE_WINDOW_START = "window_start";
    static final String TASK_STATE_WINDOW_END = "window_end";
    static final String TASK_STATE_CHANGE_ID_STREAM = "change_id_stream";
    static final String TASK_STATE_CHANGE_ID_TIME = "change_id_time";

    private TaskStateSerde() {}

    // -------------------------------------------------------------------------
    // TaskId
    // -------------------------------------------------------------------------

    /**
     * Serializes a {@link TaskId} to a stable string key.
     *
     * <p>Format: {@code <keyspace>.<table>:<generationStartMs>:<vnodeIndex>}
     *
     * @param taskId the task ID to serialize; must not be null
     * @return a stable string key uniquely identifying the task
     */
    public static String taskIdToKey(TaskId taskId) {
        Preconditions.checkNotNull(taskId);
        return taskId.getTable().keyspace + "." + taskId.getTable().name
                + ":" + taskId.getGenerationId().getGenerationStart().toDate().getTime()
                + ":" + taskId.getvNodeId().getIndex();
    }

    /**
     * Deserializes a {@link TaskId} from the string key produced by {@link #taskIdToKey}.
     *
     * @param key the string key to parse; must not be null
     * @return the deserialized TaskId
     * @throws IllegalArgumentException if the key does not match the expected format
     */
    public static TaskId keyToTaskId(String key) {
        Preconditions.checkNotNull(key);
        // Format: <keyspace>.<table>:<generationStartMs>:<vnodeIndex>
        int lastColon = key.lastIndexOf(':');
        int prevColon = key.lastIndexOf(':', lastColon - 1);
        Preconditions.checkArgument(lastColon > 0 && prevColon > 0,
                "Invalid TaskId key format (expected '<ks>.<table>:<genMs>:<vnode>'): %s", key);

        String tableSpec = key.substring(0, prevColon);
        long generationStartMs = Long.parseLong(key.substring(prevColon + 1, lastColon));
        int vnodeIndex = Integer.parseInt(key.substring(lastColon + 1));

        int dot = tableSpec.indexOf('.');
        Preconditions.checkArgument(dot > 0,
                "Invalid table spec in TaskId key (expected '<keyspace>.<table>'): %s", tableSpec);

        String keyspace = tableSpec.substring(0, dot);
        String table = tableSpec.substring(dot + 1);

        return new TaskId(
                new GenerationId(new Timestamp(new Date(generationStartMs))),
                new VNodeId(vnodeIndex),
                new TableName(keyspace, table));
    }

    // -------------------------------------------------------------------------
    // TaskState
    // -------------------------------------------------------------------------

    /**
     * Serializes a {@link TaskState} to a {@code Map<String, String>} suitable for storage in any
     * key-value backend.
     *
     * <p>Keys:
     * <ul>
     *   <li>{@code window_start} — epoch ms of window start
     *   <li>{@code window_end} — epoch ms of window end
     *   <li>{@code change_id_stream} — hex StreamId (only if lastConsumedChangeId is present)
     *   <li>{@code change_id_time} — UUID string of ChangeTime (only if lastConsumedChangeId is
     *       present)
     * </ul>
     *
     * @param state the task state to serialize; must not be null
     * @return a map representation of the state
     */
    public static Map<String, String> taskStateToMap(TaskState state) {
        Preconditions.checkNotNull(state);
        Map<String, String> map = new HashMap<>();
        map.put(TASK_STATE_WINDOW_START,
                String.valueOf(state.getWindowStartTimestamp().toDate().getTime()));
        map.put(TASK_STATE_WINDOW_END,
                String.valueOf(state.getWindowEndTimestamp().toDate().getTime()));
        state.getLastConsumedChangeId().ifPresent(changeId -> {
            map.put(TASK_STATE_CHANGE_ID_STREAM, streamIdToHex(changeId.getStreamId()));
            map.put(TASK_STATE_CHANGE_ID_TIME, changeId.getChangeTime().getUUID().toString());
        });
        return map;
    }

    /**
     * Deserializes a {@link TaskState} from the map produced by {@link #taskStateToMap}.
     *
     * @param map the map to deserialize; must contain at least {@code window_start} and
     *            {@code window_end}
     * @return the deserialized TaskState
     * @throws IllegalArgumentException if required keys are missing or values are malformed
     */
    public static TaskState mapToTaskState(Map<String, String> map) {
        Preconditions.checkNotNull(map);
        String startMs = map.get(TASK_STATE_WINDOW_START);
        String endMs = map.get(TASK_STATE_WINDOW_END);
        Preconditions.checkArgument(startMs != null,
                "Missing required key '%s' in TaskState map", TASK_STATE_WINDOW_START);
        Preconditions.checkArgument(endMs != null,
                "Missing required key '%s' in TaskState map", TASK_STATE_WINDOW_END);

        Timestamp windowStart = new Timestamp(new Date(Long.parseLong(startMs)));
        Timestamp windowEnd = new Timestamp(new Date(Long.parseLong(endMs)));

        Optional<ChangeId> lastConsumedChangeId = Optional.empty();
        String streamHex = map.get(TASK_STATE_CHANGE_ID_STREAM);
        String changeTimeStr = map.get(TASK_STATE_CHANGE_ID_TIME);
        if (streamHex != null && changeTimeStr != null) {
            StreamId streamId = hexToStreamId(streamHex);
            ChangeTime changeTime = new ChangeTime(UUID.fromString(changeTimeStr));
            lastConsumedChangeId = Optional.of(new ChangeId(streamId, changeTime));
        }

        return new TaskState(windowStart, windowEnd, lastConsumedChangeId);
    }

    // -------------------------------------------------------------------------
    // GenerationId
    // -------------------------------------------------------------------------

    /**
     * Serializes a {@link GenerationId} to a string (epoch milliseconds).
     *
     * @param generationId the generation ID to serialize; must not be null
     * @return the epoch millisecond timestamp as a string
     */
    public static String generationIdToString(GenerationId generationId) {
        Preconditions.checkNotNull(generationId);
        return String.valueOf(generationId.getGenerationStart().toDate().getTime());
    }

    /**
     * Deserializes a {@link GenerationId} from the string produced by
     * {@link #generationIdToString}.
     *
     * @param s the epoch millisecond timestamp string; must not be null
     * @return the deserialized GenerationId
     * @throws NumberFormatException if the string is not a valid long
     */
    public static GenerationId stringToGenerationId(String s) {
        Preconditions.checkNotNull(s);
        return new GenerationId(new Timestamp(new Date(Long.parseLong(s))));
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    static String streamIdToHex(StreamId streamId) {
        ByteBuffer buf = streamId.getValue();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        return BaseEncoding.base16().encode(bytes);
    }

    static StreamId hexToStreamId(String hex) {
        byte[] bytes = BaseEncoding.base16().decode(hex.toUpperCase());
        return new StreamId(ByteBuffer.wrap(bytes));
    }
}
