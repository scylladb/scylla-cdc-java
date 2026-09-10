package com.scylladb.cdc.model;

import java.util.Objects;

import com.google.common.base.Preconditions;

public final class TaskId implements Comparable<TaskId> {
    // StreamId reserves 22 bits for vnode indexes. Tablet task indexes use the disjoint range
    // above it so they cannot be confused with checkpoints created by the legacy VNodeId(0) task.
    private static final int TABLET_STREAM_TASK_INDEX_OFFSET = 1 << 22;

    private final GenerationId generationId;
    private final VNodeId vNodeId;
    private final TableName table;

    public TaskId(GenerationId generationId, VNodeId vNodeId, TableName table) {
        this.generationId = Preconditions.checkNotNull(generationId);
        this.vNodeId = Preconditions.checkNotNull(vNodeId);
        this.table = Preconditions.checkNotNull(table);
    }

    /**
     * Creates a task ID for one stream in a tablet generation.
     *
     * <p>The stream index is encoded above the 22-bit vnode index range. This keeps the existing
     * serialized task ID format while separating tablet tasks from vnode and legacy tablet tasks.
     */
    public static TaskId forTabletStream(GenerationId generationId, int streamIndex,
                                         TableName table) {
        Preconditions.checkArgument(streamIndex >= 0, "Stream index must not be negative");
        Preconditions.checkArgument(streamIndex <= Integer.MAX_VALUE - TABLET_STREAM_TASK_INDEX_OFFSET,
                "Stream index is too large");
        return new TaskId(generationId,
                new VNodeId(TABLET_STREAM_TASK_INDEX_OFFSET + streamIndex), table);
    }

    /** Returns whether this task uses the tablet stream task ID namespace. */
    public boolean isTabletStreamTask() {
        return vNodeId.getIndex() >= TABLET_STREAM_TASK_INDEX_OFFSET;
    }

    public GenerationId getGenerationId() {
        return generationId;
    }

    public VNodeId getvNodeId() {
        return vNodeId;
    }

    public TableName getTable() {
        return table;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof TaskId && generationId.equals(((TaskId) o).generationId)
                && vNodeId.equals(((TaskId) o).vNodeId) && table.equals(((TaskId) o).table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(generationId, vNodeId, table);
    }

    @Override
    public String toString() {
        return String.format("TaskId(%s, %s, %s)", generationId, vNodeId, table);
    }

    @Override
    public int compareTo(TaskId o) {
        int cmp = generationId.compareTo(o.generationId);
        if (cmp != 0) {
            return cmp;
        }
        cmp = vNodeId.compareTo(o.vNodeId);
        return cmp != 0 ? cmp : table.compareTo(o.table);
    }
}
