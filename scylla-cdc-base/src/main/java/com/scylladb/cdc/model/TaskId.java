package com.scylladb.cdc.model;

import java.util.Objects;

import com.google.common.base.Preconditions;

public class TaskId implements Comparable<TaskId> {
    private final GenerationId generationId;
    private final VNodeId vNodeId;
    private final TableName table;

    public TaskId(GenerationId generationId, VNodeId vNodeId, TableName table) {
        this.generationId = Preconditions.checkNotNull(generationId);
        this.vNodeId = Preconditions.checkNotNull(vNodeId);
        this.table = Preconditions.checkNotNull(table);
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
