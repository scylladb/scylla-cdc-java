package com.scylladb.cdc.model;

public final class VNodeId implements Comparable<VNodeId> {
    // Index in token ring. In our implementation this will be parsed from
    // stream_id.
    private final int index;

    public VNodeId(int index) {
        this.index = index;
    }

    public int getIndex() {
        return index;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof VNodeId && index == ((VNodeId) o).index;
    }

    @Override
    public int hashCode() {
        return index;
    }

    @Override
    public String toString() {
        return String.format("VNodeId(%d)", index);
    }

    @Override
    public int compareTo(VNodeId o) {
        return index - o.index;
    }

}
