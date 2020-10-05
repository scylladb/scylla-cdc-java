package com.scylladb.cdc.model;

import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;
import com.google.common.io.BaseEncoding;

public final class StreamId implements Comparable<StreamId> {
    private final ByteBuffer value;

    public StreamId(ByteBuffer value) {
        this.value = Preconditions.checkNotNull(value);
    }

    public VNodeId getVNodeId() {
        // TODO - should validate version
        long lowerDword = value.getLong(value.position() + 8);
        long vNodeId = (lowerDword & 0x3FFFFF0) >> 4;
        return new VNodeId((int) vNodeId);
    }

    public ByteBuffer getValue() {
        return value.asReadOnlyBuffer();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof StreamId && value.equals(((StreamId) o).value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public String toString() {
        byte[] buf = new byte[16];
        value.duplicate().get(buf, 0, 16);
        return String.format("StreamId(%s)", BaseEncoding.base16().encode(buf, 0, 16));
    }

    @Override
    public int compareTo(StreamId o) {
        return value.compareTo(o.value);
    }
}
