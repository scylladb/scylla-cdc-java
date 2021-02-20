package com.scylladb.cdc.model.master;

import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.Timestamp;

import java.nio.ByteBuffer;
import java.util.*;

public class MockGenerationMetadata {

    public static GenerationMetadata mockGenerationMetadata(long start, Optional<Long> end, int vnodeCount) {
        // Scale start, end to minutes.
        final long TIMESTAMP_SCALING = 1000 * 60;
        start = start * TIMESTAMP_SCALING;
        end = end.map(e -> e * TIMESTAMP_SCALING);

        return mockGenerationMetadata(new Timestamp(new Date(start)), end.map(Date::new).map(Timestamp::new), vnodeCount);
    }

    public static GenerationMetadata mockGenerationMetadata(Timestamp start, Optional<Timestamp> end, int vnodeCount) {
        // Random with deterministic seed
        Random random = new Random(start.toDate().getTime());

        SortedSet<StreamId> streams = new TreeSet<>();
        for (int vnode = 0; vnode < vnodeCount; vnode++) {
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
        return new GenerationMetadata(start, end, streams);
    }
}
