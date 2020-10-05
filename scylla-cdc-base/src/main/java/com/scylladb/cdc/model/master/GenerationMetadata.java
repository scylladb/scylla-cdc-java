package com.scylladb.cdc.model.master;

import java.util.Objects;
import java.util.Optional;
import java.util.SortedSet;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.Timestamp;

public final class GenerationMetadata {
    private final Timestamp start;
    private final Optional<Timestamp> end;
    private final SortedSet<StreamId> streams;

    public GenerationMetadata(Timestamp start, Optional<Timestamp> end, SortedSet<StreamId> streams) {
        this.start = Preconditions.checkNotNull(start);
        this.end = Preconditions.checkNotNull(end);
        this.streams = Preconditions.checkNotNull(streams);
        Preconditions.checkArgument(!streams.isEmpty());
    }

    public GenerationId getId() {
        return new GenerationId(start);
    }

    public boolean isClosed() {
        return end.isPresent();
    }

    public Timestamp getStart() {
        return start;
    }

    public Optional<Timestamp> getEnd() {
        return end;
    }

    public SortedSet<StreamId> getStreams() {
        return streams;
    }

    public Optional<GenerationId> getNextGenerationId() {
        return isClosed() ? Optional.of(new GenerationId(end.get())) : Optional.empty();
    }

    public GenerationMetadata withEnd(Timestamp end) {
        return new GenerationMetadata(start, Optional.of(end), streams);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof GenerationMetadata)) {
            return false;
        }
        GenerationMetadata o = (GenerationMetadata) other;
        return start.equals(o.start) && end.equals(o.end) && streams.equals(o.streams);
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, end, streams);
    }

    @Override
    public String toString() {
        return String.format("GenerationMetadata(%s, %s, %s)", start, end, streams);
    }
}
