package com.scylladb.cdc.model;

import com.google.common.base.Preconditions;

public final class GenerationId implements Comparable<GenerationId> {
    private final Timestamp generationStart; // Generations are "recognized" by their start time

    public GenerationId(Timestamp generationStart) {
        this.generationStart = Preconditions.checkNotNull(generationStart);
    }

    public Timestamp getGenerationStart() {
        return generationStart;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof GenerationId && generationStart.equals(((GenerationId) o).generationStart);
    }

    @Override
    public int hashCode() {
        return generationStart.hashCode();
    }

    @Override
    public String toString() {
        return String.format("GenerationId(%s)", generationStart);
    }

    @Override
    public int compareTo(GenerationId o) {
        return generationStart.compareTo(o.generationStart);
    }
}
