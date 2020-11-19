package com.scylladb.cdc.model.worker;

import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.base.Preconditions;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.Timestamp;

public final class TaskState {
    private final Timestamp windowStart;
    private final Timestamp windowEnd;
    private final Optional<ChangeId> lastConsumedChangeId;

    public TaskState(Timestamp start, Timestamp end, Optional<ChangeId> lastConsumed) {
        windowStart = Preconditions.checkNotNull(start);
        windowEnd = Preconditions.checkNotNull(end);
        lastConsumedChangeId = Preconditions.checkNotNull(lastConsumed);
    }

    public Optional<ChangeId> getLastConsumedChangeId() {
        return lastConsumedChangeId;
    }

    public UUID getWindowStart() {
        return UUIDs.startOf(windowStart.toDate().getTime());
    }

    public boolean hasPassed(Timestamp t) {
        return windowStart.compareTo(t) > 0;
    }

    public UUID getWindowEnd() {
        return UUIDs.endOf(windowEnd.toDate().getTime());
    }

    public TaskState moveToNextWindow() {
        return new TaskState(windowEnd, windowEnd.plus(30, ChronoUnit.SECONDS), Optional.empty());
    }

    public TaskState update(ChangeId seen) {
        return new TaskState(windowStart, windowEnd, Optional.of(seen));
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof TaskState)) {
            return false;
        }
        TaskState o = (TaskState) other;
        return windowStart.equals(o.windowStart) && windowEnd.equals(o.windowEnd)
                && lastConsumedChangeId.equals(o.lastConsumedChangeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(windowStart, windowEnd, lastConsumedChangeId);
    }

    @Override
    public String toString() {
        return String.format("TaskState(%s, %s, %s)", windowStart, windowEnd, lastConsumedChangeId);
    }

    /*
     * Creates an initial state for tasks in a given |generation|.
     *
     * Such initial state starts at the beginning of the generation and spans for
     * 30s.
     */
    public static TaskState createInitialFor(GenerationId generation) {
        Timestamp generationStart = generation.getGenerationStart();
        return new TaskState(generationStart, generationStart.plus(30, ChronoUnit.SECONDS), Optional.empty());
    }
}
