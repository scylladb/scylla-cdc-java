package com.scylladb.cdc.model.worker;

import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

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
        return TimeUUID.startOf(windowStart.toDate().getTime());
    }

    /**
     * Returns the timestamp of the start of the query window.
     * <p>
     * This timestamp represents a inclusive lower bound
     * of changes defined by this <code>TaskState</code>.
     *
     * @return the timestamp of the start of the query window.
     * @see #getWindowEndTimestamp()
     */
    public Timestamp getWindowStartTimestamp() {
        return windowStart;
    }

    public boolean hasPassed(Timestamp t) {
        return windowStart.compareTo(t) > 0;
    }

    public UUID getWindowEnd() {
        // Without -1, we would be reading windows 1ms too long.
        return TimeUUID.endOf(windowEnd.toDate().getTime() - 1);
    }

    public Timestamp getWindowEndTimestamp() {
        return windowEnd;
    }

    public TaskState moveToNextWindow(long nextWindowSizeMs) {
        return new TaskState(windowEnd, windowEnd.plus(nextWindowSizeMs, ChronoUnit.MILLIS), Optional.empty());
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
        return String.format("TaskState(%s (%s), %s (%s), %s)", getWindowStart(), windowStart, getWindowEnd(),
                windowEnd, lastConsumedChangeId);
    }

    /*
     * Creates an initial state for tasks in a given |generation|.
     *
     * Such initial state starts at the beginning of the generation and spans for
     * |windowSizeMs| milliseconds.
     */
    public static TaskState createInitialFor(GenerationId generation, long windowSizeMs) {
        Timestamp generationStart = generation.getGenerationStart();
        return new TaskState(generationStart, generationStart.plus(windowSizeMs, ChronoUnit.MILLIS), Optional.empty());
    }

    /* If the state is before |minimumWindowStart| then this method returns a state
     * starting at |minimumWindowStart| and spanning for |windowSizeMs| milliseconds.
     * Otherwise, the original state is returned. An intended use for |minimumWindowStart|
     * is when you are sure that there aren't any changes before |minimumWindowStart|
     * (for example due to TTL) and don't want to have a state that will span a range
     * without any changes.
     */
    public TaskState trimTaskState(Timestamp minimumWindowStart, long windowSizeMs) {
        // If the entire state is before minimumWindowStart,
        // return a new state starting at minimumWindowStart.
        if (this.windowEnd.compareTo(minimumWindowStart) < 0) {
            return new TaskState(minimumWindowStart, minimumWindowStart.plus(windowSizeMs, ChronoUnit.MILLIS), Optional.empty());
        }

        return this;
    }
}
