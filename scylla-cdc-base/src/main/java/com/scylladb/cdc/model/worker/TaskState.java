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

    public TaskState moveToNextWindow(Timestamp now, long confidenceWindowSizeMs, long newQueryWindowSizeMs) {
        Timestamp newWindowEnd = now.plus(-confidenceWindowSizeMs, ChronoUnit.MILLIS);

        // Make sure that the window is at least newQueryWindowSizeMs long.
        long windowLength = ChronoUnit.MILLIS.between(windowEnd.toDate().toInstant(), newWindowEnd.toDate().toInstant());
        if (windowLength < newQueryWindowSizeMs) {
            newWindowEnd = windowEnd.plus(newQueryWindowSizeMs, ChronoUnit.MILLIS);
        }

        return new TaskState(windowEnd, newWindowEnd, Optional.empty());
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
     * Such initial state starts at the beginning of the generation and spans
     * until now minus confidence window size.
     */
    public static TaskState createInitialFor(GenerationId generation, Timestamp now,
                                             long confidenceWindowSizeMs, long queryTimeWindowSizeMs) {
        // Start reading at generation start:
        Timestamp windowStart = generation.getGenerationStart();

        // Create a large window up to (now - confidenceWindowSizeMs), except
        // when the window gets too small - in that case create a window
        // queryTimeWindowSizeMs large (the consumer might need to wait a bit
        // for the window to be ready for reading).
        Timestamp windowEnd = now.plus(-confidenceWindowSizeMs, ChronoUnit.MILLIS);
        if (windowEnd.compareTo(windowStart) < 0) {
            windowEnd = windowStart.plus(queryTimeWindowSizeMs, ChronoUnit.MILLIS);
        }

        return new TaskState(windowStart, windowEnd, Optional.empty());
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

        // Trim the start of the window with minimumWindowStart.
        Timestamp newWindowStart = windowStart;
        if (newWindowStart.compareTo(minimumWindowStart) < 0) {
            newWindowStart = minimumWindowStart;
        }

        return new TaskState(newWindowStart, windowEnd, lastConsumedChangeId);
    }
}
