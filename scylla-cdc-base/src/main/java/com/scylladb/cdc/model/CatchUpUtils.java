package com.scylladb.cdc.model;

import com.google.common.base.Preconditions;
import com.google.common.flogger.FluentLogger;

import java.time.Clock;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public final class CatchUpUtils {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private CatchUpUtils() {
    }

    /**
     * Computes the catch-up cutoff date based on the current time and
     * the configured catch-up window size. Returns empty if catch-up
     * is disabled (catchUpWindowSizeSeconds == 0).
     *
     * @param catchUpWindowSizeSeconds the catch-up window in seconds (0 = disabled)
     * @param clock the clock to use for the current time
     * @return the cutoff date, or empty if disabled
     */
    public static Optional<Date> computeCutoff(long catchUpWindowSizeSeconds, Clock clock) {
        if (catchUpWindowSizeSeconds <= 0) {
            return Optional.empty();
        }
        Date now = Date.from(clock.instant());
        long cutoffMs = now.getTime() - TimeUnit.SECONDS.toMillis(catchUpWindowSizeSeconds);
        Date cutoff = new Date(Math.max(cutoffMs, 0));
        logger.atFine().log("Catch-up cutoff computed: %s (now=%s, window=%d seconds)", cutoff, now, catchUpWindowSizeSeconds);
        return Optional.of(cutoff);
    }

    /**
     * Validates that the catch-up window size is within allowed bounds.
     *
     * @param catchUpWindowSizeSeconds the value to validate
     * @param maxSeconds the maximum allowed value
     * @throws IllegalArgumentException if the value is out of range
     */
    public static void validateWindowSize(long catchUpWindowSizeSeconds, long maxSeconds) {
        Preconditions.checkArgument(catchUpWindowSizeSeconds >= 0,
                "catchUpWindowSizeSeconds must be >= 0, got %s", catchUpWindowSizeSeconds);
        Preconditions.checkArgument(catchUpWindowSizeSeconds <= maxSeconds,
                "catchUpWindowSizeSeconds must be <= %s (%d days), got %s",
                maxSeconds, TimeUnit.SECONDS.toDays(maxSeconds), catchUpWindowSizeSeconds);
    }

    /**
     * Attempts to jump to a recent generation using the catch-up optimization.
     * If catch-up is disabled or the CQL query fails, returns empty.
     *
     * @param catchUpCutoff the computed cutoff date (empty if catch-up disabled)
     * @param fetchFn function that takes a cutoff Date and returns a future resolving
     *                to the most recent generation at or before that cutoff
     * @param table the table this operation is for (empty for vnode-based operations)
     * @return the jump target generation, or empty if catch-up is disabled, no generation
     *         was found, or the query failed
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    public static Optional<GenerationId> tryJumpToRecentGeneration(
            Optional<Date> catchUpCutoff,
            Function<Date, CompletableFuture<Optional<GenerationId>>> fetchFn,
            Optional<TableName> table) throws InterruptedException {
        if (!catchUpCutoff.isPresent()) {
            return Optional.empty();
        }
        String context = table.map(t -> " for table " + t).orElse("");
        Date cutoff = catchUpCutoff.get();
        try {
            Optional<GenerationId> jumpTarget = fetchFn.apply(cutoff).get();
            if (jumpTarget.isPresent()) {
                logger.atInfo().log("Catch-up optimization: jumping to generation %s%s (cutoff: %s)",
                        jumpTarget.get(), context, cutoff);
                return jumpTarget;
            }
        } catch (ExecutionException e) {
            logger.atWarning().withCause(e.getCause()).log(
                    "Catch-up CQL query failed%s, falling back to normal generation iteration", context);
        }
        return Optional.empty();
    }
}
