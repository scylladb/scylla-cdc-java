package com.scylladb.cdc.model;

import com.google.common.base.Preconditions;

import java.time.Clock;
import java.time.Duration;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Shared catch-up configuration used by both Master and Worker.
 * Encapsulates the catch-up window size and provides methods
 * for computing the cutoff date and validating the window size.
 */
public final class CatchUpConfiguration {
    /**
     * Maximum allowed catch-up window in seconds (90 days). This cap prevents
     * excessively large catch-up windows that would result in massive probe
     * scans across many CDC partitions, most of which would contain only
     * expired tombstones. For longer retention needs, the window should not
     * exceed the table's CDC TTL.
     */
    public static final long MAX_CATCH_UP_WINDOW_SIZE_SECONDS = TimeUnit.DAYS.toSeconds(90);
    public static final long DEFAULT_PROBE_TIMEOUT_SECONDS = 30;
    /**
     * Maximum allowed probe timeout in seconds. Limited by the driver's
     * {@code setReadTimeoutMillis(int)} which accepts an {@code int} of
     * milliseconds. {@code Integer.MAX_VALUE / 1000} gives ~24.8 days.
     */
    public static final long MAX_PROBE_TIMEOUT_SECONDS = Integer.MAX_VALUE / 1000L;

    private final long catchUpWindowSizeSeconds;
    private final long probeTimeoutSeconds;

    CatchUpConfiguration(long catchUpWindowSizeSeconds, long probeTimeoutSeconds) {
        CatchUpUtils.validateWindowSize(catchUpWindowSizeSeconds, MAX_CATCH_UP_WINDOW_SIZE_SECONDS);
        Preconditions.checkArgument(probeTimeoutSeconds > 0, "probeTimeoutSeconds must be positive");
        Preconditions.checkArgument(probeTimeoutSeconds <= MAX_PROBE_TIMEOUT_SECONDS,
                "probeTimeoutSeconds must be <= %s (~24 days), got %s", MAX_PROBE_TIMEOUT_SECONDS, probeTimeoutSeconds);
        this.catchUpWindowSizeSeconds = catchUpWindowSizeSeconds;
        this.probeTimeoutSeconds = probeTimeoutSeconds;
    }

    public long getCatchUpWindowSizeSeconds() {
        return catchUpWindowSizeSeconds;
    }

    public long getProbeTimeoutSeconds() {
        return probeTimeoutSeconds;
    }

    public Optional<Date> computeCatchUpCutoff(Clock clock) {
        return CatchUpUtils.computeCutoff(catchUpWindowSizeSeconds, clock);
    }

    public boolean isEnabled() {
        return catchUpWindowSizeSeconds > 0;
    }

    /**
     * Reusable builder for catch-up configuration, embedded in both
     * {@code MasterConfiguration.Builder} and {@code WorkerConfiguration.Builder}.
     */
    public static final class Builder {
        private long catchUpWindowSizeSeconds = 0;
        private long probeTimeoutSeconds = DEFAULT_PROBE_TIMEOUT_SECONDS;

        public long getCatchUpWindowSizeSeconds() {
            return catchUpWindowSizeSeconds;
        }

        public void setCatchUpWindowSizeSeconds(long catchUpWindowSizeSeconds) {
            CatchUpUtils.validateWindowSize(catchUpWindowSizeSeconds, MAX_CATCH_UP_WINDOW_SIZE_SECONDS);
            this.catchUpWindowSizeSeconds = catchUpWindowSizeSeconds;
        }

        public void setCatchUpWindow(Duration catchUpWindow) {
            Preconditions.checkNotNull(catchUpWindow);
            Preconditions.checkArgument(!catchUpWindow.isNegative(), "catchUpWindow must not be negative");
            Preconditions.checkArgument(catchUpWindow.getNano() == 0,
                    "catchUpWindow has sub-second precision (nano=%d in %s); "
                    + "use a whole-second Duration (e.g. Duration.ofSeconds(%d)) or "
                    + "withCatchUpWindowSizeSeconds() instead",
                    catchUpWindow.getNano(), catchUpWindow, catchUpWindow.getSeconds());
            setCatchUpWindowSizeSeconds(catchUpWindow.getSeconds());
        }

        public void setProbeTimeoutSeconds(long probeTimeoutSeconds) {
            Preconditions.checkArgument(probeTimeoutSeconds > 0, "probeTimeoutSeconds must be positive");
            Preconditions.checkArgument(probeTimeoutSeconds <= MAX_PROBE_TIMEOUT_SECONDS,
                    "probeTimeoutSeconds must be <= %s (~24 days), got %s", MAX_PROBE_TIMEOUT_SECONDS, probeTimeoutSeconds);
            this.probeTimeoutSeconds = probeTimeoutSeconds;
        }

        public CatchUpConfiguration build() {
            return new CatchUpConfiguration(catchUpWindowSizeSeconds, probeTimeoutSeconds);
        }
    }
}
