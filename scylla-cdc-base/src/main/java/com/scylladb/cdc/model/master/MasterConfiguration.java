package com.scylladb.cdc.model.master;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.cql.MasterCQL;
import com.scylladb.cdc.model.CatchUpConfiguration;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.transport.MasterTransport;

import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class MasterConfiguration {
    public static final long DEFAULT_SLEEP_BEFORE_FIRST_GENERATION_MS = TimeUnit.SECONDS.toMillis(10);
    public static final long DEFAULT_SLEEP_BEFORE_GENERATION_DONE_MS = TimeUnit.SECONDS.toMillis(30);
    public static final long DEFAULT_SLEEP_AFTER_EXCEPTION_MS = TimeUnit.SECONDS.toMillis(10);
    public final MasterTransport transport;
    public final MasterCQL cql;
    public final Set<TableName> tables;
    public final Clock clock;

    public final long sleepBeforeFirstGenerationMs;
    public final long sleepBeforeGenerationDoneMs;
    public final long sleepAfterExceptionMs;
    public final CatchUpConfiguration catchUpConfig;

    private MasterConfiguration(MasterTransport transport, MasterCQL cql, Set<TableName> tables, Clock clock,
                               long sleepBeforeFirstGenerationMs, long sleepBeforeGenerationDoneMs, long sleepAfterExceptionMs,
                               CatchUpConfiguration catchUpConfig) {
        this.transport = Preconditions.checkNotNull(transport);
        this.cql = Preconditions.checkNotNull(cql);
        this.tables = Preconditions.checkNotNull(tables);
        Preconditions.checkArgument(!tables.isEmpty());
        this.clock = Preconditions.checkNotNull(clock);

        this.sleepBeforeFirstGenerationMs = sleepBeforeFirstGenerationMs;
        Preconditions.checkArgument(sleepBeforeFirstGenerationMs >= 0);
        this.sleepBeforeGenerationDoneMs = sleepBeforeGenerationDoneMs;
        Preconditions.checkArgument(sleepBeforeGenerationDoneMs >= 0);
        this.sleepAfterExceptionMs = sleepAfterExceptionMs;
        Preconditions.checkArgument(sleepAfterExceptionMs >= 0);
        this.catchUpConfig = Preconditions.checkNotNull(catchUpConfig);
    }

    /**
     * Computes the catch-up cutoff date based on the current time and
     * the configured catch-up window size. Returns empty if catch-up
     * is disabled (catchUpWindowSizeSeconds == 0).
     */
    public Optional<Date> computeCatchUpCutoff() {
        return catchUpConfig.computeCatchUpCutoff(clock);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private MasterTransport transport;
        private MasterCQL cql;
        private Set<TableName> tables = new HashSet<>();

        private long sleepBeforeFirstGenerationMs = DEFAULT_SLEEP_BEFORE_FIRST_GENERATION_MS;
        private long sleepBeforeGenerationDoneMs = DEFAULT_SLEEP_BEFORE_GENERATION_DONE_MS;
        private long sleepAfterExceptionMs = DEFAULT_SLEEP_AFTER_EXCEPTION_MS;

        private final CatchUpConfiguration.Builder catchUpHelper = new CatchUpConfiguration.Builder();

        private Clock clock = Clock.systemDefaultZone();

        public Builder withTransport(MasterTransport transport) {
            this.transport = Preconditions.checkNotNull(transport);
            return this;
        }

        public Builder withCQL(MasterCQL cql) {
            this.cql = Preconditions.checkNotNull(cql);
            return this;
        }

        public Builder addTable(TableName table) {
            Preconditions.checkNotNull(table);
            this.tables.add(table);
            return this;
        }

        public Builder addTables(Collection<TableName> tables) {
            for (TableName table : tables) {
                this.addTable(table);
            }
            return this;
        }

        public Builder withSleepBeforeFirstGenerationMs(long sleepBeforeFirstGenerationMs) {
            Preconditions.checkArgument(sleepBeforeFirstGenerationMs >= 0);
            this.sleepBeforeFirstGenerationMs = sleepBeforeFirstGenerationMs;
            return this;
        }

        public Builder withSleepBeforeGenerationDoneMs(long sleepBeforeGenerationDoneMs) {
            Preconditions.checkArgument(sleepBeforeGenerationDoneMs >= 0);
            this.sleepBeforeGenerationDoneMs = sleepBeforeGenerationDoneMs;
            return this;
        }

        public Builder withSleepAfterExceptionMs(long sleepAfterExceptionMs) {
            Preconditions.checkArgument(sleepAfterExceptionMs >= 0);
            this.sleepAfterExceptionMs = sleepAfterExceptionMs;
            return this;
        }

        /**
         * Sets the catch-up window size in seconds. When greater than zero,
         * enables catch-up optimization for first-time startup, allowing the
         * master to jump directly to a recent generation instead of iterating
         * through all old generations.
         *
         * @param catchUpWindowSizeSeconds the catch-up window size in seconds (0 = disabled)
         * @return this builder
         */
        public Builder withCatchUpWindowSizeSeconds(long catchUpWindowSizeSeconds) {
            catchUpHelper.setCatchUpWindowSizeSeconds(catchUpWindowSizeSeconds);
            return this;
        }

        /**
         * Sets the catch-up window size using a {@link Duration}. This is
         * equivalent to {@link #withCatchUpWindowSizeSeconds(long)} but more
         * idiomatic for Java 8+ callers. Sub-second precision is truncated.
         *
         * @param catchUpWindow the catch-up window duration (0 = disabled)
         * @return this builder
         */
        public Builder withCatchUpWindow(Duration catchUpWindow) {
            catchUpHelper.setCatchUpWindow(catchUpWindow);
            return this;
        }

        public Builder withClock(Clock clock) {
            this.clock = Preconditions.checkNotNull(clock);
            return this;
        }

        public MasterConfiguration build() {
            return new MasterConfiguration(transport, cql, tables, clock,
                    sleepBeforeFirstGenerationMs, sleepBeforeGenerationDoneMs, sleepAfterExceptionMs,
                    catchUpHelper.build());
        }
    }
}
