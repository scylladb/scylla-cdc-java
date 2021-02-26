package com.scylladb.cdc.model.master;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.cql.MasterCQL;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.transport.MasterTransport;

import java.time.Clock;
import java.util.Collection;
import java.util.HashSet;
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

    private MasterConfiguration(MasterTransport transport, MasterCQL cql, Set<TableName> tables, Clock clock,
                               long sleepBeforeFirstGenerationMs, long sleepBeforeGenerationDoneMs, long sleepAfterExceptionMs) {
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

        public Builder withClock(Clock clock) {
            this.clock = Preconditions.checkNotNull(clock);
            return this;
        }

        public MasterConfiguration build() {
            return new MasterConfiguration(transport, cql, tables, clock,
                    sleepBeforeFirstGenerationMs, sleepBeforeGenerationDoneMs, sleepAfterExceptionMs);
        }
    }
}
