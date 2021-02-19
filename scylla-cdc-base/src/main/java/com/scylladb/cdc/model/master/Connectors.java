package com.scylladb.cdc.model.master;

import com.google.common.base.Preconditions;
import com.scylladb.cdc.cql.MasterCQL;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.transport.MasterTransport;

import java.util.Set;

public class Connectors {
    public final MasterTransport transport;
    public final MasterCQL cql;
    public final Set<TableName> tables;

    public final long sleepBeforeFirstGenerationMs;
    public final long sleepBeforeGenerationDoneMs;
    public final long sleepAfterExceptionMs;

    public Connectors(MasterTransport transport, MasterCQL cql, Set<TableName> tables,
                      long sleepBeforeFirstGenerationMs, long sleepBeforeGenerationDoneMs, long sleepAfterExceptionMs) {
        this.transport = Preconditions.checkNotNull(transport);
        this.cql = Preconditions.checkNotNull(cql);
        this.tables = Preconditions.checkNotNull(tables);
        Preconditions.checkArgument(!tables.isEmpty());

        this.sleepBeforeFirstGenerationMs = sleepBeforeFirstGenerationMs;
        Preconditions.checkArgument(sleepBeforeFirstGenerationMs >= 0);
        this.sleepBeforeGenerationDoneMs = sleepBeforeGenerationDoneMs;
        Preconditions.checkArgument(sleepBeforeGenerationDoneMs >= 0);
        this.sleepAfterExceptionMs = sleepAfterExceptionMs;
        Preconditions.checkArgument(sleepAfterExceptionMs >= 0);
    }
}
