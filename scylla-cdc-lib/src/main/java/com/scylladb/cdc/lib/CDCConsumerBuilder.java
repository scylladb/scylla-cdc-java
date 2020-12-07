package com.scylladb.cdc.lib;

import java.util.Set;

import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.worker.RawChangeConsumer;

public final class CDCConsumerBuilder {
    private final Session session;
    private final RawChangeConsumer consumer;
    private final Set<TableName> tables;
    private int workersCount = getDefaultWorkersCount();

    private CDCConsumerBuilder(Session session, RawChangeConsumer consumer, Set<TableName> tables) {
        this.consumer = Preconditions.checkNotNull(consumer);
        Preconditions.checkNotNull(tables);
        Preconditions.checkArgument(!tables.isEmpty());
        this.tables = tables;
        this.session = Preconditions.checkNotNull(session);
    }

    public static CDCConsumerBuilder builder(Session session, RawChangeConsumer consumer, Set<TableName> tables) {
        return new CDCConsumerBuilder(session, consumer, tables);
    }

    public CDCConsumerBuilder workersCount(int count) {
        Preconditions.checkArgument(count > 0);
        workersCount = count;
        return this;
    }

    public CDCConsumer build() {
        return new CDCConsumer(session, consumer, tables, workersCount);
    }

    private static int getDefaultWorkersCount() {
        int result = Runtime.getRuntime().availableProcessors() - 1;
        return result > 0 ? result : 1;
    }

}
