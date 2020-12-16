package com.scylladb.cdc.replicator.operations;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.scylladb.cdc.cql.driver3.Driver3FromLibraryTranslator;
import com.scylladb.cdc.model.worker.RawChange;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class PartitionDeleteOperationHandler extends PreparedCdcOperationHandler {

    protected RegularStatement getStatement(TableMetadata t) {
        Delete builder = QueryBuilder.delete().from(t);
        t.getPartitionKey().stream().forEach(c -> builder.where(eq(c.getName(), bindMarker(c.getName()))));
        builder.using(timestamp(bindMarker(TIMESTAMP_MARKER_NAME)));
        return builder;
    }

    public PartitionDeleteOperationHandler(Session session, Driver3FromLibraryTranslator d3t, TableMetadata table) {
        super(session, d3t, table);
    }

    @Override
    protected void bindInternal(BoundStatement stmt, RawChange c) {
        bindPartitionKeyColumns(stmt, c);
    }

}
