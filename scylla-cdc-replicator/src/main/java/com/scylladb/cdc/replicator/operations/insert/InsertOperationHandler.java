package com.scylladb.cdc.replicator.operations.insert;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.scylladb.cdc.cql.driver3.Driver3FromLibraryTranslator;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.replicator.operations.ExecutingPreparedStatementHandler;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class InsertOperationHandler extends ExecutingPreparedStatementHandler {
    @Override
    protected RegularStatement getStatement(TableMetadata tableMetadata) {
        // Build an INSERT prepared statement:
        //
        // INSERT INTO table([all_columns]) VALUES ([all_column_values]) USING TIMESTAMP ? AND TTL ?

        Insert builder = QueryBuilder.insertInto(tableMetadata);
        tableMetadata.getColumns().forEach(c -> builder.value(c.getName(), bindMarker(c.getName())));
        builder.using(timestamp(bindMarker(TIMESTAMP_MARKER_NAME))).and(ttl(bindMarker(TTL_MARKER_NAME)));
        return builder;
    }

    @Override
    protected void bindInternal(BoundStatement statement, RawChange change) {
        // We prepared an INSERT statement with all columns, so
        // we can easily bind all values from change.
        //
        // If the change does not specify all column values,
        // the bound statement will have those values
        // unset - not modifying already present cells
        // in the destination table (protocol v4 guarantee).
        bindAllNonCDCColumns(statement, change);

        bindTTL(statement, change);

        // The TIMESTAMP value is set by ExecutingPreparedStatementHandler.
    }

    public InsertOperationHandler(Session session, Driver3FromLibraryTranslator driver3FromLibraryTranslator,
                                  TableMetadata tableMetadata) {
        super(session, driver3FromLibraryTranslator, tableMetadata);
    }
}
