package com.scylladb.cdc.replicator.operations.delete;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.scylladb.cdc.cql.driver3.Driver3FromLibraryTranslator;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.replicator.operations.ExecutingPreparedStatementHandler;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class RowDeleteOperationHandler extends ExecutingPreparedStatementHandler {

    protected RegularStatement getStatement(TableMetadata tableMetadata) {
        // Build a DELETE prepared statement:
        //
        // DELETE FROM table WHERE pk1 = ? AND pk2 = ? ... AND ck1 = ? AND ck2 = ? ... USING TIMESTAMP ?

        Delete builder = QueryBuilder.delete().from(tableMetadata);
        tableMetadata.getPrimaryKey().forEach(c -> builder.where(eq(c.getName(), bindMarker(c.getName()))));
        builder.using(timestamp(bindMarker(TIMESTAMP_MARKER_NAME)));
        return builder;
    }

    @Override
    protected void bindInternal(BoundStatement statement, RawChange change) {
        // As this is a row delete operation, the whole primary key
        // (partition key, clustering key) is known due to the fact
        // that deleted row has to be uniquely identified.
        bindPrimaryKeyColumns(statement, change);

        // There is no need to bind the TTL value for DELETEs.
        // The TIMESTAMP value is set by ExecutingPreparedStatementHandler.
    }

    public RowDeleteOperationHandler(Session session, Driver3FromLibraryTranslator driver3FromLibraryTranslator,
                                     TableMetadata tableMetadata) {
        super(session, driver3FromLibraryTranslator, tableMetadata);
    }
}
