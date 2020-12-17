package com.scylladb.cdc.replicator.operations.update;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.scylladb.cdc.cql.driver3.Driver3FromLibraryTranslator;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.replicator.operations.ExecutingPreparedStatementHandler;

import java.util.HashSet;
import java.util.Set;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class PreparedUpdateOperationHandler extends ExecutingPreparedStatementHandler {

    protected RegularStatement getStatement(TableMetadata tableMetadata) {
        // Build an UPDATE prepared statement:
        //
        // UPDATE table
        // USING TIMESTAMP ? AND TTL ?
        // SET v1 = ? AND v2 = ? AND v3 = ? ...
        // WHERE pk1 = ? AND pk2 = ? ... AND ck1 = ? AND ck2 = ? ...

        Update builder = QueryBuilder.update(tableMetadata);
        Set<ColumnMetadata> primaryColumns = new HashSet<>(tableMetadata.getPrimaryKey());
        tableMetadata.getColumns().forEach(column -> {
            if (primaryColumns.contains(column)) {
                // SET v_i = ?
                builder.where(eq(column.getName(), bindMarker(column.getName())));
            } else {
                // WHERE pk_i = ?
                // WHERE ck_i = ?
                builder.with(set(column.getName(), bindMarker(column.getName())));
            }
        });

        builder.using(timestamp(bindMarker(TIMESTAMP_MARKER_NAME))).and(ttl(bindMarker(TTL_MARKER_NAME)));
        return builder;
    }

    @Override
    protected void bindInternal(BoundStatement statement, RawChange change) {
        // We prepared an UPDATE statement with all columns, so
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

    public PreparedUpdateOperationHandler(Session session, Driver3FromLibraryTranslator driver3FromLibraryTranslator,
                                          TableMetadata tableMetadata) {
        super(session, driver3FromLibraryTranslator, tableMetadata);
    }
}
