package com.scylladb.cdc.replicator.operations;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.scylladb.cdc.cql.driver3.Driver3FromLibraryTranslator;
import com.scylladb.cdc.model.worker.RawChange;

import java.util.HashSet;
import java.util.Set;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class PreparedUpdateOperationHandler extends PreparedCdcOperationHandler {

    protected RegularStatement getStatement(TableMetadata t) {
        Update builder = QueryBuilder.update(t);
        Set<ColumnMetadata> primaryColumns = new HashSet<>(t.getPrimaryKey());
        t.getColumns().stream().forEach(c -> {
            if (primaryColumns.contains(c)) {
                builder.where(eq(c.getName(), bindMarker(c.getName())));
            } else {
                builder.with(set(c.getName(), bindMarker(c.getName())));
            }
        });

        builder.using(timestamp(bindMarker(TIMESTAMP_MARKER_NAME))).and(ttl(bindMarker(TTL_MARKER_NAME)));
        return builder;
    }

    public PreparedUpdateOperationHandler(Session session, Driver3FromLibraryTranslator d3t, TableMetadata table) {
        super(session, d3t, table);
    }

    @Override
    protected void bindInternal(BoundStatement stmt, RawChange c) {
        bindTTL(stmt, c);
        bindAllNonCDCColumns(stmt, c);
    }

}
