package com.scylladb.cdc.replicator.operations;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.scylladb.cdc.cql.driver3.Driver3FromLibraryTranslator;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.replicator.Main;
import com.scylladb.cdc.replicator.operations.PreparedCdcOperationHandler;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class InsertOperationHandler extends PreparedCdcOperationHandler {

    public InsertOperationHandler(Session session, Driver3FromLibraryTranslator d3t, TableMetadata table) {
        super(session, d3t, table);
    }

    @Override
    protected RegularStatement getStatement(TableMetadata t) {
        Insert builder = QueryBuilder.insertInto(t);
        t.getColumns().stream().forEach(c -> builder.value(c.getName(), bindMarker(c.getName())));
        builder.using(timestamp(bindMarker(TIMESTAMP_MARKER_NAME))).and(ttl(bindMarker(TTL_MARKER_NAME)));
        return builder;
    }

    @Override
    protected void bindInternal(BoundStatement stmt, RawChange c, Main.Mode m) {
        bindTTL(stmt, c);
        bindAllNonCDCColumns(stmt, c, m);
    }

}
