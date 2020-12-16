package com.scylladb.cdc.replicator.operations;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.io.BaseEncoding;
import com.scylladb.cdc.cql.driver3.Driver3FromLibraryTranslator;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.cql.Cell;
import com.scylladb.cdc.replicator.Main;
import com.scylladb.cdc.replicator.ReplicatorConsumer;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class RangeDeleteEndOperationHandler implements CdcOperationHandler {
    private final TableMetadata table;
    private final RangeDeleteState state;
    private final Driver3FromLibraryTranslator driver3FromLibraryTranslator;
    private final boolean endInclusive;

    public RangeDeleteEndOperationHandler(Session session, TableMetadata t, Driver3FromLibraryTranslator driver3FromLibraryTranslator, RangeDeleteState state, boolean inclusive) {
        this.table = t;
        this.state = state;
        this.driver3FromLibraryTranslator = driver3FromLibraryTranslator;
        this.endInclusive = inclusive;
    }

    @Override
    public Statement getStatement(RawChange c, ConsistencyLevel cl, Main.Mode m) {
        byte[] streamId = new byte[16];
        c.getId().getStreamId().getValue().duplicate().get(streamId, 0, 16);
        RangeDeleteState.DeletionStart start = state.getStart(streamId);
        if (start == null) {
            throw new IllegalStateException("Got range deletion end but no start in stream " + BaseEncoding.base16().encode(streamId, 0, 16));
        }

        Delete builder = QueryBuilder.delete().from(table);
        Iterator<ColumnMetadata> keyIt = table.getPrimaryKey().iterator();
        ColumnMetadata prevCol = keyIt.next();
        Cell startCell = start.val.change.getCell(prevCol.getName());
        Cell endCell = c.getCell(prevCol.getName());

        while (keyIt.hasNext()) {
            ColumnMetadata nextCol = keyIt.next();
            Cell newStartCell = start.val.change.getCell(nextCol.getName());
            Cell newEndCell = c.getCell(nextCol.getName());

            if (newStartCell.getAsObject() == null && newEndCell.getAsObject() == null) {
                break;
            }

            builder.where(eq(prevCol.getName(), driver3FromLibraryTranslator.translate(startCell)));

            startCell = newStartCell;
            endCell = newEndCell;
            prevCol = nextCol;
        }

        if (startCell.getAsObject() != null) {
            builder.where(start.inclusive ? gte(prevCol.getName(), driver3FromLibraryTranslator.translate(startCell))
                    : gt(prevCol.getName(), driver3FromLibraryTranslator.translate(startCell)));
        }
        if (endCell.getAsObject() != null) {
            builder.where(endInclusive ? lte(prevCol.getName(), driver3FromLibraryTranslator.translate(endCell))
                    : lt(prevCol.getName(), driver3FromLibraryTranslator.translate(endCell)));
        }
        Long ttl = c.getTTL();
        if (ttl != null) {
            builder.using(timestamp(c.getId().getChangeTime().getTimestamp())).and(ttl((int) ((long) ttl)));
        } else {
            builder.using(timestamp(c.getId().getChangeTime().getTimestamp()));
        }
        builder.setConsistencyLevel(cl);
        builder.setIdempotent(true);
        return builder;
    }

}
