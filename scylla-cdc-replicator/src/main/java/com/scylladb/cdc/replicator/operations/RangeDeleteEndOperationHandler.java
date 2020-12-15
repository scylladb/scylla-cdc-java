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
    private final ReplicatorConsumer.RangeTombstoneState state;
    private final Map<Integer, Map<Boolean, PreparedStatement>> stmts = new HashMap<>();
    private final Driver3FromLibraryTranslator driver3FromLibraryTranslator;

    private static PreparedStatement prepare(Session s, TableMetadata t, int prefixSize, boolean startInclusive,
                                             boolean endInclusive) {
        Delete builder = QueryBuilder.delete().from(t);
        List<ColumnMetadata> pk = t.getPrimaryKey();
        pk.subList(0, prefixSize).stream().forEach(c -> builder.where(eq(c.getName(), bindMarker(c.getName()))));
        ColumnMetadata lastCk = pk.get(prefixSize);
        builder.where(startInclusive ? gte(lastCk.getName(), bindMarker(lastCk.getName() + "_start"))
                : gt(lastCk.getName(), bindMarker(lastCk.getName() + "_start")));
        builder.where(endInclusive ? lte(lastCk.getName(), bindMarker(lastCk.getName() + "_end"))
                : lt(lastCk.getName(), bindMarker(lastCk.getName() + "_end")));
        builder.using(timestamp(bindMarker(ReplicatorConsumer.TIMESTAMP_MARKER_NAME)));
        return s.prepare(builder);
    }

    public RangeDeleteEndOperationHandler(Session session, TableMetadata t, Driver3FromLibraryTranslator driver3FromLibraryTranslator, ReplicatorConsumer.RangeTombstoneState state, boolean inclusive) {
        table = t;
        this.state = state;
        this.driver3FromLibraryTranslator = driver3FromLibraryTranslator;
        for (int i = t.getPartitionKey().size(); i < t.getPrimaryKey().size(); ++i) {
            Map<Boolean, PreparedStatement> map = new HashMap<>();
            map.put(true, prepare(session, t, i, true, inclusive));
            map.put(false, prepare(session, t, i, false, inclusive));
            stmts.put(i + 1, map);
        }
    }

    private static Statement bind(TableMetadata table, Driver3FromLibraryTranslator driver3FromLibraryTranslator, PreparedStatement stmt, RawChange change, ReplicatorConsumer.PrimaryKeyValue startVal, ConsistencyLevel cl) {
        BoundStatement s = stmt.bind();
        Iterator<ColumnMetadata> keyIt = table.getPrimaryKey().iterator();
        ColumnMetadata prevCol = keyIt.next();
        Cell end = change.getCell(prevCol.getName());
        Cell start = startVal.change.getCell(prevCol.getName());
        while (keyIt.hasNext()) {
            ColumnMetadata col = keyIt.next();
            Cell newStart = startVal.change.getCell(col.getName());
            if (newStart.getAsObject() == null) {
                break;
            }
            ReplicatorConsumer.setBytesUnsafe(driver3FromLibraryTranslator, s, prevCol.getName(), end);
            end = change.getCell(col.getName());
            prevCol = col;
            start = newStart;
        }
        ReplicatorConsumer.setBytesUnsafe(driver3FromLibraryTranslator, s, prevCol.getName() + "_start", start);
        ReplicatorConsumer.setBytesUnsafe(driver3FromLibraryTranslator, s, prevCol.getName() + "_end", end);
        s.setLong(ReplicatorConsumer.TIMESTAMP_MARKER_NAME, change.getId().getChangeTime().getTimestamp());
        s.setConsistencyLevel(cl);
        s.setIdempotent(true);
        return s;
    }

    @Override
    public Statement getStatement(RawChange c, ConsistencyLevel cl, Main.Mode m) {
        byte[] streamId = new byte[16];
        c.getId().getStreamId().getValue().duplicate().get(streamId, 0, 16);
        ReplicatorConsumer.RangeTombstoneState.DeletionStart start = state.getStart(streamId);
        if (start == null) {
            throw new IllegalStateException("Got range deletion end but no start in stream " + BaseEncoding.base16().encode(streamId, 0, 16));
        }
        try {
            return bind(table, driver3FromLibraryTranslator, stmts.get(start.val.prefixSize).get(start.inclusive), c, start.val, cl);
        } finally {
            state.clear(streamId);
        }
    }

}
