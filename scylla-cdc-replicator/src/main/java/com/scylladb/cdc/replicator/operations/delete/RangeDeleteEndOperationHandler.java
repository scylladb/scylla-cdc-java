package com.scylladb.cdc.replicator.operations.delete;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.scylladb.cdc.cql.driver3.Driver3FromLibraryTranslator;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.cql.Cell;
import com.scylladb.cdc.replicator.operations.ExecutingStatementHandler;

import java.util.Iterator;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class RangeDeleteEndOperationHandler extends ExecutingStatementHandler {
    private final TableMetadata tableMetadata;
    private final RangeDeleteState state;
    private final Driver3FromLibraryTranslator driver3FromLibraryTranslator;
    private final boolean isEndInclusive;

    @Override
    public Statement getStatement(RawChange change, ConsistencyLevel consistencyLevel) {
        // Build a DELETE statement:
        //
        // DELETE FROM table
        // USING TIMESTAMP ?
        // WHERE pk1 = ? AND pk2 = ? ... AND ck1 = ? AND ck2 = ? ... [AND ck_n [> | >=] ?] [AND ck_n [< | <=] ?]

        StreamId streamId = change.getId().getStreamId();

        // Find a matching row range delete start, which
        // was added by RangeDeleteStartOperationHandler.
        RangeDeleteState.DeletionStart start = state.consumeStart(streamId);
        if (start == null) {
            throw new IllegalStateException("Got range deletion end but no start in stream " + streamId);
        }

        Delete builder = QueryBuilder.delete().from(tableMetadata);

        // Iterate over primary key columns. The while loop
        // will add those restrictions:
        //
        // WHERE pk_i = ?
        // WHERE ck_i = ?
        //
        // When we exit the loop, we will finally add the
        // range restriction: WHERE ck_n [> | >=] ? AND ck_n [< | <=] ?
        //
        Iterator<ColumnMetadata> columnIterator = tableMetadata.getPrimaryKey().iterator();
        ColumnMetadata currentColumn = columnIterator.next();
        Cell startCell = start.change.getCell(currentColumn.getName());
        Cell endCell = change.getCell(currentColumn.getName());

        while (columnIterator.hasNext()) {
            ColumnMetadata nextColumn = columnIterator.next();
            Cell newStartCell = start.change.getCell(nextColumn.getName());
            Cell newEndCell = change.getCell(nextColumn.getName());

            if (newStartCell.getAsObject() == null && newEndCell.getAsObject() == null) {
                // For example:
                // DELETE FROM table WHERE pk1 = 5 AND pk2 = 3 AND ck1 = 6 AND ck2 > 7 AND ck2 < 15
                //
                // results in the following start and end range bounds:
                //
                //         pk1 |  pk2 |  ck1 |  ck2 |  ck3
                //        -----|------|------|------|-----
                // start:    1 |    3 |    6 |    7 | NULL
                // end:      1 |    3 |    6 |   15 | NULL
                //
                // So if the next column is NULL, we know that the current column
                // has a range. Therefore break, as this loop only handles
                // equality constraints.
                break;
            }

            // WHERE pk_i = ?
            // WHERE ck_i = ?
            builder.where(eq(currentColumn.getName(), driver3FromLibraryTranslator.translate(startCell)));

            startCell = newStartCell;
            endCell = newEndCell;
            currentColumn = nextColumn;
        }

        // We exited the loop, so the current column
        // has a range restriction: WHERE ck_n [> | >=] ? AND ck_n [< | <=] ?
        //
        // Half-open ranges are represented by NULL, so don't add
        // a restriction in such a case.
        if (startCell.getAsObject() != null) {
            builder.where(start.isInclusive ? gte(currentColumn.getName(), driver3FromLibraryTranslator.translate(startCell))
                    : gt(currentColumn.getName(), driver3FromLibraryTranslator.translate(startCell)));
        }
        if (endCell.getAsObject() != null) {
            builder.where(isEndInclusive ? lte(currentColumn.getName(), driver3FromLibraryTranslator.translate(endCell))
                    : lt(currentColumn.getName(), driver3FromLibraryTranslator.translate(endCell)));
        }

        // USING TIMESTAMP ?
        builder.using(timestamp(change.getId().getChangeTime().getTimestamp()));

        builder.setConsistencyLevel(consistencyLevel);
        builder.setIdempotent(true);
        return builder;
    }

    public RangeDeleteEndOperationHandler(Session session, TableMetadata tableMetadata, Driver3FromLibraryTranslator driver3FromLibraryTranslator,
                                          RangeDeleteState state, boolean isEndInclusive) {
        super(session);
        this.tableMetadata = tableMetadata;
        this.state = state;
        this.driver3FromLibraryTranslator = driver3FromLibraryTranslator;
        this.isEndInclusive = isEndInclusive;
    }
}
