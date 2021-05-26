package com.scylladb.cdc.replicator.operations;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TypeCodec;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.cql.Cell;
import com.scylladb.cdc.replicator.ReplicatorConsumer;
import com.scylladb.cdc.replicator.driver3.Driver3FromLibraryTranslator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;

public abstract class ExecutingPreparedStatementHandler extends ExecutingStatementHandler {
    protected final TableMetadata table;
    protected final PreparedStatement preparedStmt;
    protected final Driver3FromLibraryTranslator driver3FromLibraryTranslator;

    protected static final String TIMESTAMP_MARKER_NAME = "using_timestamp_bind_marker";
    protected static final String TTL_MARKER_NAME = "using_ttl_bind_marker";

    protected abstract RegularStatement getStatement(TableMetadata t);

    protected ExecutingPreparedStatementHandler(Session session, Driver3FromLibraryTranslator d3t, TableMetadata t) {
        super(session);

        table = t;
        preparedStmt = session.prepare(getStatement(table));
        driver3FromLibraryTranslator = d3t;
    }

    @Override
    public Statement getStatement(RawChange c, ConsistencyLevel cl) {
        BoundStatement stmt = preparedStmt.bind();
        stmt.setLong(TIMESTAMP_MARKER_NAME, c.getId().getChangeTime().getTimestamp());
        bindInternal(stmt, c);
        stmt.setConsistencyLevel(cl);
        stmt.setIdempotent(true);
        return stmt;
    }

    protected void bindTTL(BoundStatement stmt, RawChange c) {
        Long ttl = c.getTTL();
        if (ttl != null) {
            stmt.setInt(TTL_MARKER_NAME, (int) ((long) ttl));
        } else {
            stmt.unset(TTL_MARKER_NAME);
        }
    }

    protected void bindAllNonCDCColumns(BoundStatement stmt, RawChange c) {
        for (ChangeSchema.ColumnDefinition cd : c.getSchema().getNonCdcColumnDefinitions()) {
            if (c.getAsObject(cd.getColumnName()) == null && !c.getIsDeleted(cd.getColumnName()) && c.getOperationType() != RawChange.OperationType.PRE_IMAGE && c.getOperationType() != RawChange.OperationType.POST_IMAGE) {
                stmt.unset(cd.getColumnName());
            } else {
                ColumnMetadata meta = table.getColumn(cd.getColumnName());
                if (meta.getType().getName() == DataType.Name.LIST && !meta.getType().isFrozen()) {
                    TreeMap<UUID, Object> sorted = new TreeMap<>();
                    Map<UUID, Object> cMap = (Map<UUID, Object>) driver3FromLibraryTranslator.translate(c.getCell(cd.getColumnName()));
                    if (cMap == null) {
                        stmt.setToNull(cd.getColumnName());
                    } else {
                        for (Map.Entry<UUID, Object> e : cMap.entrySet()) {
                            sorted.put(e.getKey(), e.getValue());
                        }
                        List<Object> list = new ArrayList<>();
                        for (Map.Entry<UUID, Object> e : sorted.entrySet()) {
                            list.add(e.getValue());
                        }
                        stmt.setList(cd.getColumnName(), list);
                    }
                } else {
                    bindColumn(stmt, c, cd.getColumnName());
                }
            }
        }
    }

    protected void bindPrimaryKeyColumns(BoundStatement stmt, RawChange c) {
        for (ChangeSchema.ColumnDefinition cd : c.getSchema().getNonCdcColumnDefinitions()) {
            ChangeSchema.ColumnKind columnKind = cd.getBaseTableColumnKind();

            boolean isPrimaryKey = columnKind == ChangeSchema.ColumnKind.PARTITION_KEY
                    || columnKind == ChangeSchema.ColumnKind.CLUSTERING_KEY;
            if (!isPrimaryKey) {
                continue;
            }

            bindColumn(stmt, c, cd.getColumnName());
        }
    }

    protected void bindPartitionKeyColumns(BoundStatement stmt, RawChange c) {
        for (ChangeSchema.ColumnDefinition cd : c.getSchema().getNonCdcColumnDefinitions()) {
            ChangeSchema.ColumnKind columnKind = cd.getBaseTableColumnKind();
            if (columnKind != ChangeSchema.ColumnKind.PARTITION_KEY) {
                continue;
            }

            bindColumn(stmt, c, cd.getColumnName());
        }
    }

    protected void bindColumn(BoundStatement statement, RawChange change, String columnName) {
        Cell cell = change.getCell(columnName);
        TypeCodec<Object> driverCodec = driver3FromLibraryTranslator.getTypeCodec(cell.getColumnDefinition().getCdcLogDataType());
        Object driverObject = driver3FromLibraryTranslator.translate(cell);
        statement.set(columnName, driverObject, driverCodec);
    }

    protected abstract void bindInternal(BoundStatement stmt, RawChange c);
}
