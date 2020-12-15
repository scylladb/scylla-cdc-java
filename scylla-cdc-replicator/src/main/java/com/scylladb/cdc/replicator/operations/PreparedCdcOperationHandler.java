package com.scylladb.cdc.replicator.operations;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.google.common.reflect.TypeToken;
import com.scylladb.cdc.cql.driver3.Driver3FromLibraryTranslator;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.replicator.Main;
import com.scylladb.cdc.replicator.ReplicatorConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;

public abstract class PreparedCdcOperationHandler implements CdcOperationHandler {
    protected final TableMetadata table;
    protected final PreparedStatement preparedStmt;
    protected final Driver3FromLibraryTranslator driver3FromLibraryTranslator;

    protected static final String TIMESTAMP_MARKER_NAME = "using_timestamp_bind_marker";
    protected static final String TTL_MARKER_NAME = "using_ttl_bind_marker";

    protected abstract RegularStatement getStatement(TableMetadata t);

    protected PreparedCdcOperationHandler(Session session, Driver3FromLibraryTranslator d3t, TableMetadata t) {
        table = t;
        preparedStmt = session.prepare(getStatement(table));
        driver3FromLibraryTranslator = d3t;
    }

    public Statement getStatement(RawChange c, ConsistencyLevel cl, Main.Mode m) {
        BoundStatement stmt = preparedStmt.bind();
        stmt.setLong(TIMESTAMP_MARKER_NAME, c.getId().getChangeTime().getTimestamp());
        bindInternal(stmt, c, m);
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

    protected void bindAllNonCDCColumns(BoundStatement stmt, RawChange c, Main.Mode m) {
        for (ChangeSchema.ColumnDefinition cd : c.getSchema().getNonCdcColumnDefinitions()) {
            if (c.getAsObject(cd.getColumnName()) == null && !c.getIsDeleted(cd.getColumnName())) {
                stmt.unset(cd.getColumnName());
            } else {
                switch (m) {
                    case DELTA:
                    case POSTIMAGE:
                        ColumnMetadata meta = table.getColumn(cd.getColumnName());
                        if (meta.getType().getName() == DataType.Name.LIST && !meta.getType().isFrozen()) {
                            DataType innerType = meta.getType().getTypeArguments().get(0);
                            TypeToken<Object> type = CodecRegistry.DEFAULT_INSTANCE.codecFor(innerType).getJavaType();
                            TreeMap<UUID, Object> sorted = new TreeMap<>();
                            Map<UUID, Object> cMap = (Map<UUID, Object>) driver3FromLibraryTranslator.translate(c.getCell(cd.getColumnName()));
                            for (Map.Entry<UUID, Object> e : cMap.entrySet()) {
                                sorted.put(e.getKey(), e.getValue());
                            }
                            List<Object> list = new ArrayList<>();
                            for (Map.Entry<UUID, Object> e : sorted.entrySet()) {
                                list.add(e.getValue());
                            }
                            stmt.setList(cd.getColumnName(), list);
                        } else {
                            ReplicatorConsumer.setBytesUnsafe(driver3FromLibraryTranslator, stmt, cd.getColumnName(), c);
                        }
                        break;
                    case PREIMAGE:
                        throw new UnsupportedOperationException("Mode not supported " + m);
                    default:
                        throw new IllegalStateException("Unknown mode " + m);
                }
            }
        }
    }

    protected void bindPrimaryKeyColumns(BoundStatement stmt, RawChange c) {
        Set<String> primaryColumns = table.getPrimaryKey().stream().map(ColumnMetadata::getName)
                .collect(Collectors.toSet());
        for (ChangeSchema.ColumnDefinition cd : c.getSchema().getNonCdcColumnDefinitions()) {
            if (primaryColumns.contains(cd.getColumnName())) {
                ReplicatorConsumer.setBytesUnsafe(driver3FromLibraryTranslator, stmt, cd.getColumnName(), c);
            }
        }
    }

    protected void bindPartitionKeyColumns(BoundStatement stmt, RawChange c) {
        Set<String> partitionColumns = table.getPartitionKey().stream().map(ColumnMetadata::getName)
                .collect(Collectors.toSet());
        for (ChangeSchema.ColumnDefinition cd : c.getSchema().getNonCdcColumnDefinitions()) {
            if (partitionColumns.contains(cd.getColumnName())) {
                ReplicatorConsumer.setBytesUnsafe(driver3FromLibraryTranslator, stmt, cd.getColumnName(), c);
            }
        }
    }

    protected abstract void bindInternal(BoundStatement stmt, RawChange c, Main.Mode m);
}
