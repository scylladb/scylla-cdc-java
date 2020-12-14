package com.scylladb.cdc.replicator;

import static com.datastax.driver.core.querybuilder.QueryBuilder.addAll;
import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.putAll;
import static com.datastax.driver.core.querybuilder.QueryBuilder.removeAll;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.timestamp;
import static com.datastax.driver.core.querybuilder.QueryBuilder.ttl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.querybuilder.Assignment;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.ListSetIdxTimeUUIDAssignment;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import com.google.common.flogger.FluentLogger;
import com.google.common.io.BaseEncoding;
import com.google.common.reflect.TypeToken;

import com.scylladb.cdc.cql.driver3.Driver3FromLibraryTranslator;
import com.scylladb.cdc.cql.driver3.Driver3ToLibraryTranslator;
import com.scylladb.cdc.lib.CDCConsumer;
import com.scylladb.cdc.lib.CDCConsumerBuilder;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.worker.cql.Cell;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.RawChangeConsumer;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import sun.misc.Signal;

public class Main {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private static enum Mode {
        DELTA, PREIMAGE, POSTIMAGE;

        public static Mode fromString(String mode) {
            if ("delta".equals(mode)) {
                return DELTA;
            } else if ("preimage".equals(mode)) {
                return PREIMAGE;
            } else if ("postimage".equals(mode)) {
                return POSTIMAGE;
            } else {
                throw new IllegalStateException("Wrong mode " + mode);
            }
        }
    }

    private static final class Consumer implements RawChangeConsumer {
        private static final String TIMESTAMP_MARKER_NAME = "using_timestamp_bind_marker";
        private static final String TTL_MARKER_NAME = "using_ttl_bind_marker";

        private final Session session;
        private final Driver3FromLibraryTranslator driver3FromLibraryTranslator;
        private final ConsistencyLevel cl;
        private final TableMetadata table;
        private final Map<RawChange.OperationType, Operation> preparedOps = new HashMap<>();
        private final PreparedStatement preimageQuery;

        private final Mode mode;
        private final ConcurrentHashMap<ByteBuffer, Operation> nextPostimageOperation = new ConcurrentHashMap<>();

        private static interface Operation {
            Statement getStatement(RawChange c, ConsistencyLevel cl);

            Statement getStatement(RawChange c, ConsistencyLevel cl, Mode m);
        }

        private static void setBytesUnsafe(Driver3FromLibraryTranslator driver3FromLibraryTranslator, BoundStatement statement, String columnName, Cell cell) {
            TypeCodec<Object> driverCodec = driver3FromLibraryTranslator.getTypeCodec(cell.getColumnDefinition().getCdcLogDataType());
            Object driverObject = driver3FromLibraryTranslator.translate(cell);
            statement.set(columnName, driverObject, driverCodec);
        }

        private static void setBytesUnsafe(Driver3FromLibraryTranslator driver3FromLibraryTranslator, BoundStatement statement, String columnName, RawChange change) {
            Cell cell = change.getCell(columnName);
            setBytesUnsafe(driver3FromLibraryTranslator, statement, columnName, cell);
        }

        private static abstract class PreparedOperation implements Operation {
            protected final TableMetadata table;
            protected final PreparedStatement preparedStmt;
            protected final Driver3FromLibraryTranslator driver3FromLibraryTranslator;

            protected abstract RegularStatement getStatement(TableMetadata t);

            protected PreparedOperation(Session session, Driver3FromLibraryTranslator d3t, TableMetadata t) {
                table = t;
                preparedStmt = session.prepare(getStatement(table));
                driver3FromLibraryTranslator = d3t;
            }

            public Statement getStatement(RawChange c, ConsistencyLevel cl) {
                return getStatement(c, cl, Mode.DELTA);
            }

            public Statement getStatement(RawChange c, ConsistencyLevel cl, Mode m) {
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

            protected void bindAllNonCDCColumns(BoundStatement stmt, RawChange c, Mode m) {
                for (ChangeSchema.ColumnDefinition cd : c.getSchema().getNonCdcColumnDefinitions()) {
                    if (c.getAsObject(cd.getColumnName()) == null && !c.TEMPORARY_PORTING_isDeleted(cd.getColumnName())) {
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
                                    for (Entry<UUID, Object> e : cMap.entrySet()) {
                                        sorted.put(e.getKey(), e.getValue());
                                    }
                                    List<Object> list = new ArrayList<>();
                                    for (Entry<UUID, Object> e : sorted.entrySet()) {
                                        list.add(e.getValue());
                                    }
                                    stmt.setList(cd.getColumnName(), list);
                                } else {
                                    setBytesUnsafe(driver3FromLibraryTranslator, stmt, cd.getColumnName(), c);
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
                        setBytesUnsafe(driver3FromLibraryTranslator, stmt, cd.getColumnName(), c);
                    }
                }
            }

            protected void bindPartitionKeyColumns(BoundStatement stmt, RawChange c) {
                Set<String> partitionColumns = table.getPartitionKey().stream().map(ColumnMetadata::getName)
                        .collect(Collectors.toSet());
                for (ChangeSchema.ColumnDefinition cd : c.getSchema().getNonCdcColumnDefinitions()) {
                    if (partitionColumns.contains(cd.getColumnName())) {
                        setBytesUnsafe(driver3FromLibraryTranslator, stmt, cd.getColumnName(), c);
                    }
                }
            }

            protected abstract void bindInternal(BoundStatement stmt, RawChange c, Mode m);
        }

        private static class UnpreparedUpdateOp implements Operation {
            private final TableMetadata table;
            private final Driver3FromLibraryTranslator driver3FromLibraryTranslator;

            public UnpreparedUpdateOp(TableMetadata t, Driver3FromLibraryTranslator d3t) {
                table = t;
                driver3FromLibraryTranslator = d3t;
            }

            public Statement getStatement(RawChange c, ConsistencyLevel cl) {
                return getStatement(c, cl, Mode.DELTA);
            }

            @Override
            public Statement getStatement(RawChange change, ConsistencyLevel cl, Mode m) {
                Update builder = QueryBuilder.update(table);
                Set<ColumnMetadata> primaryColumns = new HashSet<>(table.getPrimaryKey());
                table.getColumns().stream().forEach(c -> {
                    TypeCodec<Object> codec = CodecRegistry.DEFAULT_INSTANCE.codecFor(c.getType());
                    if (primaryColumns.contains(c)) {
                        builder.where(eq(c.getName(), driver3FromLibraryTranslator.translate(change.getCell(c.getName()))));
                    } else {
                        Assignment op = null;
                        if (c.getType().isCollection() && !c.getType().isFrozen() && !change.TEMPORARY_PORTING_isDeleted(c.getName())) {
                            DataType innerType = c.getType().getTypeArguments().get(0);
                            TypeToken<Object> type = CodecRegistry.DEFAULT_INSTANCE.codecFor(innerType).getJavaType();
                            TypeToken<Object> type2 = null;
                            if (c.getType().getTypeArguments().size() > 1) {
                                type2 = CodecRegistry.DEFAULT_INSTANCE.codecFor(c.getType().getTypeArguments().get(1)).getJavaType();
                            }
                            String deletedElementsColumnName = "cdc$deleted_elements_" + c.getName();
                            if (change.getAsObject(deletedElementsColumnName) != null) {
                                if (c.getType().getName() == DataType.Name.SET) {
                                    op = removeAll(c.getName(), (Set) driver3FromLibraryTranslator.translate(change.getCell(c.getName())));
                                } else if (c.getType().getName() == DataType.Name.MAP) {
                                    op = removeAll(c.getName(), (Set) driver3FromLibraryTranslator.translate(change.getCell(c.getName())));
                                } else if (c.getType().getName() == DataType.Name.LIST) {
                                    Set<UUID> cSet = (Set<UUID>) driver3FromLibraryTranslator.translate(change.getCell(c.getName()));
                                    for (UUID key : cSet) {
                                        builder.with(new ListSetIdxTimeUUIDAssignment(c.getName(), key, null));
                                    }
                                    return;
                                } else {
                                    throw new IllegalStateException();
                                }
                            } else {
                                if (c.getType().getName() == DataType.Name.SET) {
                                    op = addAll(c.getName(), (Set) driver3FromLibraryTranslator.translate(change.getCell(c.getName())));
                                } else if (c.getType().getName() == DataType.Name.MAP) {
                                    op = putAll(c.getName(), (Map) driver3FromLibraryTranslator.translate(change.getCell(c.getName())));
                                } else if (c.getType().getName() == DataType.Name.LIST) {
                                    Map<UUID, Object> cMap = (Map<UUID, Object>) driver3FromLibraryTranslator.translate(change.getCell(c.getName()));
                                    for (Entry<UUID, Object> e : cMap.entrySet()) {
                                        builder.with(new ListSetIdxTimeUUIDAssignment(c.getName(), e.getKey(), e.getValue()));
                                    }
                                    return;
                                } else {
                                    throw new IllegalStateException();
                                }
                            }
                        }
                        if (op == null) {
                            op = set(c.getName(), driver3FromLibraryTranslator.translate(change.getCell(c.getName())));
                        }
                        builder.with(op);
                    }
                });
                Long ttl = change.getTTL();
                if (ttl != null) {
                    builder.using(timestamp(change.getId().getChangeTime().getTimestamp())).and(ttl((int) ((long) ttl)));
                } else {
                    builder.using(timestamp(change.getId().getChangeTime().getTimestamp()));
                }
                return builder;
            }

        }

        private static class UpdateOp extends PreparedOperation {

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

            public UpdateOp(Session session, Driver3FromLibraryTranslator d3t, TableMetadata table) {
                super(session, d3t, table);
            }

            @Override
            protected void bindInternal(BoundStatement stmt, RawChange c, Mode m) {
                bindTTL(stmt, c);
                bindAllNonCDCColumns(stmt, c, m);
            }

        }

        private static class InsertOp extends PreparedOperation {

            protected RegularStatement getStatement(TableMetadata t) {
                Insert builder = QueryBuilder.insertInto(t);
                t.getColumns().stream().forEach(c -> builder.value(c.getName(), bindMarker(c.getName())));
                builder.using(timestamp(bindMarker(TIMESTAMP_MARKER_NAME))).and(ttl(bindMarker(TTL_MARKER_NAME)));
                return builder;
            }

            public InsertOp(Session session, Driver3FromLibraryTranslator d3t, TableMetadata table) {
                super(session, d3t, table);
            }

            @Override
            protected void bindInternal(BoundStatement stmt, RawChange c, Mode m) {
                bindTTL(stmt, c);
                bindAllNonCDCColumns(stmt, c, m);
            }

        }

        private static class RowDeleteOp extends PreparedOperation {

            protected RegularStatement getStatement(TableMetadata t) {
                Delete builder = QueryBuilder.delete().from(t);
                t.getPrimaryKey().stream().forEach(c -> builder.where(eq(c.getName(), bindMarker(c.getName()))));
                builder.using(timestamp(bindMarker(TIMESTAMP_MARKER_NAME)));
                return builder;
            }

            public RowDeleteOp(Session session, Driver3FromLibraryTranslator d3t, TableMetadata table) {
                super(session, d3t, table);
            }

            @Override
            protected void bindInternal(BoundStatement stmt, RawChange c, Mode m) {
                bindPrimaryKeyColumns(stmt, c);
            }

        }

        private static class PartitionDeleteOp extends PreparedOperation {

            protected RegularStatement getStatement(TableMetadata t) {
                Delete builder = QueryBuilder.delete().from(t);
                t.getPartitionKey().stream().forEach(c -> builder.where(eq(c.getName(), bindMarker(c.getName()))));
                builder.using(timestamp(bindMarker(TIMESTAMP_MARKER_NAME)));
                return builder;
            }

            public PartitionDeleteOp(Session session, Driver3FromLibraryTranslator d3t, TableMetadata table) {
                super(session, d3t, table);
            }

            @Override
            protected void bindInternal(BoundStatement stmt, RawChange c, Mode m) {
                bindPartitionKeyColumns(stmt, c);
            }

        }

        private static class RangeDeleteStartOp implements Operation {
            private final RangeTombstoneState state;
            private final boolean inclusive;

            public RangeDeleteStartOp(RangeTombstoneState rtState, boolean inclusive) {
                state = rtState;
                this.inclusive = inclusive;
            }

            public Statement getStatement(RawChange c, ConsistencyLevel cl) {
                return getStatement(c, cl, Mode.DELTA);
            }

            @Override
            public Statement getStatement(RawChange c, ConsistencyLevel cl, Mode m) {
                state.addStart(c, inclusive);
                return null;
            }

        }

        private static class RangeDeleteEndOp implements Operation {
            private final TableMetadata table;
            private final RangeTombstoneState state;
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
                builder.using(timestamp(bindMarker(TIMESTAMP_MARKER_NAME)));
                return s.prepare(builder);
            }

            public RangeDeleteEndOp(Session session, TableMetadata t, Driver3FromLibraryTranslator driver3FromLibraryTranslator, RangeTombstoneState state, boolean inclusive) {
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

            private static Statement bind(TableMetadata table, Driver3FromLibraryTranslator driver3FromLibraryTranslator, PreparedStatement stmt, RawChange change, PrimaryKeyValue startVal, ConsistencyLevel cl) {
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
                    setBytesUnsafe(driver3FromLibraryTranslator, s, prevCol.getName(), end);
                    end = change.getCell(col.getName());
                    prevCol = col;
                    start = newStart;
                }
                setBytesUnsafe(driver3FromLibraryTranslator, s, prevCol.getName() + "_start", start);
                setBytesUnsafe(driver3FromLibraryTranslator, s, prevCol.getName() + "_end", end);
                s.setLong(TIMESTAMP_MARKER_NAME, change.getId().getChangeTime().getTimestamp());
                s.setConsistencyLevel(cl);
                s.setIdempotent(true);
                return s;
            }

            public Statement getStatement(RawChange c, ConsistencyLevel cl) {
                return getStatement(c, cl, Mode.DELTA);
            }

            @Override
            public Statement getStatement(RawChange c, ConsistencyLevel cl, Mode m) {
                byte[] streamId = new byte[16];
                c.getId().getStreamId().getValue().duplicate().get(streamId, 0, 16);
                DeletionStart start = state.getStart(streamId);
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

        private static class DeletionStart {
            public final PrimaryKeyValue val;
            public final boolean inclusive;

            public DeletionStart(PrimaryKeyValue v, boolean i) {
                val = v;
                inclusive = i;
            }
        }

        private static class PrimaryKeyValue {
            public final RawChange change;
            public final int prefixSize;

            public PrimaryKeyValue(TableMetadata table, RawChange change) {
                this.change = change;
                int prefixSize = 0;
                for (ColumnMetadata col : table.getPrimaryKey()) {
                    Object colValue = change.getAsObject(col.getName());
                    if (colValue != null) {
                        prefixSize++;
                    } else {
                        break;
                    }
                }
                this.prefixSize = prefixSize;
            }

        }

        private static class RangeTombstoneState {

            private static class Key {
                public final byte[] val;

                public Key(byte[] v) {
                    val = v;
                }

                @Override
                public boolean equals(Object o) {
                    return o instanceof Key && Arrays.equals(val, ((Key) o).val);
                }

                @Override
                public int hashCode() {
                    return Arrays.hashCode(val);
                }
            }

            private final TableMetadata table;
            private final ConcurrentHashMap<Key, DeletionStart> state = new ConcurrentHashMap<>();

            public RangeTombstoneState(TableMetadata table) {
                this.table = table;
            }

            public void addStart(RawChange c, boolean inclusive) {
                byte[] bytes = new byte[16];
                c.getId().getStreamId().getValue().duplicate().get(bytes, 0, 16);
                state.put(new Key(bytes), new DeletionStart(new PrimaryKeyValue(table, c), inclusive));
            }

            public DeletionStart getStart(byte[] streamId) {
                return state.get(new Key(streamId));
            }

            public void clear(byte[] streamId) {
                state.remove(new Key(streamId));
            }
        }

        private static PreparedStatement createPreimageQuery(Session s, TableMetadata t) {
            Select builder = QueryBuilder.select().all().from(t);
            t.getPrimaryKey().stream().forEach(c -> builder.where(eq(c.getName(), bindMarker(c.getName()))));
            System.out.println("Preimage query: " + builder);
            return s.prepare(builder);
        }

        public Consumer(Mode m, Cluster c, Session s, String keyspace, String tableName, ConsistencyLevel cl) {
            mode = m;
            session = s;
            this.cl = cl;
            table = c.getMetadata().getKeyspaces().stream().filter(k -> k.getName().equals(keyspace))
                    .findAny().get().getTable(tableName);
            driver3FromLibraryTranslator = new Driver3FromLibraryTranslator(c.getMetadata());
            preimageQuery = createPreimageQuery(session, table);
            preparedOps.put(RawChange.OperationType.ROW_UPDATE,
                    hasCollection(table) ? new UnpreparedUpdateOp(table, driver3FromLibraryTranslator) : new UpdateOp(s, driver3FromLibraryTranslator, table));
            preparedOps.put(RawChange.OperationType.ROW_INSERT, new InsertOp(s, driver3FromLibraryTranslator, table));
            preparedOps.put(RawChange.OperationType.ROW_DELETE, new RowDeleteOp(s, driver3FromLibraryTranslator, table));
            preparedOps.put(RawChange.OperationType.PARTITION_DELETE, new PartitionDeleteOp(s, driver3FromLibraryTranslator, table));
            RangeTombstoneState rtState = new RangeTombstoneState(table);
            preparedOps.put(RawChange.OperationType.ROW_RANGE_DELETE_INCLUSIVE_LEFT_BOUND, new RangeDeleteStartOp(rtState, true));
            preparedOps.put(RawChange.OperationType.ROW_RANGE_DELETE_EXCLUSIVE_LEFT_BOUND, new RangeDeleteStartOp(rtState, false));
            preparedOps.put(RawChange.OperationType.ROW_RANGE_DELETE_INCLUSIVE_RIGHT_BOUND, new RangeDeleteEndOp(s, table, driver3FromLibraryTranslator, rtState, true));
            preparedOps.put(RawChange.OperationType.ROW_RANGE_DELETE_EXCLUSIVE_RIGHT_BOUND, new RangeDeleteEndOp(s, table, driver3FromLibraryTranslator, rtState, false));
        }

        private static boolean hasCollection(TableMetadata t) {
            return t.getColumns().stream().anyMatch(c -> c.getType().isCollection() && !c.getType().isFrozen());
        }

        private CompletableFuture<Void> consumeDelta(RawChange change) {
            RawChange.OperationType operationType = change.getOperationType();
            Operation op = preparedOps.get(operationType);
            if (op == null) {
                System.err.println("Unsupported operation: " + operationType);
                throw new UnsupportedOperationException(operationType.toString());
            }
            Statement stmt = op.getStatement(change, cl);
            if (stmt == null) {
                return CompletableFuture.completedFuture(null);
            }
            return FutureUtils.convert(session.executeAsync(stmt), "Consume delta " + operationType);
        }

        private CompletableFuture<Void> consumePostimage(RawChange change) {
            RawChange.OperationType operationType = change.getOperationType();
            ByteBuffer streamid = change.getId().getStreamId().getValue();
            switch (operationType) {
                case PRE_IMAGE:
                    throw new IllegalStateException("Unexpected preimage");

                case ROW_UPDATE:
                case ROW_INSERT:
                    nextPostimageOperation.put(streamid, preparedOps.get(operationType));
                    return CompletableFuture.completedFuture(null);

                case ROW_DELETE:
                case PARTITION_DELETE:
                case ROW_RANGE_DELETE_INCLUSIVE_LEFT_BOUND:
                case ROW_RANGE_DELETE_EXCLUSIVE_LEFT_BOUND:
                case ROW_RANGE_DELETE_INCLUSIVE_RIGHT_BOUND:
                case ROW_RANGE_DELETE_EXCLUSIVE_RIGHT_BOUND:
                    nextPostimageOperation.remove(streamid);
                    return consumeDelta(change);

                case POST_IMAGE:
                    Operation op = nextPostimageOperation.remove(streamid);
                    if (op != null) {
                        Statement stmt = op.getStatement(change, cl, mode);
                        if (stmt != null) {
                            return FutureUtils.convert(session.executeAsync(stmt), "Consume postimage");
                        }
                    }
                    return CompletableFuture.completedFuture(null);

                default:
                    System.err.println("Unsupported operation: " + operationType);
                    throw new UnsupportedOperationException(operationType.toString());
            }
        }

        private CompletableFuture<Void> checkPreimage(RawChange c, ResultSet rs) {
            if (rs.getAvailableWithoutFetching() == 0) {
                if (rs.isFullyFetched()) {
                    Set<String> primaryColumns = table.getPrimaryKey().stream().map(ColumnMetadata::getName)
                            .collect(Collectors.toSet());
                    for (ChangeSchema.ColumnDefinition cd : c.getSchema().getNonCdcColumnDefinitions()) {
                        if (!primaryColumns.contains(cd.getColumnName()) && c.getAsObject(cd.getColumnName()) != null) {
                            System.out.println("Inconsistency detected.\nNo row in target.");
                            return CompletableFuture.completedFuture(null);
                        }
                    }
                    return CompletableFuture.completedFuture(null);
                }
                return FutureUtils.transformDeferred(rs.fetchMoreResults(), r -> checkPreimage(c, r));
            }
            Row expectedRow = rs.one();
            for (Definition d : expectedRow.getColumnDefinitions()) {
                try {
                    Cell libraryCell = c.getCell(d.getName());
                    Object libraryObject = libraryCell.getAsObject();
                    Object expectedLibraryObject = Driver3ToLibraryTranslator.translate(expectedRow.getObject(d.getName()), libraryCell.getColumnDefinition().getCdcLogDataType());

                    boolean consistent = Objects.equals(libraryObject, expectedLibraryObject);
                    if (!consistent) {
                        System.out.println("Inconsistency detected.\n Wrong values for column "
                                + d.getName() + " expected " + expectedLibraryObject + " but got " + libraryObject);
                        break;
                    }
                } catch (Exception e) {
                    System.out.println("Inconsistency detected.\nException.");
                    e.printStackTrace();
                    break;
                }
            }
            return CompletableFuture.completedFuture(null);
        }

        private CompletableFuture<Void> consumePreimage(RawChange change) {
            RawChange.OperationType operationType = change.getOperationType();
            switch (operationType) {
                case PRE_IMAGE:
                    BoundStatement stmt = preimageQuery.bind();
                    Set<String> primaryColumns = table.getPrimaryKey().stream().map(ColumnMetadata::getName)
                            .collect(Collectors.toSet());
                    for (ChangeSchema.ColumnDefinition cd : change.getSchema().getAllColumnDefinitions()) {
                        if (primaryColumns.contains(cd.getColumnName())) {
                            setBytesUnsafe(driver3FromLibraryTranslator, stmt, cd.getColumnName(), change);
                        }
                    }
                    stmt.setConsistencyLevel(ConsistencyLevel.ALL);
                    stmt.setIdempotent(true);
                    return FutureUtils.transformDeferred(session.executeAsync(stmt), r -> checkPreimage(change, r));
                case POST_IMAGE:
                    throw new IllegalStateException("Unexpected postimage");
                default:
                    return consumeDelta(change);
            }
        }

        @Override
        public CompletableFuture<Void> consume(RawChange change) {
            logger.atInfo().log("Replicator consuming change: %s", change.getId());

            switch (mode) {
                case DELTA:
                    return consumeDelta(change);
                case POSTIMAGE:
                    return consumePostimage(change);
                case PREIMAGE:
                    return consumePreimage(change);
            }
            throw new IllegalStateException("Unknown mode " + mode);
        }
    }

    private static void replicateChanges(Mode mode, String source, String destination, String keyspace, String table,
                                         ConsistencyLevel cl) {
        try (Cluster sCluster = Cluster.builder().addContactPoint(source).build();
             Session sSession = sCluster.connect();
             Cluster dCluster = Cluster.builder().addContactPoint(destination).build();
             Session dSession = dCluster.connect()) {

            HashSet<TableName> tables = new HashSet<>();
            tables.add(new TableName(keyspace, table));

            CDCConsumer consumer = CDCConsumerBuilder.builder(sSession, (threadId) -> new Consumer(mode, dCluster, dSession, keyspace, table, cl), tables).workersCount(1).build();
            consumer.start();

            try {
                CountDownLatch terminationLatch = new CountDownLatch(1);
                Signal.handle(new Signal("INT"), signal -> terminationLatch.countDown());
                terminationLatch.await();
                consumer.stop();
            } catch (InterruptedException e) {
                // Ignore exception.
            }
        }
    }

    private static Namespace parseArguments(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("CDCReplicator").build().defaultHelp(true);
        parser.addArgument("-m", "--mode").setDefault("delta").help("Mode of operation. Can be delta, preimage or postimage. Default is delta");
        parser.addArgument("-k", "--keyspace").help("Keyspace name");
        parser.addArgument("-t", "--table").help("Table name");
        parser.addArgument("-s", "--source").help("Address of a node in source cluster");
        parser.addArgument("-d", "--destination").help("Address of a node in destination cluster");
        parser.addArgument("-cl", "--consistency-level").setDefault("quorum")
                .help("Consistency level of writes. QUORUM by default");

        try {
            return parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(-1);
            return null;
        }
    }

    public static void main(String[] args) throws Exception {
        Namespace ns = parseArguments(args);
        replicateChanges(Mode.fromString(ns.getString("mode")), ns.getString("source"),
                ns.getString("destination"), ns.getString("keyspace"), ns.getString("table"),
                ConsistencyLevel.valueOf(ns.getString("consistency_level").toUpperCase()));
    }

}