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

import com.scylladb.cdc.lib.CDCConsumer;
import com.scylladb.cdc.lib.CDCConsumerBuilder;
import com.scylladb.cdc.model.TableName;
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
        private final ConsistencyLevel cl;
        private final TableMetadata table;
        private final Map<Byte, Operation> preparedOps = new HashMap<>();
        private final PreparedStatement preimageQuery;

        private final Mode mode;
        private final ConcurrentHashMap<ByteBuffer, Operation> nextPostimageOperation = new ConcurrentHashMap<>();

        private static interface Operation {
            Statement getStatement(RawChange c, ConsistencyLevel cl);
            Statement getStatement(RawChange c, ConsistencyLevel cl, Mode m);
        }

        private static long timeuuidToTimestamp(UUID from) {
            return (from.timestamp() - 0x01b21dd213814000L) / 10;
        }

        private static abstract class PreparedOperation implements Operation {
            protected final TableMetadata table;
            protected final PreparedStatement preparedStmt;

            protected abstract RegularStatement getStatement(TableMetadata t);

            protected PreparedOperation(Session session, TableMetadata t) {
                table = t;
                preparedStmt = session.prepare(getStatement(table));
            }

            public Statement getStatement(RawChange c, ConsistencyLevel cl) {
                return getStatement(c, cl, Mode.DELTA);
            }

            public Statement getStatement(RawChange c, ConsistencyLevel cl, Mode m) {
                BoundStatement stmt = preparedStmt.bind();
                stmt.setLong(TIMESTAMP_MARKER_NAME, timeuuidToTimestamp(c.TEMPORARY_PORTING_getTime()));
                bindInternal(stmt, c, m);
                stmt.setConsistencyLevel(cl);
                stmt.setIdempotent(true);
                return stmt;
            }

            protected void bindTTL(BoundStatement stmt, RawChange c) {
                Integer ttl = c.TEMPORARY_PORTING_getTTL();
                if (ttl != null) {
                    stmt.setInt(TTL_MARKER_NAME, ttl);
                } else {
                    stmt.unset(TTL_MARKER_NAME);
                }
            }

            protected void bindAllNonCDCColumns(BoundStatement stmt, RawChange c, Mode m) {
                for (Definition d : c.TEMPORARY_PORTING_row().getColumnDefinitions()) {
                    if (!d.getName().startsWith("cdc$")) {
                        if (c.TEMPORARY_PORTING_row().isNull(d.getName()) && !c.TEMPORARY_PORTING_isDeleted(d.getName())) {
                            stmt.unset(d.getName());
                        } else {
                            switch (m) {
                                case DELTA:
                                case POSTIMAGE:
                                    ColumnMetadata meta = table.getColumn(d.getName());
                                    if (meta.getType().getName() == DataType.Name.LIST && !meta.getType().isFrozen()) {
                                        DataType innerType = meta.getType().getTypeArguments().get(0);
                                        TypeToken<Object> type = CodecRegistry.DEFAULT_INSTANCE.codecFor(innerType).getJavaType();
                                        TreeMap<UUID, Object> sorted = new TreeMap<>();
                                        Map<UUID, Object> cMap = c.getMap(d.getName());
                                        for (Entry<UUID, Object> e : cMap.entrySet()) {
                                            sorted.put(e.getKey(), e.getValue());
                                        }
                                        List<Object> list = new ArrayList<>();
                                        for (Entry<UUID, Object> e : sorted.entrySet()) {
                                            list.add(e.getValue());
                                        }
                                        stmt.setList(d.getName(), list);
                                    } else {
                                        stmt.setBytesUnsafe(d.getName(), c.TEMPORARY_PORTING_row().getBytesUnsafe(d.getName()));
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
            }

            protected void bindPrimaryKeyColumns(BoundStatement stmt, RawChange c) {
                Set<String> primaryColumns = table.getPrimaryKey().stream().map(ColumnMetadata::getName)
                        .collect(Collectors.toSet());
                for (Definition d : c.TEMPORARY_PORTING_row().getColumnDefinitions()) {
                    if (!d.getName().startsWith("cdc$")) {
                        if (primaryColumns.contains(d.getName())) {
                            stmt.setBytesUnsafe(d.getName(), c.TEMPORARY_PORTING_row().getBytesUnsafe(d.getName()));
                        }
                    }
                }
            }

            protected void bindPartitionKeyColumns(BoundStatement stmt, RawChange c) {
                Set<String> partitionColumns = table.getPartitionKey().stream().map(ColumnMetadata::getName)
                        .collect(Collectors.toSet());
                for (Definition d : c.TEMPORARY_PORTING_row().getColumnDefinitions()) {
                    if (!d.getName().startsWith("cdc$")) {
                        if (partitionColumns.contains(d.getName())) {
                            stmt.setBytesUnsafe(d.getName(), c.TEMPORARY_PORTING_row().getBytesUnsafe(d.getName()));
                        }
                    }
                }
            }

            protected abstract void bindInternal(BoundStatement stmt, RawChange c, Mode m);
        }

        private static class UnpreparedUpdateOp implements Operation {
            private final TableMetadata table;

            public UnpreparedUpdateOp(TableMetadata t) {
                table = t;
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
                        builder.where(eq(c.getName(), change.TEMPORARY_PORTING_row().get(c.getName(), codec)));
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
                            if (!change.TEMPORARY_PORTING_row().isNull(deletedElementsColumnName)) {
                                if (c.getType().getName() == DataType.Name.SET) {
                                    op = removeAll(c.getName(), change.TEMPORARY_PORTING_row().getSet(deletedElementsColumnName, type));
                                } else if (c.getType().getName() == DataType.Name.MAP) {
                                    op = removeAll(c.getName(), change.TEMPORARY_PORTING_row().getSet(deletedElementsColumnName, type));
                                } else if (c.getType().getName() == DataType.Name.LIST) {
                                    for (UUID key : change.TEMPORARY_PORTING_row().getSet(deletedElementsColumnName, UUID.class)) {
                                        builder.with(new ListSetIdxTimeUUIDAssignment(c.getName(), key, null));
                                    }
                                    return;
                                } else {
                                    throw new IllegalStateException();
                                }
                            } else {
                                if (c.getType().getName() == DataType.Name.SET) {
                                    op = addAll(c.getName(), change.TEMPORARY_PORTING_row().getSet(c.getName(), type));
                                } else if (c.getType().getName() == DataType.Name.MAP) {
                                    op = putAll(c.getName(), change.getMap(c.getName()));
                                } else if (c.getType().getName() == DataType.Name.LIST) {
                                    Map<UUID, Object> cMap = change.getMap(c.getName());
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
                            op = set(c.getName(), change.TEMPORARY_PORTING_row().get(c.getName(), codec));
                        }
                        builder.with(op);
                    }
                });
                Integer ttl = change.TEMPORARY_PORTING_getTTL();
                if (ttl != null) {
                    builder.using(timestamp(timeuuidToTimestamp(change.TEMPORARY_PORTING_getTime()))).and(ttl(ttl));
                } else {
                    builder.using(timestamp(timeuuidToTimestamp(change.TEMPORARY_PORTING_getTime())));
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

            public UpdateOp(Session session, TableMetadata table) {
                super(session, table);
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

            public InsertOp(Session session, TableMetadata table) {
                super(session, table);
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

            public RowDeleteOp(Session session, TableMetadata table) {
                super(session, table);
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

            public PartitionDeleteOp(Session session, TableMetadata table) {
                super(session, table);
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

            public RangeDeleteEndOp(Session session, TableMetadata t, RangeTombstoneState state, boolean inclusive) {
                table = t;
                this.state = state;
                for (int i = t.getPartitionKey().size(); i < t.getPrimaryKey().size(); ++i) {
                    Map<Boolean, PreparedStatement> map = new HashMap<>();
                    map.put(true, prepare(session, t, i, true, inclusive));
                    map.put(false, prepare(session, t, i, false, inclusive));
                    stmts.put(i + 1, map);
                }
            }

            private static Statement bind(TableMetadata table, PreparedStatement stmt, RawChange change, PrimaryKeyValue startVal, ConsistencyLevel cl) {
                BoundStatement s = stmt.bind();
                Iterator<ColumnMetadata> keyIt = table.getPrimaryKey().iterator();
                ColumnMetadata prevCol = keyIt.next();
                ByteBuffer end = change.TEMPORARY_PORTING_row().getBytesUnsafe(prevCol.getName());
                byte[] start = startVal.values.get(prevCol.getName());
                while (keyIt.hasNext()) {
                    ColumnMetadata col = keyIt.next();
                    byte[] newStart = startVal.values.get(col.getName());
                    if (newStart == null) {
                        break;
                    }
                    s.setBytesUnsafe(prevCol.getName(), end);
                    end = change.TEMPORARY_PORTING_row().getBytesUnsafe(col.getName());
                    prevCol = col;
                    start = newStart;
                }
                s.setBytesUnsafe(prevCol.getName() + "_start", ByteBuffer.wrap(start));
                s.setBytesUnsafe(prevCol.getName() + "_end", end);
                s.setLong(TIMESTAMP_MARKER_NAME, timeuuidToTimestamp(change.TEMPORARY_PORTING_getTime()));
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
                    return bind(table, stmts.get(start.val.values.size()).get(start.inclusive), c, start.val, cl);
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
            public final Map<String, byte[]> values = new HashMap<>();

            public PrimaryKeyValue(TableMetadata table, Row row) {
                for (ColumnMetadata col : table.getPrimaryKey()) {
                    ByteBuffer buf = row.getBytesUnsafe(col.getName());
                    if (buf != null) {
                        byte[] bytes = new byte[buf.remaining()];
                        buf.get(bytes);
                        values.put(col.getName(), bytes);
                    }
                }
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
                    return o instanceof Key && Arrays.equals(val, ((Key)o).val);
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
                state.put(new Key(bytes), new DeletionStart(new PrimaryKeyValue(table, c.TEMPORARY_PORTING_row()), inclusive));
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
            preimageQuery = createPreimageQuery(session, table);
            preparedOps.put((byte) 1,
                    hasCollection(table) ? new UnpreparedUpdateOp(table) : new UpdateOp(s, table));
            preparedOps.put((byte) 2, new InsertOp(s, table));
            preparedOps.put((byte) 3, new RowDeleteOp(s, table));
            preparedOps.put((byte) 4, new PartitionDeleteOp(s, table));
            RangeTombstoneState rtState = new RangeTombstoneState(table);
            preparedOps.put((byte) 5, new RangeDeleteStartOp(rtState, true));
            preparedOps.put((byte) 6, new RangeDeleteStartOp(rtState, false));
            preparedOps.put((byte) 7, new RangeDeleteEndOp(s, table, rtState, true));
            preparedOps.put((byte) 8, new RangeDeleteEndOp(s, table, rtState, false));
        }

        private static boolean hasCollection(TableMetadata t) {
            return t.getColumns().stream().anyMatch(c -> c.getType().isCollection() && !c.getType().isFrozen());
        }

        private CompletableFuture<Void> consumeDelta(RawChange change) {
            Byte operationType = change.TEMPORARY_PORTING_getOperation();
            Operation op = preparedOps.get(operationType);
            if (op == null) {
                System.err.println("Unsupported operation: " + change.TEMPORARY_PORTING_getOperation());
                throw new UnsupportedOperationException("" + change.TEMPORARY_PORTING_getOperation());
            }
            Statement stmt = op.getStatement(change, cl);
            if (stmt == null) {
                return FutureUtils.completed(null);
            }
            return FutureUtils.convert(session.executeAsync(stmt), "Consume delta " + operationType);
        }

        private CompletableFuture<Void> consumePostimage(RawChange change) {
            Byte operationType = change.TEMPORARY_PORTING_getOperation();
            ByteBuffer streamid = change.getId().getStreamId().getValue();
            if (operationType == 0) {
                throw new IllegalStateException("Unexpected preimage");
            } else if (operationType < 3) {
                nextPostimageOperation.put(streamid, preparedOps.get(operationType));
                return FutureUtils.completed(null);
            } else if (operationType < 9) {
                nextPostimageOperation.remove(streamid);
                return consumeDelta(change);
            } else {
                Operation op = nextPostimageOperation.remove(streamid);
                if (op != null) {
                    Statement stmt = op.getStatement(change, cl, mode);
                    if (stmt != null) {
                        return FutureUtils.convert(session.executeAsync(stmt), "Consume postimage");
                    }
                }
                return FutureUtils.completed(null);
            }
        }

        private static String print(ByteBuffer b) {
            byte[] bytes = new byte[b.remaining()];
            b.duplicate().get(bytes, 0, bytes.length);
            return BaseEncoding.base16().encode(bytes, 0, bytes.length);
        }

        private static boolean equal(ByteBuffer a, ByteBuffer b) {
            if (a == null && b != null) {
                return false;
            }
            if (a != null && b == null) {
                return false;
            }
            if (a == null && b == null) {
                return true;
            }
            return a.equals(b);
        }

        private CompletableFuture<Void> checkPreimage(RawChange c, ResultSet rs) {
            if (rs.getAvailableWithoutFetching() == 0) {
                if (rs.isFullyFetched()) {
                    Set<String> primaryColumns = table.getPrimaryKey().stream().map(ColumnMetadata::getName)
                            .collect(Collectors.toSet());
                    for (Definition d : c.TEMPORARY_PORTING_row().getColumnDefinitions()) {
                        if (!d.getName().startsWith("cdc$") && !primaryColumns.contains(d.getName()) && !c.TEMPORARY_PORTING_row().isNull(d.getName())) {
                            System.out.println("Inconsistency detected.\nNo row in target.");
                            return FutureUtils.completed(null);
                        }
                    }
                    return FutureUtils.completed(null);
                }
                return FutureUtils.transformDeferred(rs.fetchMoreResults(), r -> checkPreimage(c, r));
            }
            Row expectedRow = rs.one();
            for (Definition d : expectedRow.getColumnDefinitions()) {
                try {
                    ByteBuffer expectedColumn = expectedRow.getBytesUnsafe(d.getName());
                    ByteBuffer column = c.TEMPORARY_PORTING_row().getBytesUnsafe(d.getName());
                    if (!equal(expectedColumn, column)) {
                        System.out.println("Inconsistency detected.\n Wrong values for column "
                                + d.getName() + " expected " + print(expectedColumn) + " but got " + print(column));
                        break;
                    }
                } catch (Exception e) {
                    System.out.println("Inconsistency detected.\nException.");
                    e.printStackTrace();
                    break;
                }
            }
            return FutureUtils.completed(null);
        }

        private CompletableFuture<Void> consumePreimage(RawChange change) {
            Byte operationType = change.TEMPORARY_PORTING_getOperation();
            if (operationType == 0) {
                BoundStatement stmt = preimageQuery.bind();
                Set<String> primaryColumns = table.getPrimaryKey().stream().map(ColumnMetadata::getName)
                        .collect(Collectors.toSet());
                for (Definition d : change.TEMPORARY_PORTING_row().getColumnDefinitions()) {
                    if (primaryColumns.contains(d.getName())) {
                        stmt.setBytesUnsafe(d.getName(), change.TEMPORARY_PORTING_row().getBytesUnsafe(d.getName()));
                    }
                }
                stmt.setConsistencyLevel(ConsistencyLevel.ALL);
                stmt.setIdempotent(true);
                return FutureUtils.transformDeferred(session.executeAsync(stmt), r -> checkPreimage(change, r));
            } else if (operationType < 9) {
                return consumeDelta(change);
            } else {
                throw new IllegalStateException("Unexpected postimage");
            }
        }

        @Override
        public CompletableFuture<Void> consume(RawChange change) {
            logger.atInfo().log("Replicator consuming change: %s", change.getId());

            switch (mode) {
                case DELTA: return consumeDelta(change);
                case POSTIMAGE: return consumePostimage(change);
                case PREIMAGE: return consumePreimage(change);
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

            CDCConsumer consumer = CDCConsumerBuilder.builder(sSession, new Consumer(mode, dCluster, dSession, keyspace, table, cl), tables).workersCount(1).build();
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