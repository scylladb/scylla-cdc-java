package com.scylladb.cdc.replicator;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.flogger.FluentLogger;
import com.scylladb.cdc.cql.driver3.Driver3FromLibraryTranslator;
import com.scylladb.cdc.cql.driver3.Driver3ToLibraryTranslator;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.RawChangeConsumer;
import com.scylladb.cdc.model.worker.cql.Cell;
import com.scylladb.cdc.replicator.operations.CdcOperationHandler;
import com.scylladb.cdc.replicator.operations.InsertOperationHandler;
import com.scylladb.cdc.replicator.operations.PartitionDeleteOperationHandler;
import com.scylladb.cdc.replicator.operations.PreparedUpdateOperationHandler;
import com.scylladb.cdc.replicator.operations.RangeDeleteEndOperationHandler;
import com.scylladb.cdc.replicator.operations.RangeDeleteStartOperationHandler;
import com.scylladb.cdc.replicator.operations.RangeDeleteState;
import com.scylladb.cdc.replicator.operations.RowDeleteOperationHandler;
import com.scylladb.cdc.replicator.operations.UnpreparedUpdateOperationHandler;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;

public class ReplicatorConsumer implements RawChangeConsumer {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private final Session session;
    private final Driver3FromLibraryTranslator driver3FromLibraryTranslator;
    private final ConsistencyLevel cl;
    private final TableMetadata table;
    private final Map<RawChange.OperationType, CdcOperationHandler> operationHandlers = new HashMap<>();
    private final PreparedStatement preimageQuery;

    private final Main.Mode mode;
    private final ConcurrentHashMap<ByteBuffer, CdcOperationHandler> nextPostimageOperation = new ConcurrentHashMap<>();

    public static void setBytesUnsafe(Driver3FromLibraryTranslator driver3FromLibraryTranslator, BoundStatement statement, String columnName, Cell cell) {
        TypeCodec<Object> driverCodec = driver3FromLibraryTranslator.getTypeCodec(cell.getColumnDefinition().getCdcLogDataType());
        Object driverObject = driver3FromLibraryTranslator.translate(cell);
        statement.set(columnName, driverObject, driverCodec);
    }

    public static void setBytesUnsafe(Driver3FromLibraryTranslator driver3FromLibraryTranslator, BoundStatement statement, String columnName, RawChange change) {
        Cell cell = change.getCell(columnName);
        setBytesUnsafe(driver3FromLibraryTranslator, statement, columnName, cell);
    }

    private static PreparedStatement createPreimageQuery(Session s, TableMetadata t) {
        Select builder = QueryBuilder.select().all().from(t);
        t.getPrimaryKey().stream().forEach(c -> builder.where(eq(c.getName(), bindMarker(c.getName()))));
        System.out.println("Preimage query: " + builder);
        return s.prepare(builder);
    }

    public ReplicatorConsumer(Main.Mode m, Cluster c, Session s, String keyspace, String tableName, ConsistencyLevel cl) {
        mode = m;
        session = s;
        this.cl = cl;
        table = c.getMetadata().getKeyspace(keyspace).getTable(tableName);
        driver3FromLibraryTranslator = new Driver3FromLibraryTranslator(c.getMetadata());
        preimageQuery = createPreimageQuery(session, table);
        operationHandlers.put(RawChange.OperationType.ROW_UPDATE,
                hasCollection() ? new UnpreparedUpdateOperationHandler(table, driver3FromLibraryTranslator) : new PreparedUpdateOperationHandler(s, driver3FromLibraryTranslator, table));
        operationHandlers.put(RawChange.OperationType.ROW_INSERT, new InsertOperationHandler(s, driver3FromLibraryTranslator, table));
        operationHandlers.put(RawChange.OperationType.ROW_DELETE, new RowDeleteOperationHandler(s, driver3FromLibraryTranslator, table));
        operationHandlers.put(RawChange.OperationType.PARTITION_DELETE, new PartitionDeleteOperationHandler(s, driver3FromLibraryTranslator, table));
        RangeDeleteState rangeDeleteState = new RangeDeleteState();
        operationHandlers.put(RawChange.OperationType.ROW_RANGE_DELETE_INCLUSIVE_LEFT_BOUND, new RangeDeleteStartOperationHandler(rangeDeleteState, true));
        operationHandlers.put(RawChange.OperationType.ROW_RANGE_DELETE_EXCLUSIVE_LEFT_BOUND, new RangeDeleteStartOperationHandler(rangeDeleteState, false));
        operationHandlers.put(RawChange.OperationType.ROW_RANGE_DELETE_INCLUSIVE_RIGHT_BOUND, new RangeDeleteEndOperationHandler(table, driver3FromLibraryTranslator, rangeDeleteState, true));
        operationHandlers.put(RawChange.OperationType.ROW_RANGE_DELETE_EXCLUSIVE_RIGHT_BOUND, new RangeDeleteEndOperationHandler(table, driver3FromLibraryTranslator, rangeDeleteState, false));
    }

    private boolean hasCollection() {
        return table.getColumns().stream().anyMatch(c -> c.getType().isCollection() && !c.getType().isFrozen());
    }

    private CompletableFuture<Void> consumeDelta(RawChange change) {
        RawChange.OperationType operationType = change.getOperationType();
        CdcOperationHandler op = operationHandlers.get(operationType);
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
                nextPostimageOperation.put(streamid, operationHandlers.get(operationType));
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
                CdcOperationHandler op = nextPostimageOperation.remove(streamid);
                if (op != null) {
                    Statement stmt = op.getStatement(change, cl);
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
        for (ColumnDefinitions.Definition d : expectedRow.getColumnDefinitions()) {
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
        logger.atInfo().log("Replicator consuming change: %s, %s", change.getId(), change.getOperationType());

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
