package com.scylladb.cdc.replicator.operations.preimage;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

import java.util.ArrayList;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.cql.Cell;
import com.scylladb.cdc.model.worker.cql.Field;
import com.scylladb.cdc.replicator.driver3.Driver3FromLibraryTranslator;
import com.scylladb.cdc.replicator.driver3.Driver3ToLibraryTranslator;
import com.scylladb.cdc.replicator.operations.CdcOperationHandler;
import com.scylladb.cdc.replicator.operations.FutureUtils;

public class PreImageOperationHandler implements CdcOperationHandler {
    private final Session session;
    private final TableMetadata tableMetadata;
    private final Driver3FromLibraryTranslator driver3FromLibraryTranslator;
    private final PreparedStatement statement;

    public PreImageOperationHandler(Session session, Driver3FromLibraryTranslator driver3FromLibraryTranslator,
                                    TableMetadata tableMetadata) {
        this.session = session;
        this.driver3FromLibraryTranslator = driver3FromLibraryTranslator;
        this.tableMetadata = tableMetadata;
        this.statement = createPreimageQuery();
    }

    private PreparedStatement createPreimageQuery() {
        Select builder = QueryBuilder.select().all().from(tableMetadata);
        tableMetadata.getPrimaryKey().forEach(c -> builder.where(eq(c.getName(), bindMarker(c.getName()))));
        return session.prepare(builder);
    }

    @Override
    public CompletableFuture<Void> handle(RawChange change, ConsistencyLevel cl) {
        BoundStatement stmt = statement.bind();
        for (ChangeSchema.ColumnDefinition cd : change.getSchema().getNonCdcColumnDefinitions()) {
            if (cd.getBaseTableColumnKind() != ChangeSchema.ColumnKind.PARTITION_KEY
                    && cd.getBaseTableColumnKind() != ChangeSchema.ColumnKind.CLUSTERING_KEY) {
                continue;
            }

            Cell cell = change.getCell(cd.getColumnName());
            TypeCodec<Object> driverCodec = driver3FromLibraryTranslator.getTypeCodec(cell.getColumnDefinition().getCdcLogDataType());
            Object driverObject = driver3FromLibraryTranslator.translate(cell);
            stmt.set(cd.getColumnName(), driverObject, driverCodec);
        }
        stmt.setConsistencyLevel(ConsistencyLevel.ALL);
        stmt.setIdempotent(true);
        return FutureUtils.transformDeferred(session.executeAsync(stmt), r -> checkPreimage(change, r));
    }

    private CompletableFuture<Void> checkPreimage(RawChange c, ResultSet rs) {
        if (rs.getAvailableWithoutFetching() == 0) {
            if (rs.isFullyFetched()) {
                Set<String> primaryColumns = tableMetadata.getPrimaryKey().stream().map(ColumnMetadata::getName)
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
                Object libraryObject = asObject(libraryCell);
                ChangeSchema.DataType baseType = calculateBaseType(libraryCell.getColumnDefinition());
                Object expectedLibraryObject = Driver3ToLibraryTranslator.translate(expectedRow.getObject(d.getName()), baseType);

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

    private static boolean isBaseTypeNonfrozenList(ChangeSchema.ColumnDefinition columnDefinition) {
        ChangeSchema.DataType cellBaseType = columnDefinition.getBaseTableDataType();
        return cellBaseType.getCqlType() == ChangeSchema.CqlType.LIST && !cellBaseType.isFrozen();
    }

    /* Casts the given preimage cell to a value as if it would appear in a standard CQL read from the base table. */
    private static Object asObject(Cell preimageCell) {
        if (isBaseTypeNonfrozenList(preimageCell.getColumnDefinition())) {
            // The keys (timeuuids) define the order of values in a non-frozen list
            // and we store our maps in a way that preserves the order of keys returned by the driver.
            // To get the list as it appears in a CQL base table read, we simply drop all keys:
            return new ArrayList<Field>(preimageCell.getMap().values());
        }
        return preimageCell.getAsObject();
    }

    // hack: we're using ChangeSchema.DataType to represent base table types
    // FIXME: once colDef.getBaseTableDataType() is consistent with colDef.getCdcLogDataType(), remove this
    //  with colDef.getBaseTableDataType().
    private static ChangeSchema.DataType calculateBaseType(ChangeSchema.ColumnDefinition colDef) {
        ChangeSchema.DataType logType = colDef.getCdcLogDataType();
        if (isBaseTypeNonfrozenList(colDef)) {
            return ChangeSchema.DataType.list(logType.getTypeArguments().get(1));
        }
        return logType;
    }

}
