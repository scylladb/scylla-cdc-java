package com.scylladb.cdc.cql.driver3;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.google.common.base.Preconditions;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.worker.Change;
import com.scylladb.cdc.model.worker.ChangeId;
import com.scylladb.cdc.model.worker.ChangeSchema;

import java.util.List;
import java.util.stream.Collectors;

import static com.datastax.driver.core.Metadata.quoteIfNecessary;

public final class Driver3Change implements Change {
    private final Row row;

    public Driver3Change(Row row) {
        this.row = Preconditions.checkNotNull(row);
    }

    @Override
    public ChangeId getId() {
        return new ChangeId(new StreamId(row.getBytes(quoteIfNecessary("cdc$stream_id"))),
                row.getUUID(quoteIfNecessary("cdc$time")));
    }

    @Override
    public ChangeSchema getSchema() {
        List<ColumnDefinitions.Definition> driverColumnDefinitions = row.getColumnDefinitions().asList();
        List<ChangeSchema.ColumnDefinition> columnDefinitions =
                driverColumnDefinitions.stream().map(this::translateColumnDefinition).collect(Collectors.toList());
        return new ChangeSchema(columnDefinitions);
    }

    @Override
    public int getInt(String columnName) {
        return row.getInt(columnName);
    }

    private ChangeSchema.ColumnDefinition translateColumnDefinition(ColumnDefinitions.Definition driverDefinition) {
        String columnName = driverDefinition.getName();
        ChangeSchema.ColumnType columnType = translateColumnType(driverDefinition.getType());
        return new ChangeSchema.ColumnDefinition(columnName, columnType);
    }

    private ChangeSchema.ColumnType translateColumnType(DataType driverType) {
        switch (driverType.getName()) {
            case INT:
                return ChangeSchema.ColumnType.INT;
            case TEXT:
                return ChangeSchema.ColumnType.TEXT;
            case BLOB:
                return ChangeSchema.ColumnType.BLOB;
            case TIMEUUID:
                return ChangeSchema.ColumnType.TIMEUUID;
            case BOOLEAN:
                return ChangeSchema.ColumnType.BOOLEAN;
            case TINYINT:
                return ChangeSchema.ColumnType.TINYINT;
            case BIGINT:
                return ChangeSchema.ColumnType.BIGINT;
            default:
                throw new RuntimeException(String.format("Type %s is currently not supported.", driverType.getName()));
        }
    }

}
