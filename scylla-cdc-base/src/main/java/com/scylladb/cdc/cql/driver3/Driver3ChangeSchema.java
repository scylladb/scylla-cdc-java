package com.scylladb.cdc.cql.driver3;

import com.datastax.driver.core.Row;
import com.google.common.collect.ImmutableList;
import com.scylladb.cdc.model.worker.ChangeSchema;

public class Driver3ChangeSchema extends ChangeSchema {
    /*
     * Row that was the source of information about the schema.
     */
    private final Row underlyingRow;

    public Driver3ChangeSchema(ImmutableList<ColumnDefinition> columnDefinitions, Row underlyingRow) {
        super(columnDefinitions);
        this.underlyingRow = underlyingRow;
    }

    public com.datastax.driver.core.DataType getDriverType(String columnName) {
        return underlyingRow.getColumnDefinitions().getType(columnName);
    }
}
