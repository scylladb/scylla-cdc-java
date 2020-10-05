package com.scylladb.cdc.model.worker;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

public final class ChangeSchema {
    public enum ColumnType {
        INT, TINYINT, BIGINT, TEXT, BLOB, TIMEUUID, BOOLEAN
    }

    public static final class ColumnDefinition {
        private final String columnName;
        private final ColumnType columnType;

        public ColumnDefinition(String columnName, ColumnType columnType) {
            this.columnName = columnName;
            this.columnType = columnType;
        }

        public String getColumnName() {
            return columnName;
        }

        public ColumnType getColumnType() {
            return columnType;
        }
    }

    private final ImmutableList<ColumnDefinition> columnDefinitions;

    public ChangeSchema(ImmutableList<ColumnDefinition> columnDefinitions) {
        this.columnDefinitions = Preconditions.checkNotNull(columnDefinitions);
    }

    public ImmutableList<ColumnDefinition> getColumnDefinitions() {
        return columnDefinitions;
    }
}
