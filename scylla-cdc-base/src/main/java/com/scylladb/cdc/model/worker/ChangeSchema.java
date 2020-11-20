package com.scylladb.cdc.model.worker;

import java.util.List;

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

    private final List<ColumnDefinition> columnDefinitions;

    public ChangeSchema(List<ColumnDefinition> columnDefinitions) {
        this.columnDefinitions = columnDefinitions;
    }

    public List<ColumnDefinition> getColumnDefinitions() {
        return columnDefinitions;
    }
}
