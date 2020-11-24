package com.scylladb.cdc.model.worker;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.Objects;

public final class ChangeSchema {
    public enum DataType {
        INT, TINYINT, BIGINT, TEXT, BLOB, TIMEUUID, BOOLEAN
    }

    public enum ColumnType {
        REGULAR, PARTITION_KEY, CLUSTERING_KEY
    }

    public static final class ColumnDefinition {
        private final String columnName;
        private final DataType dataType;
        private final ColumnType columnType;

        public ColumnDefinition(String columnName, DataType dataType, ColumnType columnType) {
            this.columnName = columnName;
            this.dataType = dataType;
            this.columnType = columnType;
        }

        public String getColumnName() {
            return columnName;
        }

        public DataType getDataType() {
            return dataType;
        }

        public ColumnType getColumnType() {
            return columnType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ColumnDefinition that = (ColumnDefinition) o;
            return columnName.equals(that.columnName) &&
                    dataType == that.dataType &&
                    columnType == that.columnType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(columnName, dataType, columnType);
        }
    }

    private final ImmutableList<ColumnDefinition> columnDefinitions;

    public ChangeSchema(ImmutableList<ColumnDefinition> columnDefinitions) {
        this.columnDefinitions = Preconditions.checkNotNull(columnDefinitions);
    }

    public ImmutableList<ColumnDefinition> getColumnDefinitions() {
        return columnDefinitions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ChangeSchema that = (ChangeSchema) o;
        return columnDefinitions.equals(that.columnDefinitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnDefinitions);
    }
}
