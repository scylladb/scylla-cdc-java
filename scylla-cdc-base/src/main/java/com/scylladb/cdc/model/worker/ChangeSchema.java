package com.scylladb.cdc.model.worker;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.Objects;

public class ChangeSchema {
    // TODO - Support more information about UDT types and similar.
    public enum DataType {
        ASCII,
        BIGINT,
        BLOB,
        BOOLEAN,
        COUNTER,
        DECIMAL,
        DOUBLE,
        FLOAT,
        INT,
        TEXT,
        TIMESTAMP,
        UUID,
        VARCHAR,
        VARINT,
        TIMEUUID,
        INET,
        DATE,
        TIME,
        SMALLINT,
        TINYINT,
        DURATION,
        LIST,
        MAP,
        SET,
        UDT,
        TUPLE,
    }

    public enum ColumnType {
        REGULAR, PARTITION_KEY, CLUSTERING_KEY
    }

    public static final class ColumnDefinition {
        private final String columnName;
        private final DataType dataType;
        private final ColumnType baseTableColumnType;

        public ColumnDefinition(String columnName, DataType dataType, ColumnType baseTableColumnType) {
            this.columnName = columnName;
            this.dataType = dataType;
            this.baseTableColumnType = baseTableColumnType;
        }

        public String getColumnName() {
            return columnName;
        }

        public DataType getDataType() {
            return dataType;
        }

        public ColumnType getBaseTableColumnType() {
            // TODO - incorrectly returns REGULAR when
            // executed on cdc$ columns. Should throw
            // instead.
            return baseTableColumnType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ColumnDefinition that = (ColumnDefinition) o;
            return columnName.equals(that.columnName) &&
                    dataType == that.dataType &&
                    baseTableColumnType == that.baseTableColumnType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(columnName, dataType, baseTableColumnType);
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
