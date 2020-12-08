package com.scylladb.cdc.model.worker;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.Objects;
import java.util.stream.Collectors;

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
        private final DataType cdcLogDataType;
        private final ColumnType baseTableColumnType;

        public ColumnDefinition(String columnName, DataType cdcLogDataType, ColumnType baseTableColumnType) {
            this.columnName = columnName;
            this.cdcLogDataType = cdcLogDataType;
            this.baseTableColumnType = baseTableColumnType;
        }

        public boolean isCdcColumn() {
            return this.columnName.startsWith("cdc$");
        }

        public String getColumnName() {
            return columnName;
        }

        public DataType getCdcLogDataType() {
            return cdcLogDataType;
        }

        public ColumnType getBaseTableColumnType() {
            if (isCdcColumn()) {
                throw new IllegalStateException("Cannot get base table column type for CDC columns.");
            }
            return baseTableColumnType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ColumnDefinition that = (ColumnDefinition) o;
            return columnName.equals(that.columnName) &&
                    cdcLogDataType == that.cdcLogDataType &&
                    baseTableColumnType == that.baseTableColumnType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(columnName, cdcLogDataType, baseTableColumnType);
        }
    }

    private final ImmutableList<ColumnDefinition> columnDefinitions;

    public ChangeSchema(ImmutableList<ColumnDefinition> columnDefinitions) {
        this.columnDefinitions = Preconditions.checkNotNull(columnDefinitions);
    }

    public ImmutableList<ColumnDefinition> getAllColumnDefinitions() {
        return columnDefinitions;
    }

    public ImmutableList<ColumnDefinition> getCdcColumnDefinitions() {
        return columnDefinitions.stream().filter(ColumnDefinition::isCdcColumn)
                .collect(Collectors.collectingAndThen(Collectors.toList(), ImmutableList::copyOf));
    }

    public ImmutableList<ColumnDefinition> getNonCdcColumnDefinitions() {
        return columnDefinitions.stream().filter(c -> !c.isCdcColumn())
                .collect(Collectors.collectingAndThen(Collectors.toList(), ImmutableList::copyOf));
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
