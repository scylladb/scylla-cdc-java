package com.scylladb.cdc.model.worker;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.Objects;
import java.util.stream.Collectors;

public class ChangeSchema {
    public enum CqlType {
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

    public static class DataType {
        private final CqlType cqlType;
        private final ImmutableList<DataType> typeArguments;

        public DataType(CqlType cqlType) {
            this(cqlType, null);
        }

        public DataType(CqlType cqlType, ImmutableList<DataType> typeArguments) {
            this.cqlType = cqlType;
            this.typeArguments = typeArguments;

            boolean hasTypeArguments = typeArguments != null;
            boolean shouldHaveTypeArguments = cqlType == CqlType.MAP
                    || cqlType == CqlType.LIST || cqlType == CqlType.SET || cqlType == CqlType.TUPLE;

            Preconditions.checkArgument(hasTypeArguments == shouldHaveTypeArguments,
                    "Unexpected value of type arguments for this CQL type: " + cqlType.name());
        }

        public CqlType getCqlType() {
            return cqlType;
        }

        public ImmutableList<DataType> getTypeArguments() {
            if (typeArguments == null) {
                throw new IllegalStateException("Cannot get type arguments for this CQL type: " + cqlType.name());
            }
            return typeArguments;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DataType dataType = (DataType) o;
            return cqlType == dataType.cqlType &&
                    Objects.equals(typeArguments, dataType.typeArguments);
        }

        @Override
        public int hashCode() {
            return Objects.hash(cqlType, typeArguments);
        }

        @Override
        public String toString() {
            String result = cqlType.name();
            if (typeArguments != null) {
                result += '<';
                result += typeArguments.stream().map(DataType::toString).collect(Collectors.joining(", "));
                result += '>';
            }
            return result;
        }
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
