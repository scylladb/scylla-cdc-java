package com.scylladb.cdc.model.worker;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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

        /*
         * Used in MAP, LIST, SET and TUPLE.
         */
        private final ImmutableList<DataType> typeArguments;

        public static class UdtType {
            /*
             * Map from field name to its DataType.
             * Type ImmutableMap is ordered by insertion time,
             * so the order of entries matches the order in UDT.
             */
            private final ImmutableMap<String, DataType> fields;

            private final String keyspace;
            private final String name;

            public UdtType(ImmutableMap<String, DataType> fields, String keyspace, String name) {
                this.fields = fields;
                this.keyspace = keyspace;
                this.name = name;
            }

            public ImmutableMap<String, DataType> getFields() {
                return fields;
            }

            public String getKeyspace() {
                return keyspace;
            }

            public String getName() {
                return name;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                UdtType udtType = (UdtType) o;
                return fields.equals(udtType.fields) &&
                        keyspace.equals(udtType.keyspace) &&
                        name.equals(udtType.name);
            }

            @Override
            public int hashCode() {
                return Objects.hash(fields, keyspace, name);
            }
        }

        private final UdtType udtType;

        public DataType(CqlType cqlType) {
            this(cqlType, null, null);
        }

        public DataType(CqlType cqlType, ImmutableList<DataType> typeArguments) {
            this(cqlType, typeArguments, null);
        }

        public DataType(CqlType cqlType, UdtType udtType) {
            this(cqlType, null, udtType);
        }

        public DataType(CqlType cqlType, ImmutableList<DataType> typeArguments, UdtType udtType) {
            Preconditions.checkArgument(typeArguments == null || udtType == null,
                    "Cannot have both non-null type arguments and UdtType.");

            this.cqlType = cqlType;
            this.typeArguments = typeArguments;
            this.udtType = udtType;

            boolean hasTypeArguments = typeArguments != null;
            boolean shouldHaveTypeArguments = cqlType == CqlType.MAP
                    || cqlType == CqlType.LIST || cqlType == CqlType.SET || cqlType == CqlType.TUPLE;

            Preconditions.checkArgument(hasTypeArguments == shouldHaveTypeArguments,
                    "Unexpected value of type arguments for this CQL type: " + cqlType.name());

            boolean hasUdtType = udtType != null;
            boolean shouldHaveUdtType = cqlType == CqlType.UDT;

            Preconditions.checkArgument(hasUdtType == shouldHaveUdtType,
                    "Unexpected value of UdtType for this CQL type: " + cqlType.name());
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

        public UdtType getUdtType() {
            if (udtType == null) {
                throw new IllegalStateException("Cannot get UdtType for this CQL type: " + cqlType.name());
            }
            return udtType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DataType dataType = (DataType) o;
            return cqlType == dataType.cqlType &&
                    Objects.equals(typeArguments, dataType.typeArguments) &&
                    Objects.equals(udtType, dataType.udtType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(cqlType, typeArguments, udtType);
        }

        @Override
        public String toString() {
            StringBuilder result = new StringBuilder();
            result.append(cqlType.name());
            if (typeArguments != null) {
                result.append('<');
                result.append(typeArguments.stream().map(DataType::toString).collect(Collectors.joining(", ")));
                result.append('>');
            }
            if (udtType != null) {
                result.append('(').append(udtType.keyspace).append('.').append(udtType.name).append(')');
                result.append('{');
                result.append(udtType.getFields().entrySet().stream().map(e -> e.getKey() + " " + e.getValue())
                        .collect(Collectors.joining(", ")));
                result.append('}');
            }
            return result.toString();
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

    public ColumnDefinition getColumnDefinition(String columnName) {
        // TODO - do not linearly search
        Optional<ColumnDefinition> result = columnDefinitions.stream().filter(c -> c.getColumnName().equals(columnName)).findFirst();
        return result.orElseThrow(() -> new IllegalArgumentException("Column name " + columnName + " is not present in change schema."));
    }

    // TODO - add getTableName() here.

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
