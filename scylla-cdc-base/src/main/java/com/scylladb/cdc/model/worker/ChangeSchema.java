package com.scylladb.cdc.model.worker;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

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

        // All types up to this point
        // are atomic types. This order
        // is important, as this fact is used in
        // isAtomic.
        LIST,
        MAP,
        SET,
        UDT,
        TUPLE,
    }

    public static class DataType {
        private final CqlType cqlType;
        private final boolean frozen;

        /*
         * Used in MAP, LIST, SET and TUPLE.
         */
        private final List<DataType> typeArguments;

        public static class UdtType {
            /*
             * Map from field name to its DataType.
             */
            private final Map<String, DataType> fields;

            private final String keyspace;
            private final String name;

            public UdtType(Map<String, DataType> fields, String keyspace, String name) {
                this.fields = fields;
                this.keyspace = keyspace;
                this.name = name;
            }

            public Map<String, DataType> getFields() {
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
            this(cqlType, null, null, false);
        }

        public DataType(CqlType cqlType, List<DataType> typeArguments, boolean frozen) {
            this(cqlType, typeArguments, null, frozen);
        }

        public DataType(CqlType cqlType, UdtType udtType, boolean frozen) {
            this(cqlType, null, udtType, frozen);
        }

        public DataType(CqlType cqlType, List<DataType> typeArguments, UdtType udtType, boolean frozen) {
            Preconditions.checkArgument(typeArguments == null || udtType == null,
                    "Cannot have both non-null type arguments and UdtType.");

            this.cqlType = cqlType;
            this.typeArguments = typeArguments;
            this.udtType = udtType;
            this.frozen = frozen;

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

        public static DataType list(DataType valueType) {
            return list(valueType, false);
        }

        public static DataType list(DataType valueType, boolean frozen) {
            return new DataType(CqlType.LIST, ImmutableList.of(valueType), frozen);
        }

        public CqlType getCqlType() {
            return cqlType;
        }

        public boolean isFrozen() {
            return frozen;
        }

        public boolean isAtomic() {
            return cqlType.compareTo(CqlType.LIST) < 0 || isFrozen();
        }

        public List<DataType> getTypeArguments() {
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

    // FIXME: misnomer: this should be named ColumnKind
    public enum ColumnType {
        REGULAR, PARTITION_KEY, CLUSTERING_KEY
    }

    public static final class ColumnDefinition {
        private final String columnName;
        private final int index;
        private final DataType cdcLogDataType;
        private final DataType baseTableDataType;
        private final ColumnType baseTableColumnType;
        private final boolean baseIsNonfrozenList;

        public ColumnDefinition(String columnName, int index, DataType cdcLogDataType, DataType baseTableDataType, ColumnType baseTableColumnType, boolean baseIsNonfrozenList) {
            this.columnName = columnName;
            this.index = index;
            this.cdcLogDataType = cdcLogDataType;
            this.baseTableDataType = baseTableDataType;
            this.baseTableColumnType = baseTableColumnType;
            this.baseIsNonfrozenList = baseIsNonfrozenList;
        }

        public int getIndex() {
            return index;
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

        public DataType getBaseTableDataType() {
            return baseTableDataType;
        }
        
        public ColumnType getBaseTableColumnType() {
            if (isCdcColumn()) {
                throw new IllegalStateException("Cannot get base table column type for CDC columns.");
            }
            return baseTableColumnType;
        }

        public boolean baseIsNonfrozenList() {
            if (isCdcColumn()) {
                // TODO: actually, this may make sense for cdc$deleted_ and cdc$deleted_elements_ columns as well
                // but ensure that whoever constructs `ColumnDefinition` passes a correct value
                throw new IllegalStateException("Cannot get base table column type for CDC columns.");
            }
            return baseIsNonfrozenList;
        }

        public ColumnDefinition getDeletedColumn(ChangeSchema schema) {
            if (baseTableDataType == null) {
                throw new IllegalStateException("Cannot get deleted elements column for CDC columns.");
            }
            return schema.getColumnDefinition("cdc$deleted_" + columnName);
        }

        public ColumnDefinition getDeletedElementsColumn(ChangeSchema schema) {
            if (baseTableDataType == null) {
                throw new IllegalStateException("Cannot get deleted elements column for CDC columns.");
            }
            if (baseTableDataType.isAtomic()) {
                throw new IllegalStateException(
                        "Cannot get deleted elements column for frozen or non-collection columns.");
            }
            return schema.getColumnDefinition("cdc$deleted_elements_" + columnName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(baseIsNonfrozenList, baseTableColumnType, baseTableDataType, cdcLogDataType, columnName,
                    index);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof ColumnDefinition)) {
                return false;
            }
            ColumnDefinition other = (ColumnDefinition) obj;
            return baseIsNonfrozenList == other.baseIsNonfrozenList && baseTableColumnType == other.baseTableColumnType
                    && Objects.equals(baseTableDataType, other.baseTableDataType)
                    && Objects.equals(cdcLogDataType, other.cdcLogDataType)
                    && Objects.equals(columnName, other.columnName) && index == other.index;
        }
        
    }

    private final List<ColumnDefinition> columnDefinitions;

    public ChangeSchema(List<ColumnDefinition> columnDefinitions) {
        this.columnDefinitions = Preconditions.checkNotNull(columnDefinitions);
    }

    public List<ColumnDefinition> getAllColumnDefinitions() {
        return columnDefinitions;
    }

    public List<ColumnDefinition> getCdcColumnDefinitions() {
        return columnDefinitions.stream().filter(ColumnDefinition::isCdcColumn)
                .collect(Collectors.collectingAndThen(Collectors.toList(), ImmutableList::copyOf));
    }

    public List<ColumnDefinition> getNonCdcColumnDefinitions() {
        return columnDefinitions.stream().filter(c -> !c.isCdcColumn())
                .collect(Collectors.collectingAndThen(Collectors.toList(), ImmutableList::copyOf));
    }

    public ColumnDefinition getColumnDefinition(int i) {
        return columnDefinitions.get(i);
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
