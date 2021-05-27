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
        // are native types. This order
        // is important, as this fact is used in
        // isNative.
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

        /**
         * Constructs a new data type based on a given data type,
         * but with a different value of frozen.
         *
         * @param dataType the data type that the constructed data type will be based on.
         * @param isFrozen whether the constructed type will be frozen.
         */
        public DataType(DataType dataType, boolean isFrozen) {
            this(dataType.cqlType, dataType.typeArguments, dataType.udtType, isFrozen);
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

        /**
         * Returns whether this data type is frozen.
         * <p>
         * This method is only relevant for collection types
         * (such as {@code SET<INT>}), UDTs and tuples. For all
         * other types (native types), this method always
         * returns <code>false</code>. If a type is frozen,
         * it can only be updated as a whole (i.e. a single
         * element cannot be added to a frozen list).
         *
         * @return whether this data type is frozen.
         * @see <a href="https://docs.scylladb.com/getting-started/types/#noteworthy-characteristics">Data Types</a>
         * @see #isAtomic()
         */
        public boolean isFrozen() {
            return frozen;
        }

        /**
         * Returns whether this data type is atomic.
         * <p>
         * A data type is atomic, if it is a
         * <a href="https://docs.scylladb.com/getting-started/types/#native-types">native type</a>
         * (for example <code>INT</code>) or it is a
         * frozen collection (such as {@code FROZEN<SET<INT>>}),
         * frozen UDT or frozen tuple.
         * <p>
         * Columns with non-atomic types can be updated
         * partially (without overriding the whole value), for
         * example by inserting a single element to a {@code SET<INT>}.
         * Due to this fact, CDC operations on such columns
         * are represented differently in the CDC log. See
         * <a href="https://docs.scylladb.com/using-scylla/cdc/cdc-advanced-types/">Advanced column types</a>
         * for more details.
         *
         * @return whether this data type is atomic.
         * @see <a href="https://docs.scylladb.com/using-scylla/cdc/cdc-advanced-types/">Advanced column types</a>
         * @see <a href="https://docs.scylladb.com/getting-started/types/">Data Types</a>
         * @see #isNative()
         * @see #isFrozen()
         */
        public boolean isAtomic() {
            return isNative() || isFrozen();
        }

        /**
         * Returns whether this data type is native.
         * <p>
         * The native types (such as <code>INT</code>) represent
         * only a single value (as opposed to collections, UDTs or tuples).
         * For collections, UDTs or tuples, this method will return <code>true</code> and
         * for all other types it will return <code>false</code>.
         * See <a href="https://docs.scylladb.com/getting-started/types/#native-types">Data types</a>
         * for the full list of available native types.
         *
         * @return whether this data type is native.
         * @see <a href="https://docs.scylladb.com/getting-started/types/#native-types">Data types</a>
         */
        public boolean isNative() {
            return cqlType.compareTo(CqlType.LIST) < 0;
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
                    frozen == dataType.frozen &&
                    Objects.equals(typeArguments, dataType.typeArguments) &&
                    Objects.equals(udtType, dataType.udtType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(cqlType, frozen, typeArguments, udtType);
        }

        @Override
        public String toString() {
            StringBuilder result = new StringBuilder();
            if (frozen) {
                result.append("FROZEN<");
            }
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
            if (frozen) {
                result.append('>');
            }
            return result.toString();
        }
    }

    /**
     * The kind of a Scylla column.
     *
     * @deprecated Use {@link ColumnKind} instead.
     * @see ColumnDefinition#getBaseTableColumnKind()
     */
    @Deprecated
    public enum ColumnType {
        REGULAR, PARTITION_KEY, CLUSTERING_KEY
    }

    /**
     * The kind of a Scylla column. A Scylla
     * column can either be a part of primary
     * key (either as a {@link #PARTITION_KEY} or
     * a {@link #CLUSTERING_KEY}) or be a {@link #REGULAR}
     * column.
     *
     * @see ColumnDefinition#getBaseTableColumnKind()
     * @see <a href="https://docs.scylladb.com/getting-started/ddl/#primary-key">The primary key</a>
     * @see <a href="https://docs.scylladb.com/getting-started/ddl/#clustering-columns">The clustering columns</a>
     */
    public enum ColumnKind {
        /**
         * A regular column. This column
         * is not a part of the primary key.
         */
        REGULAR,

        /**
         * A partition key column. This
         * column is a part of the primary key.
         * There could be many partition key
         * columns in a partition key. A Scylla table
         * must contain at least one partition
         * key column.
         * @see <a href="https://docs.scylladb.com/getting-started/ddl/#primary-key">The primary key</a>
         */
        PARTITION_KEY,

        /**
         * A clustering key column. This
         * column is a part of the primary key.
         * There could be many clustering key
         * columns in a clustering key.
         * @see <a href="https://docs.scylladb.com/getting-started/ddl/#clustering-columns">The clustering columns</a>
         */
        CLUSTERING_KEY
    }

    public static final class ColumnDefinition {
        private final String columnName;
        private final int index;
        private final DataType cdcLogDataType;
        private final DataType baseTableDataType;
        private final ColumnKind baseTableColumnKind;

        public ColumnDefinition(String columnName, int index, DataType cdcLogDataType, DataType baseTableDataType, ColumnKind baseTableColumnKind) {
            this.columnName = columnName;
            this.index = index;
            this.cdcLogDataType = cdcLogDataType;
            this.baseTableDataType = baseTableDataType;
            this.baseTableColumnKind = baseTableColumnKind;
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
            if (isCdcColumn()) {
                throw new IllegalStateException("Cannot get base table data type for CDC columns.");
            }
            return baseTableDataType;
        }
        
        /**
         * Returns the kind of this column in the base table.
         *
         * @deprecated Use {@link #getBaseTableColumnKind()} instead.
         * @return the kind of this column in the base table.
         */
        @Deprecated
        public ColumnType getBaseTableColumnType() {
            switch (getBaseTableColumnKind()) {
                case REGULAR:
                    return ColumnType.REGULAR;
                case CLUSTERING_KEY:
                    return ColumnType.CLUSTERING_KEY;
                case PARTITION_KEY:
                    return ColumnType.PARTITION_KEY;
                default:
                    throw new IllegalStateException("Unknown column kind: " + getBaseTableColumnKind());
            }
        }

        /**
         * Returns the kind of this column in the base table.
         * This method is only relevant for columns that
         * appear in the base table. It will throw
         * {@code IllegalStateException} for CDC columns.
         * Note that the column kind in the CDC log table
         * is different, as the primary key of the CDC log
         * table is different from the base table.
         *
         * @return the kind of this column in the base table.
         * @throws IllegalStateException if the column is a CDC column.
         * @see <a href="https://docs.scylladb.com/using-scylla/cdc/cdc-log-table/">The CDC Log Table</a>
         */
        public ColumnKind getBaseTableColumnKind() {
            if (isCdcColumn()) {
                throw new IllegalStateException("Cannot get base table column type for CDC columns.");
            }
            return baseTableColumnKind;
        }

        @Override
        public int hashCode() {
            return Objects.hash(baseTableColumnKind, baseTableDataType, cdcLogDataType, columnName, index);
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
            return baseTableColumnKind == other.baseTableColumnKind
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

    /**
     * Returns the column definition of the deleted column for the provided
     * column name.
     * <p>
     * This method returns the column definition of the <code>cdc$deleted_</code>
     * column for the given column name. It is only relevant for the regular columns
     * (columns not in a primary key) of the base table, because only
     * those columns have a corresponding deleted column. See
     * <a href="https://docs.scylladb.com/using-scylla/cdc/cdc-basic-operations/">Basic operations in CDC</a> page for
     * more details.
     *
     * @param columnName the column name for which to retrieve the deleted column definition.
     * @return the column definition of the deleted column for the given column name.
     * @see RawChange#isDeleted(String)
     * @see <a href="https://docs.scylladb.com/using-scylla/cdc/cdc-basic-operations/">Basic operations in CDC</a>
     */
    public ColumnDefinition getDeletedColumnDefinition(String columnName) {
        return getColumnDefinition("cdc$deleted_" + columnName);
    }

    /**
     * Returns the column definition of the deleted column for the provided
     * column definition.
     * <p>
     * This method returns the column definition of the <code>cdc$deleted_</code>
     * column for the given column definition. It is only relevant for the regular columns
     * (columns not in a primary key) of the base table, because only
     * those columns have a corresponding deleted column. See
     * <a href="https://docs.scylladb.com/using-scylla/cdc/cdc-basic-operations/">Basic operations in CDC</a> page for
     * more details.
     *
     * @param columnDefinition the column definition for which to retrieve the deleted column definition.
     * @return the column definition of the deleted column for the given column definition.
     * @see RawChange#isDeleted(ColumnDefinition)
     * @see <a href="https://docs.scylladb.com/using-scylla/cdc/cdc-basic-operations/">Basic operations in CDC</a>
     */
    public ColumnDefinition getDeletedColumnDefinition(ColumnDefinition columnDefinition) {
        return getDeletedColumnDefinition(columnDefinition.getColumnName());
    }

    /**
     * Returns the column definition of the deleted elements column for the provided
     * column name.
     * <p>
     * This method returns the column definition of the <code>cdc$deleted_elements_</code>
     * column for the given column name. It is only relevant for the regular columns
     * (columns not in a primary key) of the base table with a non-frozen collection
     * type (for example {@code SET<INT>}). Other columns don't have a
     * corresponding deleted elements column. See
     * <a href="https://docs.scylladb.com/using-scylla/cdc/cdc-advanced-types/">Advanced column types</a> page for
     * more details.
     *
     * @param columnName the column name for which to retrieve the deleted elements column definition.
     * @return the column definition of the deleted elements column for the given column name.
     * @see RawChange#getDeletedElements(String)
     * @see <a href="https://docs.scylladb.com/using-scylla/cdc/cdc-advanced-types/">Advanced column types</a>
     */
    public ColumnDefinition getDeletedElementsColumnDefinition(String columnName) {
        return getColumnDefinition("cdc$deleted_elements_" + columnName);
    }

    /**
     * Returns the column definition of the deleted elements column for the provided
     * column definition.
     * <p>
     * This method returns the column definition of the <code>cdc$deleted_elements_</code>
     * column for the given column definition. It is only relevant for the regular columns
     * (columns not in a primary key) of the base table with a non-frozen collection
     * type (for example {@code SET<INT>}). Other columns don't have a
     * corresponding deleted elements column. See
     * <a href="https://docs.scylladb.com/using-scylla/cdc/cdc-advanced-types/">Advanced column types</a> page for
     * more details.
     *
     * @param columnDefinition the column definition for which to retrieve the deleted elements column definition.
     * @return the column definition of the deleted elements column for the given column definition.
     * @see RawChange#getDeletedElements(ColumnDefinition)
     * @see <a href="https://docs.scylladb.com/using-scylla/cdc/cdc-advanced-types/">Advanced column types</a>
     */
    public ColumnDefinition getDeletedElementsColumnDefinition(ColumnDefinition columnDefinition) {
        return getDeletedElementsColumnDefinition(columnDefinition.getColumnName());
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
