package com.scylladb.cdc.cql.driver4;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.scylladb.cdc.model.worker.ChangeSchema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Builds a {@link ChangeSchema} from a CDC log row and the base table metadata, the counterpart of
 * {@code Driver3SchemaFactory}.
 * <p>
 * The two schemas may straddle one concurrent {@code ALTER TABLE}: either can be the older one.
 * The result always matches the CDC log schema. The CDC log schema is used wherever it is enough,
 * and the base table schema is read only through {@link BaseTableSchema}, whose methods stay
 * correct across one such change. When they cannot, {@link UnresolvableSchemaInconsistencyException}
 * is thrown rather than a wrong schema returned. Two changes in between, such as a drop and a
 * re-create of the table, are not handled.
 */
final class Driver4SchemaFactory {
    private static final String SCYLLA_CDC_LOG_SUFFIX = "_scylla_cdc_log";

    // 4.x has a single text type, which 3.x reports as VARCHAR.
    private static final Map<DataType, ChangeSchema.CqlType> NATIVE_TYPES = ImmutableMap.<DataType, ChangeSchema.CqlType>builder()
            .put(DataTypes.ASCII, ChangeSchema.CqlType.ASCII)
            .put(DataTypes.BIGINT, ChangeSchema.CqlType.BIGINT)
            .put(DataTypes.BLOB, ChangeSchema.CqlType.BLOB)
            .put(DataTypes.BOOLEAN, ChangeSchema.CqlType.BOOLEAN)
            .put(DataTypes.COUNTER, ChangeSchema.CqlType.COUNTER)
            .put(DataTypes.DECIMAL, ChangeSchema.CqlType.DECIMAL)
            .put(DataTypes.DOUBLE, ChangeSchema.CqlType.DOUBLE)
            .put(DataTypes.FLOAT, ChangeSchema.CqlType.FLOAT)
            .put(DataTypes.INT, ChangeSchema.CqlType.INT)
            .put(DataTypes.TEXT, ChangeSchema.CqlType.VARCHAR)
            .put(DataTypes.TIMESTAMP, ChangeSchema.CqlType.TIMESTAMP)
            .put(DataTypes.UUID, ChangeSchema.CqlType.UUID)
            .put(DataTypes.VARINT, ChangeSchema.CqlType.VARINT)
            .put(DataTypes.TIMEUUID, ChangeSchema.CqlType.TIMEUUID)
            .put(DataTypes.INET, ChangeSchema.CqlType.INET)
            .put(DataTypes.DATE, ChangeSchema.CqlType.DATE)
            .put(DataTypes.TIME, ChangeSchema.CqlType.TIME)
            .put(DataTypes.SMALLINT, ChangeSchema.CqlType.SMALLINT)
            .put(DataTypes.TINYINT, ChangeSchema.CqlType.TINYINT)
            .put(DataTypes.DURATION, ChangeSchema.CqlType.DURATION)
            .build();

    private Driver4SchemaFactory() {
    }

    /**
     * Builds the change schema of a CDC log row, as of the CDC log schema the row was read with.
     *
     * @throws UnresolvableSchemaInconsistencyException if the base table schema is missing or too
     *                                                  different to build a correct schema.
     */
    static ChangeSchema getChangeSchema(Row cdcRow, Metadata metadata) throws UnresolvableSchemaInconsistencyException {
        Preconditions.checkNotNull(cdcRow);
        Preconditions.checkNotNull(metadata);
        List<ColumnDefinition> cdcColumns = new ArrayList<>();
        for (ColumnDefinition cdcColumn : cdcRow.getColumnDefinitions()) {
            cdcColumns.add(cdcColumn);
        }
        Preconditions.checkArgument(!cdcColumns.isEmpty());
        CqlIdentifier keyspace = cdcColumns.get(0).getKeyspace();
        String cdcLogTableName = cdcColumns.get(0).getTable().asInternal();
        Preconditions.checkArgument(cdcLogTableName.endsWith(SCYLLA_CDC_LOG_SUFFIX));
        String baseTableName = cdcLogTableName.substring(0, cdcLogTableName.length() - SCYLLA_CDC_LOG_SUFFIX.length());

        Optional<KeyspaceMetadata> keyspaceMetadata = metadata.getKeyspace(keyspace);
        if (!keyspaceMetadata.isPresent()) {
            throw new UnresolvableSchemaInconsistencyException(
                    String.format("Keyspace '%s' is missing from the cluster metadata.", keyspace.asInternal()));
        }
        Optional<TableMetadata> tableMetadata = keyspaceMetadata.get().getTable(CqlIdentifier.fromInternal(baseTableName));
        if (!tableMetadata.isPresent()) {
            throw new UnresolvableSchemaInconsistencyException(String.format(
                    "Table '%s' is missing from the keyspace '%s' metadata.", baseTableName, keyspace.asInternal()));
        }
        return getChangeSchema(cdcColumns, tableMetadata.get());
    }

    static ChangeSchema getChangeSchema(List<ColumnDefinition> cdcColumns, TableMetadata baseTable)
            throws UnresolvableSchemaInconsistencyException {
        Set<String> cdcColumnNames = cdcColumns.stream().map(c -> c.getName().asInternal()).collect(Collectors.toSet());
        BaseTableSchema baseTableSchema = new BaseTableSchema(baseTable);

        ImmutableList.Builder<ChangeSchema.ColumnDefinition> builder = ImmutableList.builder();
        int index = 0;
        for (ColumnDefinition cdcColumn : cdcColumns) {
            String columnName = cdcColumn.getName().asInternal();
            ChangeSchema.DataType cdcDataType = translateCdcColumnDataType(cdcColumn.getType());

            ChangeSchema.DataType baseTableDataType = null;
            ChangeSchema.ColumnKind baseTableColumnKind = null;
            if (!columnName.startsWith("cdc$")) {
                baseTableDataType = inferBaseTableDataType(cdcColumnNames, baseTableSchema, columnName, cdcDataType);
                baseTableColumnKind = baseTableSchema.getColumnKind(columnName, cdcColumnNames);
            }
            builder.add(new ChangeSchema.ColumnDefinition(columnName, index++, cdcDataType, baseTableDataType, baseTableColumnKind));
        }
        return new ChangeSchema(builder.build());
    }

    /**
     * Returns the base table type of a non-metadata column, as of the CDC log schema.
     */
    private static ChangeSchema.DataType inferBaseTableDataType(Set<String> cdcColumnNames, BaseTableSchema baseTableSchema,
            String columnName, ChangeSchema.DataType cdcDataType) throws UnresolvableSchemaInconsistencyException {
        if (cdcDataType.isNative()) {
            return cdcDataType;
        }
        // The CDC log freezes every non-native type. A cdc$deleted_elements_ column exists only
        // for a column that is not frozen in the base table.
        if (!cdcColumnNames.contains("cdc$deleted_elements_" + columnName)) {
            return cdcDataType;
        }
        // LIST<X> and MAP<TIMEUUID, X> are both FROZEN<MAP<TIMEUUID, X>> in the CDC log.
        if (cdcDataType.getCqlType() == ChangeSchema.CqlType.MAP
                && cdcDataType.getTypeArguments().get(0).getCqlType() == ChangeSchema.CqlType.TIMEUUID
                && baseTableSchema.isList(columnName)) {
            return new ChangeSchema.DataType(ChangeSchema.CqlType.LIST,
                    Collections.singletonList(cdcDataType.getTypeArguments().get(1)), false);
        }
        return new ChangeSchema.DataType(cdcDataType, false);
    }

    /**
     * The base table schema, answering as of the CDC log schema.
     * <p>
     * Before changing or adding a method, check it against each change that can separate the two
     * schemas: a column added, removed or renamed (only clustering columns can be renamed), or a
     * type changed, with either schema the newer one.
     */
    private static final class BaseTableSchema {
        private final TableMetadata table;
        private final Set<String> partitionColumns;
        private final Set<String> clusteringColumns;
        private final Set<String> regularColumns;

        BaseTableSchema(TableMetadata table) {
            this.table = table;
            this.partitionColumns = names(table.getPartitionKey());
            this.clusteringColumns = names(table.getClusteringColumns().keySet());
            this.regularColumns = names(table.getColumns().values());
            this.regularColumns.removeAll(partitionColumns);
            this.regularColumns.removeAll(clusteringColumns);
        }

        private static Set<String> names(Collection<ColumnMetadata> columns) {
            return columns.stream().map(c -> c.getName().asInternal()).collect(Collectors.toCollection(HashSet::new));
        }

        /**
         * Returns the kind of a column.
         * <ul>
         *   <li>Added or removed columns are regular, and a regular column is found in neither key.</li>
         *   <li>Type changes keep the column set.</li>
         *   <li>A renamed clustering column leaves a base clustering column missing from the CDC log.
         *       Then only that rename happened, so a column in both schemas that is regular in the
         *       base is regular, and any other is the renamed clustering column.</li>
         * </ul>
         */
        ChangeSchema.ColumnKind getColumnKind(String columnName, Set<String> cdcColumnNames) {
            if (partitionColumns.contains(columnName)) {
                return ChangeSchema.ColumnKind.PARTITION_KEY;
            } else if (clusteringColumns.contains(columnName)) {
                return ChangeSchema.ColumnKind.CLUSTERING_KEY;
            } else if (!cdcColumnNames.containsAll(clusteringColumns)) {
                return cdcColumnNames.contains(columnName) && regularColumns.contains(columnName)
                        ? ChangeSchema.ColumnKind.REGULAR
                        : ChangeSchema.ColumnKind.CLUSTERING_KEY;
            }
            return ChangeSchema.ColumnKind.REGULAR;
        }

        /**
         * Returns whether a column that is {@code LIST<X>} or {@code MAP<TIMEUUID, X>} as of the CDC
         * log schema is a list.
         * <ul>
         *   <li>Neither type can change into the other: a non-frozen collection can change only to
         *       {@code BLOB}.</li>
         *   <li>So a base column that is missing, or of any other type, means the base schema is
         *       newer and the answer is unknown, and this throws.</li>
         * </ul>
         */
        boolean isList(String columnName) throws UnresolvableSchemaInconsistencyException {
            ColumnMetadata column = table.getColumns().get(CqlIdentifier.fromInternal(columnName));
            if (column == null) {
                throw new UnresolvableSchemaInconsistencyException(String.format("Column '%s' in base table metadata "
                        + "is missing, making it impossible to deduce whether it was a LIST at a time of CDC log schema.",
                        columnName));
            }
            DataType type = column.getType();
            boolean isList = type instanceof ListType;
            boolean isMapTimeuuid = type instanceof MapType && DataTypes.TIMEUUID.equals(((MapType) type).getKeyType());
            if (!isList && !isMapTimeuuid) {
                throw new UnresolvableSchemaInconsistencyException(String.format("The type of column '%s' in base "
                        + "table metadata is %s, making it impossible to deduce whether it was a LIST at a time of CDC "
                        + "log schema.", columnName, type.asCql(false, true)));
            }
            return isList;
        }
    }

    /**
     * Thrown when the base table schema and the CDC log schema are too different to build a
     * correct change schema.
     */
    static final class UnresolvableSchemaInconsistencyException extends Exception {
        private UnresolvableSchemaInconsistencyException(String message) {
            super(message);
        }
    }

    /**
     * Translates the type of a CDC log column. Every non-native CDC log type is frozen, even when
     * the driver type, from prepared statement metadata, does not say so.
     */
    static ChangeSchema.DataType translateCdcColumnDataType(DataType driverType) {
        ChangeSchema.CqlType nativeType = NATIVE_TYPES.get(driverType);
        if (nativeType != null) {
            return new ChangeSchema.DataType(nativeType);
        }
        if (driverType instanceof ListType) {
            return new ChangeSchema.DataType(ChangeSchema.CqlType.LIST,
                    translate(Collections.singletonList(((ListType) driverType).getElementType())), true);
        } else if (driverType instanceof SetType) {
            return new ChangeSchema.DataType(ChangeSchema.CqlType.SET,
                    translate(Collections.singletonList(((SetType) driverType).getElementType())), true);
        } else if (driverType instanceof MapType) {
            MapType mapType = (MapType) driverType;
            return new ChangeSchema.DataType(ChangeSchema.CqlType.MAP,
                    translate(ImmutableList.of(mapType.getKeyType(), mapType.getValueType())), true);
        } else if (driverType instanceof UserDefinedType) {
            UserDefinedType userType = (UserDefinedType) driverType;
            ImmutableMap.Builder<String, ChangeSchema.DataType> fields = ImmutableMap.builder();
            for (int i = 0; i < userType.getFieldNames().size(); i++) {
                fields.put(userType.getFieldNames().get(i).asInternal(), translateCdcColumnDataType(userType.getFieldTypes().get(i)));
            }
            return new ChangeSchema.DataType(ChangeSchema.CqlType.UDT, new ChangeSchema.DataType.UdtType(
                    fields.build(), userType.getKeyspace().asInternal(), userType.getName().asInternal()), true);
        } else if (driverType instanceof TupleType) {
            List<DataType> componentTypes = ((TupleType) driverType).getComponentTypes();
            Preconditions.checkArgument(!componentTypes.isEmpty());
            return new ChangeSchema.DataType(ChangeSchema.CqlType.TUPLE, translate(componentTypes), true);
        }
        throw new RuntimeException(String.format("Data type %s is currently not supported.", driverType.asCql(false, true)));
    }

    private static ImmutableList<ChangeSchema.DataType> translate(List<DataType> driverTypes) {
        return driverTypes.stream().map(Driver4SchemaFactory::translateCdcColumnDataType)
                .collect(Collectors.collectingAndThen(Collectors.toList(), ImmutableList::copyOf));
    }
}
