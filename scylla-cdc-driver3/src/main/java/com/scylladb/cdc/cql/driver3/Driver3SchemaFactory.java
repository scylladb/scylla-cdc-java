package com.scylladb.cdc.cql.driver3;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.scylladb.cdc.model.worker.ChangeSchema;

/**
 * A factory of {@link ChangeSchema} based on
 * the base table schema and CDC log schema.
 * <p>
 * This implementation does not require that
 * the base table schema and CDC log schema
 * are both fresh.
 * <p>
 * For example, in case of <code>ALTER TABLE</code>,
 * base table schema containing a schema before
 * that operation and CDC log schema containing a schema
 * after that operation, this factory will
 * build a correct <code>ChangeSchema</code>,
 * that will be consistent with the CDC log schema,
 * properly synthesizing the base table schema parts
 * in a created <code>ChangeSchema</code>.
 * <p>
 * A substantial effort has been made to show that
 * this factory works correctly even when
 * the provided schemas are inconsistent w.r.t.
 * each other. The "inconsistent" term that is used
 * in this class refers to a situation when a concurrent
 * <code>ALTER TABLE</code> happened and
 * one of the schemas (base table schema or CDC table schema)
 * is old (before <code>ALTER TABLE</code>), while
 * the other one is new (after <code>ALTER TABLE</code>).
 * The only assumption is that
 * only at most a single schema change
 * has been performed (single <code>ALTER TABLE</code>)
 * between the CDC log schema fetch and base table
 * schema fetch. This factory will not properly handle
 * tricky cases, such as <code>DROP TABLE</code>
 * and immediately <code>CREATE TABLE</code>
 * with the same name (two schema changes).
 * <p>
 * This implementation of this factory is based
 * on a simple idea: use the CDC log schema
 * as much as possible. For some information
 * (such as whether a column was part
 * of the primary key in the base table), the
 * factory uses the base table schema. However,
 * this usage is made only through the {@link BaseTableSchema}
 * class and all methods of this class are proven
 * to be correct under potential inconsistencies
 * (see JavaDocs in {@link BaseTableSchema}).
 * <p>
 * In a case that the inconsistency cannot
 * be resolved, {@link UnresolvableSchemaInconsistencyException}
 * is thrown and in no circumstance a wrong
 * result will be returned.
 */
class Driver3SchemaFactory {
    private static final String SCYLLA_CDC_LOG_SUFFIX = "_scylla_cdc_log";

    /**
     * Builds a change schema from the CDC log table schema
     * and the base table schema.
     * <p>
     * The CDC log table schema is extracted from
     * the column definitions of the given CDC log row.
     * The base table schema is fetched from the
     * given metadata object.
     * <p>
     * The resulting schema combines information from
     * both schemas. This implementation does not require
     * that the base table schema and the CDC log schema
     * are both fresh. In case of a concurrent <code>ALTER TABLE</code>
     * operation, one of the schemas could be stale (for
     * example the base table schema). The resulting change schema
     * returned by this method will reflect how the change
     * schema looked like at a time of CDC log table schema
     * (extracted from the provided CDC log row).
     * <p>
     * In some cases, this method will be unable to
     * to build a change schema due to unresolvable differences
     * between the base table schema and the CDC log schema.
     * {@link UnresolvableSchemaInconsistencyException} is thrown
     * in such a case.
     *
     * @param cdcRow the CDC log row from which the CDC log table schema is extracted from.
     * @param metadata the cluster metadata used to fetch the base table schema.
     * @return a change schema combining both schemas, consistent with CDC log schema.
     * @throws UnresolvableSchemaInconsistencyException if this method was unable
     *                                                  to build a change schema
     *                                                  due to unresolvable differences
     *                                                  between the base table schema
     *                                                  and the CDC log schema.
     */
    public static ChangeSchema getChangeSchema(Row cdcRow, Metadata metadata) throws UnresolvableSchemaInconsistencyException {
        Preconditions.checkNotNull(cdcRow);
        Preconditions.checkNotNull(metadata);

        String keyspace = cdcRow.getColumnDefinitions().getKeyspace(0);
        String cdcLogTableName = cdcRow.getColumnDefinitions().getTable(0);
        Preconditions.checkArgument(cdcLogTableName.endsWith(SCYLLA_CDC_LOG_SUFFIX));

        String baseTableName = cdcLogTableName.substring(0, cdcLogTableName.length() - SCYLLA_CDC_LOG_SUFFIX.length());
        ColumnDefinitions cdcColumnDefinitions = cdcRow.getColumnDefinitions();

        KeyspaceMetadata keyspaceMetadata = metadata.getKeyspace(keyspace);
        if (keyspaceMetadata == null) {
            throw new UnresolvableSchemaInconsistencyException(
                    String.format("Keyspace '%s' is missing from the cluster metadata.", keyspace));
        }

        TableMetadata tableMetadata = keyspaceMetadata.getTable(baseTableName);
        if (tableMetadata == null) {
            throw new UnresolvableSchemaInconsistencyException(
                    String.format("Table '%s' is missing from the keyspace '%s' metadata.", baseTableName, keyspace));
        }

        BaseTableSchema baseTableSchema = new BaseTableSchema(tableMetadata);

        ImmutableList.Builder<ChangeSchema.ColumnDefinition> builder = ImmutableList.builder();

        int index = 0;
        for (ColumnDefinitions.Definition cdcColumn : cdcColumnDefinitions) {
            String columnName = cdcColumn.getName();
            ChangeSchema.DataType cdcDataType = translateCdcColumnDataType(cdcColumn.getType());

            ChangeSchema.DataType baseTableDataType = null;
            ChangeSchema.ColumnKind baseTableColumnKind = null;
            if (!columnName.startsWith("cdc$")) {
                baseTableDataType = inferBaseTableDataType(cdcColumnDefinitions, baseTableSchema, cdcColumn, cdcDataType);
                baseTableColumnKind = baseTableSchema.getColumnKind(columnName, cdcColumnDefinitions);
            }

            builder.add(new ChangeSchema.ColumnDefinition(columnName, index++, cdcDataType, baseTableDataType, baseTableColumnKind));
        }

        return new ChangeSchema(builder.build());
    }

    /**
     * Returns an inferred data type for a corresponding
     * base table column to a given CDC log column.
     * This method should only be called for columns
     * that appear both in the base table and the CDC log table
     * (non-metadata columns).
     * <p>
     * This implementation does not require
     * that the base table schema and the CDC log schema
     * are both fresh. In case of a concurrent <code>ALTER TABLE</code>
     * operation, one of the schemas could be stale (for
     * example the base table schema). The resulting base table data type
     * returned by this method will reflect how the base table data type
     * looked like at a time of CDC log table schema. In case this
     * information cannot be inferred, {@link UnresolvableSchemaInconsistencyException}
     * is thrown.
     * <p>
     * This method relies as much as possible on only the data in
     * CDC log table schema. In some cases it accesses the base
     * table schema through the {@link BaseTableSchema} wrapper object,
     * which is safe under possible schema inconsistencies (see
     * {@link Driver3SchemaFactory} for a definition of a inconsistency).
     *
     * @param cdcColumnDefinitions column definitions of all columns in the CDC log table.
     * @param baseTableSchema the base table schema.
     * @param cdcColumn the CDC log table column for which to generate the base table data type.
     * @param cdcDataType the data type of the {@code cdcColumn}.
     * @return the base table data type of a given column, as of the CDC log schema time.
     * @throws UnresolvableSchemaInconsistencyException if this method was unable
     *                                                  to infer the base table type
     *                                                  due to unresolvable differences
     *                                                  between the base table schema
     *                                                  and the CDC log schema.
     */
    private static ChangeSchema.DataType inferBaseTableDataType(ColumnDefinitions cdcColumnDefinitions,
        BaseTableSchema baseTableSchema, ColumnDefinitions.Definition cdcColumn, ChangeSchema.DataType cdcDataType) throws UnresolvableSchemaInconsistencyException {
        if (!cdcDataType.isNative()) {
            // In the CDC table we have a frozen non-native type (frozen collection,
            // frozen UDT or frozen tuple). However, both non-frozen and frozen
            // non-native types in the base table map to a frozen non-native
            // type in the CDC log table.
            // We use the existence of cdc$deleted_elements_ column as a proxy
            // for the information whether the column was frozen or non-frozen
            // in the base table.

            boolean hasDeletedElementsColumn =
                    cdcColumnDefinitions.contains("cdc$deleted_elements_" + cdcColumn.getName());
            if (hasDeletedElementsColumn) {
                // If the column has cdc$deleted_elements_,
                // that means it was non-frozen in base table.

                // LIST<X>, MAP<TIMEUUID, X> in the base table
                // are both represented as FROZEN<MAP<TIMEUUID, X>>
                // in the CDC log table. This checks if it
                // was a LIST<X> in the base table.
                if (cdcDataType.getCqlType() == ChangeSchema.CqlType.MAP
                        && cdcDataType.getTypeArguments().get(0).getCqlType() == ChangeSchema.CqlType.TIMEUUID
                        && baseTableSchema.isList(cdcColumn.getName())) {
                    return new ChangeSchema.DataType(ChangeSchema.CqlType.LIST,
                            Collections.singletonList(cdcDataType.getTypeArguments().get(1)), false);
                }

                // Unfreeze:
                return new ChangeSchema.DataType(cdcDataType, false);
            } else {
                // No cdc$deleted_elements_ column, so this column was frozen
                // in the base table, so we can just return the cdcDataType.
                return cdcDataType;
            }
        } else {
            // We have a non-collection column. The type
            // in base table is exactly the same.
            return cdcDataType;
        }
    }

    /**
     * A wrapper for the base table schema.
     * <p>
     * The purpose of this class is to mandate
     * that all access to the base table schema
     * is made through this class. This is important,
     * as we must deal with potential inconsistencies (see
     * {@link Driver3SchemaFactory} for a definition of a inconsistency)
     * with the CDC log schema.
     * <p>
     * All methods should return some information
     * from the base table schema <i>as it was at
     * the time of the CDC table schema</i> (possibly
     * "synthesizing" such information).
     * <p>
     * <b>
     * Important: before you modify
     * or add a method to this class, make sure
     * to properly prove that it is safe
     * under potential inconsistencies.
     * Copy the table used in other methods
     * and fill it in.
     * </b>
     */
    private static class BaseTableSchema {
        private final TableMetadata baseTableSchema;
        private final Set<String> partitionColumns;
        private final Set<String> clusteringColumns;
        private final Set<String> regularColumns;

        public BaseTableSchema(TableMetadata baseTableSchema) {
            this.baseTableSchema = baseTableSchema;
            this.partitionColumns = baseTableSchema.getPartitionKey()
                    .stream().map(ColumnMetadata::getName).collect(Collectors.toSet());
            this.clusteringColumns = baseTableSchema.getClusteringColumns()
                    .stream().map(ColumnMetadata::getName).collect(Collectors.toSet());
            this.regularColumns = baseTableSchema.getColumns().stream()
                    .map(ColumnMetadata::getName)
                    .filter(c -> !this.partitionColumns.contains(c))
                    .filter(c -> !this.clusteringColumns.contains(c))
                    .collect(Collectors.toSet());
        }

        /**
         * Returns the column kind for a column name in the base table,
         * as of the time of CDC schema.
         * <p>
         * Correctness of this implementation under possible
         * inconsistencies with the base table schema:
         * <table border="1">
         *     <caption>Correctness under possible base table schema inconsistencies</caption>
         *     <tr>
         *         <th>Inconsistency type</th>
         *         <th>Schema change type</th>
         *         <th>Commentary</th>
         *     </tr>
         *     <tr>
         *         <td>Base schema newer than CDC log schema</td>
         *         <td>Column added</td>
         *         <td>This method will never be called for that added column,
         *             as the column name was not present in the CDC log schema (caller).</td>
         *     </tr>
         *     <tr>
         *         <td>Base schema newer than CDC log schema</td>
         *         <td>Column removed</td>
         *         <td>You cannot remove columns from the primary key. Therefore,
         *             the removed column must have been a regular column. This
         *             method will correctly return REGULAR for that column
         *             (the kind of that column as of the CDC schema time),
         *             as it won't find this column in a primary key of the
         *             base table.</td>
         *     </tr>
         *     <tr>
         *         <td>Base schema newer than CDC log schema</td>
         *         <td>Column renamed</td>
         *         <td>Only clustering key columns can be renamed. We detect
         *             if all clustering key columns in the base table
         *             schema are present in the CDC log schema
         *             (base schema: pk1, ck_new, col1;
         *             CDC log schema: pk1, ck_old, col1). If there is
         *             an inconsistency, that means that a clustering column
         *             was renamed. That means that all other columns are not
         *             changed. If the queried CDC column is a part of partition key
         *             of base schema, that means that it was a partition
         *             key (as of CDC schema time) - partition columns
         *             were not modified (clustering column was modified).
         *             Similarly for regular columns. But, if the column
         *             is missing from the base table schema, that means
         *             that we are seeing a old clustering column.
         *             </td>
         *     </tr>
         *     <tr>
         *         <td>Base schema newer than CDC log schema</td>
         *         <td>Column data type changed</td>
         *         <td>The column data type change does not change
         *             the column set (column names, whether the columns
         *             are in the primary key). Therefore, the schemas
         *             are consistent with regard to column kind.</td>
         *     </tr>
         *     <tr>
         *         <td>CDC log schema newer than base schema</td>
         *         <td>Column added</td>
         *         <td>Only regular columns can be added. This method
         *             will not find this added column in the
         *             base schema primary key, so it will
         *             correctly return REGULAR.</td>
         *     </tr>
         *     <tr>
         *         <td>CDC log schema newer than base schema</td>
         *         <td>Column removed</td>
         *         <td>This method will never be called for that removed column,
         *             as the column name was not present in the CDC log schema (caller).</td>
         *     </tr>
         *     <tr>
         *         <td>CDC log schema newer than base schema</td>
         *         <td>Column renamed</td>
         *         <td>The same logic as in "Column renamed" when
         *             the base schema is newer than CDC log schema.
         *             The reasoning is a bit symmetrical
         *             (ck_new in CDC log schema rather than
         *             base table schema).</td>
         *     </tr>
         *     <tr>
         *         <td>CDC log schema newer than base schema</td>
         *         <td>Column data type changed</td>
         *         <td>The same logic as in "Column data type changed" when
         *             the base schema is newer than CDC log schema.</td>
         *     </tr>
         * </table>
         *
         * @param columnName the column name to get the column kind for.
         * @param cdcColumnDefinitions the column definitions for the CDC log table.
         * @return the column kind in the base table.
         */
        public ChangeSchema.ColumnKind getColumnKind(String columnName, ColumnDefinitions cdcColumnDefinitions) {
            if (partitionColumns.contains(columnName)) {
                return ChangeSchema.ColumnKind.PARTITION_KEY;
            } else if (clusteringColumns.contains(columnName)) {
                return ChangeSchema.ColumnKind.CLUSTERING_KEY;
            } else {
                // Detect if a clustering columns
                // changed between base table schema
                // and CDC log schema:
                Set<String> cdcColumnNames = cdcColumnDefinitions.asList().stream()
                        .map(ColumnDefinitions.Definition::getName).collect(Collectors.toSet());
                if (!cdcColumnNames.containsAll(clusteringColumns)) {
                    // There was a clustering column rename.

                    // By now, we know that the queried column
                    // is either a REGULAR column or the old
                    // CLUSTERING_KEY (the partition key
                    // cannot be changed and would
                    // have been handled by previous ifs). If the column
                    // is present in both base table schema
                    // and CDC log schema, it is not the
                    // renamed clustering column.
                    if (cdcColumnNames.contains(columnName) && regularColumns.contains(columnName)) {
                        return ChangeSchema.ColumnKind.REGULAR;
                    } else {
                        return ChangeSchema.ColumnKind.CLUSTERING_KEY;
                    }
                }

                return ChangeSchema.ColumnKind.REGULAR;
            }
        }

        /**
         * Returns whether the column is a <code>LIST</code>
         * in the base table, as of the time of CDC schema.
         * <p>
         * This method should be called only to discern
         * between {@code LIST<X>} and {@code MAP<TIMEUUID, X>}
         * This method should only be called for that single purpose
         * and if you are certain that the base table
         * had a column with either of these data types
         * as of CDC log schema time.
         * <p>
         * Correctness of this implementation under possible
         * inconsistencies with the base table schema:
         * <table border="1">
         *     <caption>Correctness under possible base table schema inconsistencies</caption>
         *     <tr>
         *         <th>Inconsistency type</th>
         *         <th>Schema change type</th>
         *         <th>Commentary</th>
         *     </tr>
         *     <tr>
         *         <td>Base schema newer than CDC log schema</td>
         *         <td>Column added</td>
         *         <td>This method will never be called for that added column,
         *             as the column name was not present in the CDC log schema (caller).</td>
         *     </tr>
         *     <tr>
         *         <td>Base schema newer than CDC log schema</td>
         *         <td>Column removed</td>
         *         <td>If we query the deleted column,
         *             it will be missing from the base table
         *             schema. Therefore, we cannot determine
         *             whether it was a <code>LIST</code>
         *             as of the CDC log schema time.
         *             Throw an {@link UnresolvableSchemaInconsistencyException}
         *             in such case.</td>
         *     </tr>
         *     <tr>
         *         <td>Base schema newer than CDC log schema</td>
         *         <td>Column renamed</td>
         *         <td>Only clustering key columns can be renamed. This method
         *             is called only if a type in the CDC log
         *             is {@code MAP<TIMEUUID, X>} and there is
         *             a <code>cdc$deleted_elements_</code> column
         *             (to discern between {@code LIST<X>}
         *             and {@code MAP<TIMEUUID, X>} in the base
         *             table). There won't be a <code>cdc$deleted_elements_</code>
         *             column for a clustering key, therefore
         *             this method will never get called in such a case.</td>
         *     </tr>
         *     <tr>
         *         <td>Base schema newer than CDC log schema</td>
         *         <td>Column data type changed</td>
         *         <td>A column with <code>LIST</code> type
         *             can only be changed to <code>BLOB</code>.
         *             The base table schema will contain
         *             the <code>BLOB</code> type in that case.
         *             We cannot determine if the column
         *             was a <code>LIST</code> at a time
         *             of CDC log schema. Throw an
         *             {@link UnresolvableSchemaInconsistencyException}
         *             in such case. Similarly, if a
         *             column was of {@code MAP<TIMEUUID, X>} type
         *             it can only be changed to <code>BLOB</code>
         *             (therefore two possible answers).</td>
         *     </tr>
         *     <tr>
         *         <td>CDC log schema newer than base schema</td>
         *         <td>Column added</td>
         *         <td>Since a column was added, this column
         *             will not be present in the base schema.
         *             Throw an {@link UnresolvableSchemaInconsistencyException}
         *             in such case, as we will not be able to
         *             determine the data type of the added column.</td>
         *     </tr>
         *     <tr>
         *         <td>CDC log schema newer than base schema</td>
         *         <td>Column removed</td>
         *         <td>This method will never be called for that removed column,
         *             as the column name was not present in the CDC log schema (caller).</td>
         *     </tr>
         *     <tr>
         *         <td>CDC log schema newer than base schema</td>
         *         <td>Column renamed</td>
         *         <td>The same logic as in "Column renamed" when
         *             the base schema is newer than CDC log schema.</td>
         *     </tr>
         *     <tr>
         *         <td>CDC log schema newer than base schema</td>
         *         <td>Column data type changed</td>
         *         <td>{@code LIST<X>} or {@code MAP<TIMEUUID, X>} can
         *             only be changed to {@code BLOB} data type.
         *             In this case, the CDC log schema will have
         *             this column with {@code BLOB} and this
         *             method will not get called.</td>
         *     </tr>
         * </table>
         *
         * @param columnName the column name to check if it is a <code>LIST</code>
         * @return whether the column is a <code>LIST</code> in the base table.
         * @throws UnresolvableSchemaInconsistencyException if whether the column
         *                                                is a <code>LIST</code> cannot
         *                                                be deduced.
         */
        public boolean isList(String columnName) throws UnresolvableSchemaInconsistencyException {
            ColumnMetadata baseColumnMetadata = baseTableSchema.getColumn(columnName);
            if (baseColumnMetadata == null) {
                throw new UnresolvableSchemaInconsistencyException(
                        String.format("Column '%s' in base table metadata is missing, " +
                                "making it impossible to deduce whether it was a LIST at " +
                                "a time of CDC log schema.", columnName));
            }

            boolean isList = baseColumnMetadata.getType().getName() == DataType.Name.LIST;
            boolean isMapTimeuuid = baseColumnMetadata.getType().getName() == DataType.Name.MAP
                    && baseColumnMetadata.getType().getTypeArguments().get(0).getName() == DataType.Name.TIMEUUID;

            if (!isList && !isMapTimeuuid) {
                throw new UnresolvableSchemaInconsistencyException(
                        String.format("The type of column '%s' in base table metadata is %s, " +
                                "making it impossible to deduce whether it was a LIST at " +
                                "a time of CDC log schema.", columnName, baseColumnMetadata.getType().getName()));
            }

            return isList;
        }
    }

    /**
     * Thrown when the {@link Driver3SchemaFactory} cannot resolve
     * a schema inconsistency between the base table schema
     * and CDC log schema. A "schema inconsistency" term that is used
     * here refers to a situation when a concurrent
     * <code>ALTER TABLE</code> happened and
     * one of the schemas (base table schema or CDC table schema)
     * is old (before <code>ALTER TABLE</code>), while
     * the other one is new (after <code>ALTER TABLE</code>).
     */
    public static class UnresolvableSchemaInconsistencyException extends Exception {
        private UnresolvableSchemaInconsistencyException(String message) {
            super(message);
        }
    }

    private static ChangeSchema.DataType translateCdcColumnDataType(DataType driverType) {
        // This method only translates driverType
        // from CDC table columns. All CDC table columns (with non-native types)
        // are frozen, even though the driverType can lack this information
        // (when driverType is from a PreparedStatement metadata).

        List<DataType> driverTypeArguments = driverType.getTypeArguments();
        Preconditions.checkNotNull(driverTypeArguments);
        ImmutableList<ChangeSchema.DataType> typeArguments = driverTypeArguments.stream()
                .map(Driver3SchemaFactory::translateCdcColumnDataType)
                .collect(Collectors.collectingAndThen(Collectors.toList(), ImmutableList::copyOf));

        switch (driverType.getName()) {
            case ASCII:
                return new ChangeSchema.DataType(ChangeSchema.CqlType.ASCII);
            case BIGINT:
                return new ChangeSchema.DataType(ChangeSchema.CqlType.BIGINT);
            case BLOB:
                return new ChangeSchema.DataType(ChangeSchema.CqlType.BLOB);
            case BOOLEAN:
                return new ChangeSchema.DataType(ChangeSchema.CqlType.BOOLEAN);
            case COUNTER:
                return new ChangeSchema.DataType(ChangeSchema.CqlType.COUNTER);
            case DECIMAL:
                return new ChangeSchema.DataType(ChangeSchema.CqlType.DECIMAL);
            case DOUBLE:
                return new ChangeSchema.DataType(ChangeSchema.CqlType.DOUBLE);
            case FLOAT:
                return new ChangeSchema.DataType(ChangeSchema.CqlType.FLOAT);
            case INT:
                return new ChangeSchema.DataType(ChangeSchema.CqlType.INT);
            case TEXT:
                return new ChangeSchema.DataType(ChangeSchema.CqlType.TEXT);
            case TIMESTAMP:
                return new ChangeSchema.DataType(ChangeSchema.CqlType.TIMESTAMP);
            case UUID:
                return new ChangeSchema.DataType(ChangeSchema.CqlType.UUID);
            case VARCHAR:
                return new ChangeSchema.DataType(ChangeSchema.CqlType.VARCHAR);
            case VARINT:
                return new ChangeSchema.DataType(ChangeSchema.CqlType.VARINT);
            case TIMEUUID:
                return new ChangeSchema.DataType(ChangeSchema.CqlType.TIMEUUID);
            case INET:
                return new ChangeSchema.DataType(ChangeSchema.CqlType.INET);
            case DATE:
                return new ChangeSchema.DataType(ChangeSchema.CqlType.DATE);
            case TIME:
                return new ChangeSchema.DataType(ChangeSchema.CqlType.TIME);
            case SMALLINT:
                return new ChangeSchema.DataType(ChangeSchema.CqlType.SMALLINT);
            case TINYINT:
                return new ChangeSchema.DataType(ChangeSchema.CqlType.TINYINT);
            case DURATION:
                return new ChangeSchema.DataType(ChangeSchema.CqlType.DURATION);
            case LIST:
                Preconditions.checkArgument(typeArguments.size() == 1);
                return new ChangeSchema.DataType(ChangeSchema.CqlType.LIST, typeArguments, true);
            case MAP:
                Preconditions.checkArgument(typeArguments.size() == 2);
                return new ChangeSchema.DataType(ChangeSchema.CqlType.MAP, typeArguments, true);
            case SET:
                Preconditions.checkArgument(typeArguments.size() == 1);
                return new ChangeSchema.DataType(ChangeSchema.CqlType.SET, typeArguments, true);
            case UDT:
                Preconditions.checkArgument(driverType instanceof UserType);
                UserType userType = (UserType) driverType;
                ImmutableMap.Builder<String, ChangeSchema.DataType> fieldsBuilder = new ImmutableMap.Builder<>();
                for (UserType.Field f : userType) {
                    fieldsBuilder.put(f.getName(), translateCdcColumnDataType(f.getType()));
                }
                ChangeSchema.DataType.UdtType udtType = new ChangeSchema.DataType.UdtType(fieldsBuilder.build(), userType.getKeyspace(), userType.getTypeName());
                return new ChangeSchema.DataType(ChangeSchema.CqlType.UDT, udtType, true);
            case TUPLE:
                Preconditions.checkArgument(driverType instanceof TupleType);
                TupleType tupleType = (TupleType) driverType;
                typeArguments = tupleType.getComponentTypes().stream().map(Driver3SchemaFactory::translateCdcColumnDataType)
                        .collect(Collectors.collectingAndThen(Collectors.toList(), ImmutableList::copyOf));
                Preconditions.checkArgument(!typeArguments.isEmpty());
                return new ChangeSchema.DataType(ChangeSchema.CqlType.TUPLE, typeArguments, true);
            default:
                throw new RuntimeException(String.format("Data type %s is currently not supported.", driverType.getName()));
        }
    }
}
