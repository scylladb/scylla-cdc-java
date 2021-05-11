package com.scylladb.cdc.cql.driver3;

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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.scylladb.cdc.model.worker.ChangeSchema;

public class Driver3SchemaBuilder {
    private static final String SCYLLA_CDC_LOG_SUFFIX = "_scylla_cdc_log";

    private Row row;
    private String keyspace;
    private String tableName;
    private String baseTableName;
    private Metadata metadata;
    private TableMetadata baseTableMetadata;

    private Set<String> baseTablePartitionKeyColumnNames;
    private Set<String> baseTableClusteringKeyColumnNames;

    public Driver3SchemaBuilder withRow(Row row) {
        this.row = Preconditions.checkNotNull(row);
        this.keyspace = row.getColumnDefinitions().getKeyspace(0);
        this.tableName = row.getColumnDefinitions().getTable(0);
        Preconditions.checkArgument(this.tableName.endsWith(SCYLLA_CDC_LOG_SUFFIX));
        this.baseTableName = tableName.substring(0, tableName.length() - SCYLLA_CDC_LOG_SUFFIX.length());
        return this;
    }

    public Driver3SchemaBuilder withClusterMetadata(Metadata metadata) {
        this.metadata = Preconditions.checkNotNull(metadata);
        return this;
    }

    public ChangeSchema build() {
        generatePrimaryKeyColumns();

        return new ChangeSchema(generateChangeSchemaColumns());
    }

    private void generatePrimaryKeyColumns() {
        Preconditions.checkNotNull(keyspace);
        Preconditions.checkNotNull(baseTableName);
        Preconditions.checkNotNull(metadata);

        /*
         * This is a potentially really dangerous operation. Rationale:
         * 1) Metadata (returned by session.getCluster().getMetadata()) might be stale.
         * 2) Even if Metadata was always up-to-date, we could have a race condition:
         *    a) session.getCluster().getMetadata()
         *    b) Do a query on CDC log (which returns other metadata)
         * If the metadata changed between call a) and b) there could be a discrepancy
         * between them. However, in this place we are only reading what
         * columns are in primary key, which cannot be changed. But, there could
         * be a situation when someone drops the table and quickly recreates it (with
         * different schema) and this operation took place between call a) and b).
         * In such case, this code is not correct!
         *
         * Ideally, the server should send the corresponding base table metadata together with the CDC log table metadata,
         * and we'll be able to drop this hack. See Scylla issue #7824.
         */
        TableMetadata baseTableMetadata = baseTableMetadata();
        baseTablePartitionKeyColumnNames = baseTableMetadata.getPartitionKey().stream().map(ColumnMetadata::getName).collect(Collectors.toSet());
        baseTableClusteringKeyColumnNames = baseTableMetadata.getClusteringColumns().stream().map(ColumnMetadata::getName).collect(Collectors.toSet());
    }

    private ImmutableList<ChangeSchema.ColumnDefinition> generateChangeSchemaColumns() {
        Preconditions.checkNotNull(row);
        Preconditions.checkNotNull(baseTablePartitionKeyColumnNames);
        Preconditions.checkNotNull(baseTableClusteringKeyColumnNames);

        List<ColumnDefinitions.Definition> driverColumnDefinitions = row.getColumnDefinitions().asList();
        ImmutableList.Builder<ChangeSchema.ColumnDefinition> builder = ImmutableList.builder();
        int i = 0; 
        for (ColumnDefinitions.Definition c : driverColumnDefinitions) {
            builder.add(translateColumnDefinition(c, i++));
        }
        return builder.build();
    }

    private ChangeSchema.ColumnDefinition translateColumnDefinition(ColumnDefinitions.Definition driverDefinition, int index) {
        String columnName = driverDefinition.getName();
        ChangeSchema.DataType dataType = translateColumnDataType(driverDefinition.getType());
        ChangeSchema.DataType baseType = null;
        ChangeSchema.ColumnType baseTableColumnType = ChangeSchema.ColumnType.REGULAR;
        if (baseTablePartitionKeyColumnNames.contains(columnName)) {
            baseTableColumnType = ChangeSchema.ColumnType.PARTITION_KEY;
        } else if (baseTableClusteringKeyColumnNames.contains(columnName)) {
            baseTableColumnType = ChangeSchema.ColumnType.CLUSTERING_KEY;
        } else {
            /* Fill in the baseType field. One of its uses is to determine
             * if the base column is a non-frozen list. Below is commentary
             * why it's safe to use this value:
             *
             * See comment in `generatePrimaryKeyColumns` about us racing with schema changes.
             * Luckily, it's not possible to change the type of a column that is a non-frozen list,
             * and after removing such a column, it's not possible to re-add a column of the same name
             * (true for all non-frozen types; see https://docs.scylladb.com/getting-started/ddl/#alter-table-statement).
             * Also we're considering removing altering column types altogether (https://github.com/scylladb/scylla/issues/4550).
             *
             * Still, it's possible to drop a table and create a new with different schema, so the base table metadata
             * could potentially come from the old schema. Also, it's possible to delete or rename a column.
             * For deleted columns, stale base table metadata is not a problem, but it would be a problem if OUR metadata is stale
             * and base table metadata is newer (so we have a column, but the base doesn't) - can this happen?
             *
             */
            TableMetadata baseTableMetadata = baseTableMetadata();
            // TODO: this sets it only for value columns (and not e.g. for `cdc$deleted_` columns), but maybe it's enough?
            ColumnMetadata baseColumnMetadata = baseTableMetadata.getColumn(columnName);
            if (baseColumnMetadata != null) {
                baseType = translateColumnDataType(baseColumnMetadata.getType());
            }
        }
        return new ChangeSchema.ColumnDefinition(columnName, index, dataType, baseType, baseTableColumnType);
    }

    private ChangeSchema.DataType translateColumnDataType(DataType driverType) {
        List<DataType> driverTypeArguments = driverType.getTypeArguments();
        Preconditions.checkNotNull(driverTypeArguments);
        ImmutableList<ChangeSchema.DataType> typeArguments = driverTypeArguments.stream()
                .map(this::translateColumnDataType)
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
                return new ChangeSchema.DataType(ChangeSchema.CqlType.LIST, typeArguments, driverType.isFrozen());
            case MAP:
                Preconditions.checkArgument(typeArguments.size() == 2);
                return new ChangeSchema.DataType(ChangeSchema.CqlType.MAP, typeArguments, driverType.isFrozen());
            case SET:
                Preconditions.checkArgument(typeArguments.size() == 1);
                return new ChangeSchema.DataType(ChangeSchema.CqlType.SET, typeArguments, driverType.isFrozen());
            case UDT:
                Preconditions.checkArgument(driverType instanceof UserType);
                UserType userType = (UserType) driverType;
                ImmutableMap.Builder<String, ChangeSchema.DataType> fieldsBuilder = new ImmutableMap.Builder<>();
                for (UserType.Field f : userType) {
                    fieldsBuilder.put(f.getName(), translateColumnDataType(f.getType()));
                }
                ChangeSchema.DataType.UdtType udtType = new ChangeSchema.DataType.UdtType(fieldsBuilder.build(), userType.getKeyspace(), userType.getTypeName());
                return new ChangeSchema.DataType(ChangeSchema.CqlType.UDT, udtType, driverType.isFrozen());
            case TUPLE:
                Preconditions.checkArgument(driverType instanceof TupleType);
                TupleType tupleType = (TupleType) driverType;
                typeArguments = tupleType.getComponentTypes().stream().map(this::translateColumnDataType)
                        .collect(Collectors.collectingAndThen(Collectors.toList(), ImmutableList::copyOf));
                Preconditions.checkArgument(!typeArguments.isEmpty());
                return new ChangeSchema.DataType(ChangeSchema.CqlType.TUPLE, typeArguments, driverType.isFrozen());
            default:
                throw new RuntimeException(String.format("Data type %s is currently not supported.", driverType.getName()));
        }
    }

    private TableMetadata baseTableMetadata() {
        if (baseTableMetadata == null) {
            Preconditions.checkNotNull(metadata);
            baseTableMetadata = metadata.getKeyspace(keyspace).getTable(baseTableName);
        }

        return baseTableMetadata;
    }
}
