package com.scylladb.cdc.cql.driver3;

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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class Driver3SchemaBuilder {
    private static final String SCYLLA_CDC_LOG_SUFFIX = "_scylla_cdc_log";

    private Row row;
    private String keyspace;
    private String tableName;
    private String baseTableName;
    private Metadata metadata;

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
         */
        TableMetadata baseTableMetadata = metadata.getKeyspace(keyspace).getTable(baseTableName);
        baseTablePartitionKeyColumnNames = baseTableMetadata.getPartitionKey().stream().map(ColumnMetadata::getName).collect(Collectors.toSet());
        baseTableClusteringKeyColumnNames = baseTableMetadata.getClusteringColumns().stream().map(ColumnMetadata::getName).collect(Collectors.toSet());
    }

    private ImmutableList<ChangeSchema.ColumnDefinition> generateChangeSchemaColumns() {
        Preconditions.checkNotNull(row);
        Preconditions.checkNotNull(baseTablePartitionKeyColumnNames);
        Preconditions.checkNotNull(baseTableClusteringKeyColumnNames);

        List<ColumnDefinitions.Definition> driverColumnDefinitions = row.getColumnDefinitions().asList();
        ImmutableList.Builder<ChangeSchema.ColumnDefinition> builder = ImmutableList.builder();
        driverColumnDefinitions.stream().map(this::translateColumnDefinition).forEach(builder::add);
        return builder.build();
    }

    private ChangeSchema.ColumnDefinition translateColumnDefinition(ColumnDefinitions.Definition driverDefinition) {
        String columnName = driverDefinition.getName();
        ChangeSchema.DataType dataType = translateColumnDataType(driverDefinition.getType());
        ChangeSchema.ColumnType baseTableColumnType = ChangeSchema.ColumnType.REGULAR;
        if (baseTablePartitionKeyColumnNames.contains(columnName)) {
            baseTableColumnType = ChangeSchema.ColumnType.PARTITION_KEY;
        } else if (baseTableClusteringKeyColumnNames.contains(columnName)) {
            baseTableColumnType = ChangeSchema.ColumnType.CLUSTERING_KEY;
        }
        return new ChangeSchema.ColumnDefinition(columnName, dataType, baseTableColumnType);
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
                return new ChangeSchema.DataType(ChangeSchema.CqlType.LIST, typeArguments);
            case MAP:
                Preconditions.checkArgument(typeArguments.size() == 2);
                return new ChangeSchema.DataType(ChangeSchema.CqlType.MAP, typeArguments);
            case SET:
                Preconditions.checkArgument(typeArguments.size() == 1);
                return new ChangeSchema.DataType(ChangeSchema.CqlType.SET, typeArguments);
            case UDT:
                Preconditions.checkArgument(driverType instanceof UserType);
                UserType userType = (UserType) driverType;
                ImmutableMap.Builder<String, ChangeSchema.DataType> fieldsBuilder = new ImmutableMap.Builder<>();
                for (UserType.Field f : userType) {
                    fieldsBuilder.put(f.getName(), translateColumnDataType(f.getType()));
                }
                ChangeSchema.DataType.UdtType udtType = new ChangeSchema.DataType.UdtType(fieldsBuilder.build(), userType.getKeyspace(), userType.getTypeName());
                return new ChangeSchema.DataType(ChangeSchema.CqlType.UDT, udtType);
            case TUPLE:
                Preconditions.checkArgument(driverType instanceof TupleType);
                TupleType tupleType = (TupleType) driverType;
                typeArguments = tupleType.getComponentTypes().stream().map(this::translateColumnDataType)
                        .collect(Collectors.collectingAndThen(Collectors.toList(), ImmutableList::copyOf));
                Preconditions.checkArgument(!typeArguments.isEmpty());
                return new ChangeSchema.DataType(ChangeSchema.CqlType.TUPLE, typeArguments);
            default:
                throw new RuntimeException(String.format("Data type %s is currently not supported.", driverType.getName()));
        }
    }
}
