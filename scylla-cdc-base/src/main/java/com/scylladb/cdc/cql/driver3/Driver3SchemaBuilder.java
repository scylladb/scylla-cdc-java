package com.scylladb.cdc.cql.driver3;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.scylladb.cdc.model.worker.ChangeSchema;

import java.util.List;
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

    public Driver3ChangeSchema build() {
        generatePrimaryKeyColumns();

        return new Driver3ChangeSchema(generateChangeSchemaColumns(), row);
    }

    private void generatePrimaryKeyColumns() {
        Preconditions.checkNotNull(keyspace);
        Preconditions.checkNotNull(baseTableName);
        Preconditions.checkNotNull(metadata);

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
        ChangeSchema.ColumnType columnType = ChangeSchema.ColumnType.REGULAR;
        if (baseTablePartitionKeyColumnNames.contains(columnName)) {
            columnType = ChangeSchema.ColumnType.PARTITION_KEY;
        } else if (baseTableClusteringKeyColumnNames.contains(columnName)) {
            columnType = ChangeSchema.ColumnType.CLUSTERING_KEY;
        }
        return new ChangeSchema.ColumnDefinition(columnName, dataType, columnType);
    }

    private ChangeSchema.DataType translateColumnDataType(DataType driverType) {
        switch (driverType.getName()) {
            case ASCII:
                return ChangeSchema.DataType.ASCII;
            case BIGINT:
                return ChangeSchema.DataType.BIGINT;
            case BLOB:
                return ChangeSchema.DataType.BLOB;
            case BOOLEAN:
                return ChangeSchema.DataType.BOOLEAN;
            case COUNTER:
                return ChangeSchema.DataType.COUNTER;
            case DECIMAL:
                return ChangeSchema.DataType.DECIMAL;
            case DOUBLE:
                return ChangeSchema.DataType.DOUBLE;
            case FLOAT:
                return ChangeSchema.DataType.FLOAT;
            case INT:
                return ChangeSchema.DataType.INT;
            case TEXT:
                return ChangeSchema.DataType.TEXT;
            case TIMESTAMP:
                return ChangeSchema.DataType.TIMESTAMP;
            case UUID:
                return ChangeSchema.DataType.UUID;
            case VARCHAR:
                return ChangeSchema.DataType.VARCHAR;
            case VARINT:
                return ChangeSchema.DataType.VARINT;
            case TIMEUUID:
                return ChangeSchema.DataType.TIMEUUID;
            case INET:
                return ChangeSchema.DataType.INET;
            case DATE:
                return ChangeSchema.DataType.DATE;
            case TIME:
                return ChangeSchema.DataType.TIME;
            case SMALLINT:
                return ChangeSchema.DataType.SMALLINT;
            case TINYINT:
                return ChangeSchema.DataType.TINYINT;
            case DURATION:
                return ChangeSchema.DataType.DURATION;
            case LIST:
                return ChangeSchema.DataType.LIST;
            case MAP:
                return ChangeSchema.DataType.MAP;
            case SET:
                return ChangeSchema.DataType.SET;
            case UDT:
                return ChangeSchema.DataType.UDT;
            case TUPLE:
                return ChangeSchema.DataType.TUPLE;
            default:
                throw new RuntimeException(String.format("Data type %s is currently not supported.", driverType.getName()));
        }
    }
}
