package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.worker.ChangeSchema;
import io.debezium.data.Envelope;
import io.debezium.pipeline.txmetadata.TransactionMonitor;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.schema.DatabaseSchema;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class ScyllaSchema implements DatabaseSchema<CollectionId> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScyllaSchema.class);
    public static final String CELL_VALUE = "value";

    private final Schema sourceSchema;
    private final SchemaNameAdjuster adjuster = SchemaNameAdjuster.create(LOGGER);
    private final Map<CollectionId, ScyllaCollectionSchema> dataCollectionSchemas = new HashMap<>();
    private final Map<CollectionId, ChangeSchema> changeSchemas = new HashMap<>();

    public ScyllaSchema(Schema sourceSchema) {
        this.sourceSchema = sourceSchema;
    }

    @Override
    public void close() {
    }

    @Override
    public DataCollectionSchema schemaFor(CollectionId collectionId) {
        return dataCollectionSchemas.computeIfAbsent(collectionId, this::computeDataCollectionSchema);
    }

    private ScyllaCollectionSchema computeDataCollectionSchema(CollectionId collectionId) {
        // TODO - refactor this code
        // TODO - support more data types

        ChangeSchema changeSchema = changeSchemas.get(collectionId);

        if (changeSchema == null) {
            return null;
        }

        Map<String, Schema> cellSchemas = new HashMap<>();
        for (ChangeSchema.ColumnDefinition cdef : changeSchema.getAllColumnDefinitions()) {
            if (cdef.getColumnName().startsWith("cdc$")) continue;
            if (cdef.getBaseTableColumnType() == ChangeSchema.ColumnType.PARTITION_KEY
                    || cdef.getBaseTableColumnType() == ChangeSchema.ColumnType.CLUSTERING_KEY) continue;

            Schema columnSchema = Schema.OPTIONAL_STRING_SCHEMA;
            if (cdef.getCdcLogDataType() == ChangeSchema.DataType.INT) {
                columnSchema = Schema.OPTIONAL_INT32_SCHEMA;
            }
            Schema cellSchema = SchemaBuilder.struct()
                    .field(CELL_VALUE, columnSchema).optional().build();
            cellSchemas.put(cdef.getColumnName(), cellSchema);
        }

        SchemaBuilder keySchemaBuilder = SchemaBuilder.struct()
                .name(adjuster.adjust(collectionId.getTableName().keyspace + "." + collectionId.getTableName().name + ".Key"));
        for (ChangeSchema.ColumnDefinition cdef : changeSchema.getAllColumnDefinitions()) {
            if (cdef.getColumnName().startsWith("cdc$")) continue;
            if (cdef.getBaseTableColumnType() != ChangeSchema.ColumnType.PARTITION_KEY
                    && cdef.getBaseTableColumnType() != ChangeSchema.ColumnType.CLUSTERING_KEY) continue;

            Schema columnSchema = Schema.OPTIONAL_STRING_SCHEMA;
            if (cdef.getCdcLogDataType() == ChangeSchema.DataType.INT) {
                columnSchema = Schema.OPTIONAL_INT32_SCHEMA;
            }
            keySchemaBuilder = keySchemaBuilder.field(cdef.getColumnName(), columnSchema);
        }

        final Schema keySchema = keySchemaBuilder.build();

        SchemaBuilder afterSchemaBuilder = SchemaBuilder.struct();
        for (ChangeSchema.ColumnDefinition cdef : changeSchema.getAllColumnDefinitions()) {
            if (cdef.getColumnName().startsWith("cdc$")) continue;
            if (cdef.getBaseTableColumnType() != ChangeSchema.ColumnType.PARTITION_KEY && cdef.getBaseTableColumnType() != ChangeSchema.ColumnType.CLUSTERING_KEY) {
                afterSchemaBuilder = afterSchemaBuilder.field(cdef.getColumnName(), cellSchemas.get(cdef.getColumnName()));
            } else {
                Schema columnSchema = Schema.OPTIONAL_STRING_SCHEMA;
                if (cdef.getCdcLogDataType() == ChangeSchema.DataType.INT) {
                    columnSchema = Schema.OPTIONAL_INT32_SCHEMA;
                }
                afterSchemaBuilder = afterSchemaBuilder.field(cdef.getColumnName(), columnSchema);
            }
        }
        Schema afterSchema = afterSchemaBuilder.optional().build();
        Schema beforeSchema = afterSchema;

        final Schema valueSchema = SchemaBuilder.struct()
                .name(adjuster.adjust(Envelope.schemaName(collectionId.getTableName().keyspace + "." + collectionId.getTableName().name)))
                .field(Envelope.FieldName.SOURCE, sourceSchema)
                .field(Envelope.FieldName.BEFORE, beforeSchema)
                .field(Envelope.FieldName.AFTER, afterSchema)
                .field(Envelope.FieldName.OPERATION, Schema.OPTIONAL_STRING_SCHEMA)
                .field(Envelope.FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
                .field(Envelope.FieldName.TRANSACTION, TransactionMonitor.TRANSACTION_BLOCK_SCHEMA)
                .build();

        final Envelope envelope = Envelope.fromSchema(valueSchema);

        return new ScyllaCollectionSchema(collectionId, keySchema, valueSchema, beforeSchema, afterSchema, cellSchemas, envelope);
    }

    public ScyllaCollectionSchema updateChangeSchema(CollectionId collectionId, ChangeSchema changeSchema) {
        changeSchemas.put(collectionId, changeSchema);
        dataCollectionSchemas.put(collectionId, computeDataCollectionSchema(collectionId));
        return dataCollectionSchemas.get(collectionId);
    }

    @Override
    public boolean tableInformationComplete() {
        return false;
    }
}
