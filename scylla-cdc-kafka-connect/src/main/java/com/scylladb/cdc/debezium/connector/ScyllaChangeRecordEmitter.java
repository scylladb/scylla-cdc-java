package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.ChangeSchema;
import io.debezium.data.Envelope;
import io.debezium.pipeline.AbstractChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.util.Clock;
import org.apache.kafka.connect.data.Struct;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class ScyllaChangeRecordEmitter extends AbstractChangeRecordEmitter<ScyllaCollectionSchema> {

    private final RawChange change;
    private final ScyllaSchema schema;

    public ScyllaChangeRecordEmitter(RawChange change, OffsetContext offsetContext, ScyllaSchema schema, Clock clock) {
        super(offsetContext, clock);
        this.change = change;
        this.schema = schema;
    }

    public RawChange getChange() {
        return change;
    }

    public ScyllaSchema getSchema() {
        return schema;
    }

    @Override
    protected Envelope.Operation getOperation() {
        byte cdcOperation = this.change.getCell("cdc$operation").getByte();
        if (cdcOperation == 2) {
            return Envelope.Operation.CREATE;
        } else if (cdcOperation == 1) {
            return Envelope.Operation.UPDATE;
        } else if (cdcOperation == 3) {
            return Envelope.Operation.DELETE;
        } else {
            throw new RuntimeException(String.format("Unsupported operation type: %d.", cdcOperation));
        }
    }

    @Override
    protected void emitReadRecord(Receiver receiver, ScyllaCollectionSchema scyllaCollectionSchema) throws InterruptedException {
        throw new NotImplementedException();
    }

    @Override
    protected void emitCreateRecord(Receiver receiver, ScyllaCollectionSchema scyllaCollectionSchema) throws InterruptedException {
        scyllaCollectionSchema = this.schema.updateChangeSchema(scyllaCollectionSchema.id(), change.getSchema());

        Struct keyStruct = new Struct(scyllaCollectionSchema.keySchema());
        Struct afterStruct = new Struct(scyllaCollectionSchema.afterSchema());
        fillStructWithChange(scyllaCollectionSchema, keyStruct, afterStruct, change);

        Struct envelope = scyllaCollectionSchema.getEnvelopeSchema().create(afterStruct, getOffset().getSourceInfo(), getClock().currentTimeAsInstant());

        receiver.changeRecord(scyllaCollectionSchema, getOperation(), keyStruct, envelope, getOffset(), null);
    }

    @Override
    protected void emitUpdateRecord(Receiver receiver, ScyllaCollectionSchema scyllaCollectionSchema) throws InterruptedException {
        scyllaCollectionSchema = this.schema.updateChangeSchema(scyllaCollectionSchema.id(), change.getSchema());

        Struct keyStruct = new Struct(scyllaCollectionSchema.keySchema());
        Struct afterStruct = new Struct(scyllaCollectionSchema.afterSchema());
        fillStructWithChange(scyllaCollectionSchema, keyStruct, afterStruct, change);

        Struct envelope = scyllaCollectionSchema.getEnvelopeSchema().update(null, afterStruct, getOffset().getSourceInfo(), getClock().currentTimeAsInstant());

        receiver.changeRecord(scyllaCollectionSchema, getOperation(), keyStruct, envelope, getOffset(), null);
    }

    @Override
    protected void emitDeleteRecord(Receiver receiver, ScyllaCollectionSchema scyllaCollectionSchema) throws InterruptedException {
        scyllaCollectionSchema = this.schema.updateChangeSchema(scyllaCollectionSchema.id(), change.getSchema());

        Struct keyStruct = new Struct(scyllaCollectionSchema.keySchema());
        Struct beforeStruct = new Struct(scyllaCollectionSchema.beforeSchema());
        fillStructWithChange(scyllaCollectionSchema, keyStruct, beforeStruct, change);

        Struct envelope = scyllaCollectionSchema.getEnvelopeSchema().delete(beforeStruct, getOffset().getSourceInfo(), getClock().currentTimeAsInstant());

        receiver.changeRecord(scyllaCollectionSchema, getOperation(), keyStruct, envelope, getOffset(), null);
    }

    private void fillStructWithChange(ScyllaCollectionSchema schema, Struct keyStruct, Struct valueStruct, RawChange change) {
        for (ChangeSchema.ColumnDefinition cdef : change.getSchema().getNonCdcColumnDefinitions()) {
            ChangeSchema.DataType type = cdef.getCdcLogDataType();
            Object value = change.getCell(cdef.getColumnName()).getInt();

            if (cdef.getBaseTableColumnType() == ChangeSchema.ColumnType.PARTITION_KEY || cdef.getBaseTableColumnType() == ChangeSchema.ColumnType.CLUSTERING_KEY) {
                valueStruct.put(cdef.getColumnName(), value);
                keyStruct.put(cdef.getColumnName(), value);
            } else {
                Boolean isDeleted = this.change.getCell("cdc$deleted_" + cdef.getColumnName()).getBoolean();
                if (value != null || (isDeleted != null && isDeleted)) {
                    Struct cell = new Struct(schema.cellSchema(cdef.getColumnName()));
                    cell.put(ScyllaSchema.CELL_VALUE, value);
                    valueStruct.put(cdef.getColumnName(), cell);
                }
            }
        }
    }
}
