package com.scylladb.cdc.cql.driver3;

import static com.datastax.driver.core.Metadata.quoteIfNecessary;

import com.datastax.driver.core.Row;
import com.google.common.base.Preconditions;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.worker.ChangeTime;
import com.scylladb.cdc.model.worker.cql.Cell;
import com.scylladb.cdc.model.worker.cql.Field;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.ChangeId;
import com.scylladb.cdc.model.worker.ChangeSchema;

import java.text.DateFormat;

public final class Driver3RawChange implements RawChange {
    private final Row row;
    private final ChangeSchema schema;

    public Driver3RawChange(Row row, ChangeSchema schema) {
        this.row = Preconditions.checkNotNull(row);
        this.schema = Preconditions.checkNotNull(schema);
    }

    @Override
    public ChangeId getId() {
        return new ChangeId(new StreamId(row.getBytes(quoteIfNecessary("cdc$stream_id"))),
                new ChangeTime(row.getUUID(quoteIfNecessary("cdc$time"))));
    }

    @Override
    public ChangeSchema getSchema() {
        return schema;
    }

    @Override
    public Object getAsObject(String columnName) {
        if (row.isNull(columnName)) {
            return null;
        } else {
            // TODO - check if quoteIfNecessary is needed here in getObject()
            ChangeSchema.ColumnDefinition columnDefinition = schema.getColumnDefinition(columnName);
            return Driver3ToLibraryTranslator.translate(row.getObject(columnName), columnDefinition.getCdcLogDataType());
        }
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append("Driver3RawChange(");
        boolean first = true;
        for (ChangeSchema.ColumnDefinition columnDefinition : getSchema().getAllColumnDefinitions()) {
            if (!first) {
                result.append(",");
            } else {
                first = false;
            }

            String name = columnDefinition.getColumnName();
            Cell cell = getCell(name);
            result.append(name).append("=").append(cell);
        }
        result.append(")");
        return result.toString();
    }
}
