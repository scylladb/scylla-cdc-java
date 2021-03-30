package com.scylladb.cdc.model.worker.cql;

import com.scylladb.cdc.model.worker.ChangeSchema;

public interface Cell extends Field {
    ChangeSchema.ColumnDefinition getColumnDefinition();
}
