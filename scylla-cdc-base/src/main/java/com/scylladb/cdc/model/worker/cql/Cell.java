package com.scylladb.cdc.model.worker.cql;

import java.nio.ByteBuffer;
import java.util.Set;

import com.scylladb.cdc.model.worker.ChangeSchema;

public interface Cell extends Field {
    ChangeSchema.ColumnDefinition getColumnDefinition();

    Set<Field> getDeletedElements();

    boolean hasDeletedElements();

    boolean isDeleted();
    
    ByteBuffer getUnsafeBytes();
}
