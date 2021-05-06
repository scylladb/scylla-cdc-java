package com.scylladb.cdc.model.worker.cql;

import java.nio.ByteBuffer;
import java.util.Set;

import com.scylladb.cdc.model.worker.ChangeSchema;

public interface Cell extends Field {
    ChangeSchema.ColumnDefinition getColumnDefinition();

    Set<Field> getDeletedElements();

    boolean hasDeletedElements();

    boolean isDeleted();
    
    /**
     * Returns the value of a binary representation of this cell as a <code>ByteBuffer</code>.
     * <p>
     * This method returns a binary representation of this cell as it was received
     * by the underlying Scylla driver. This representation may vary between different
     * versions of this library and <code>WorkerCQL</code> implementations.
     * <p>
     * This method can be called for any type of column, not only <code>BLOB</code>.
     * If you want to read the value of a <code>BLOB</code> column, please use the
     * {@link Field#getBytes() getBytes} method.
     * <p>
     * If a value of this cell is <code>NULL</code>, this method returns <code>null</code>.
     *
     * @return the value of a binary representation of this cell. If the value of this cell
     *         is <code>NULL</code>, <code>null</code> is returned.
     */
    ByteBuffer getAsUnsafeBytes();
}
