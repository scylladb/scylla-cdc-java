package com.scylladb.cdc.model.worker;

import com.datastax.driver.core.Row;

import java.util.Map;
import java.util.UUID;

/*
 * Represents a single CDC log row,
 * without any post-processing.
 */
public interface RawChange {

    ChangeId getId();

    ChangeSchema getSchema();

    /*
     * Gets the value of column as Java Object.
     */
    Object getAsObject(String columnName);

    Integer getInt(String columnName);

    Byte getByte(String columnName);

    Boolean getBoolean(String columnName);

    Map getMap(String columnName);

    /*
     * What follows are temporary methods
     * used for porting the replicator
     * from old library to new library.
     *
     * Those methods should be removed
     * after the porting process is done.
     */

    @Deprecated
    UUID TEMPORARY_PORTING_getTime();

    @Deprecated
    Integer TEMPORARY_PORTING_getTTL();

    @Deprecated
    boolean TEMPORARY_PORTING_isDeleted(String name);

    @Deprecated
    Row TEMPORARY_PORTING_row();

    @Deprecated
    byte TEMPORARY_PORTING_getOperation();
}
