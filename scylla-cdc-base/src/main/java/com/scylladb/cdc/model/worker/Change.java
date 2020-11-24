package com.scylladb.cdc.model.worker;

public interface Change {

    ChangeId getId();

    ChangeSchema getSchema();

    Integer getInt(String columnName);

    Byte getByte(String columnName);

}
