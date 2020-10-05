package com.scylladb.cdc.model.worker;

public interface Change {

    ChangeId getId();

    ChangeSchema getSchema();

    int getInt(String columnName);

}
