package com.scylladb.cdc.connector.kafka;

/**
 * @author buntykumar
 * @version 1.0
 */
public interface IConnector {
    void init();
    Object getConnector();
}
