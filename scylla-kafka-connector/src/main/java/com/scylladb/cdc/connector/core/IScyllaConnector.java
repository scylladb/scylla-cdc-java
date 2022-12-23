package com.scylladb.cdc.connector.core;

import java.io.IOException;

/**
 * @author buntykumar
 * @version 1.0
 */

public interface IScyllaConnector {
    void initialize() throws IOException;
    void start() throws InterruptedException;
}
