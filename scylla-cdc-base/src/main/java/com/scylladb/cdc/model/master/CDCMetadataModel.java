package com.scylladb.cdc.model.master;

import java.util.concurrent.ExecutionException;

/**
 * Abstraction for CDC metadata model.
 * Implementations: Generation-based (vnode) and Tablet-based.
 */
public interface CDCMetadataModel {

    /**
     * Runs the master loop until an exception occurs or the thread is interrupted.
     */
    void runMasterLoop() throws InterruptedException, ExecutionException;

}
