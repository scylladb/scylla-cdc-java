package com.scylladb.cdc.model.master;

import java.util.concurrent.ExecutionException;

/**
 * Abstraction for CDC metadata model.
 * Implementations: Generation-based (vnode) and Tablet-based.
 */
public interface CDCMetadataModel {

    /**
     * Perform one master step for this CDC metadata model.
     */
    void runMasterStep() throws InterruptedException, ExecutionException;

}
