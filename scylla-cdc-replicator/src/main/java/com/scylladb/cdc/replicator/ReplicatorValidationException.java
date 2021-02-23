package com.scylladb.cdc.replicator;

public class ReplicatorValidationException extends Exception {
    public ReplicatorValidationException(String message) {
        super(message);
    }
}
