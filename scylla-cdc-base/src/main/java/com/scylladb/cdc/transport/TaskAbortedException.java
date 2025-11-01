package com.scylladb.cdc.transport;

public class TaskAbortedException extends RuntimeException {
    public TaskAbortedException(String message) {
        super(message);
    }
}
