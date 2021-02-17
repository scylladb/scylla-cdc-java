package com.scylladb.cdc.model;

public interface RetryBackoff {
    int getRetryBackoffTimeMs(int tryAttempt);
}
