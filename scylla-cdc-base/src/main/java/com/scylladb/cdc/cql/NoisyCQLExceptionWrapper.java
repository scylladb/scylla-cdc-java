package com.scylladb.cdc.cql;

/**
 * Wrapper for exceptions upon which it should be acceptable to repeatedly retry with exponential retries.
 * Generally it is not advisable to use since it may result in suppressing an important error.
 * Only useful when number of exceptions is the only issue and application continues to work.
 * What gets wrapped is controlled by specific WorkerCQL implementations.
 */
public class NoisyCQLExceptionWrapper extends Exception {
  public NoisyCQLExceptionWrapper(Throwable cause) {
    super(cause);
  }
}
