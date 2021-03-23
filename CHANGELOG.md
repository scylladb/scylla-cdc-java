# Changelog

## 1.1.0 - 23 March 2021

### Breaking changes
- **Connection creation.** In the previous version of the library, when creating a `CDCConsumer` instance (or lower-level `MasterCQL` or `WorkerCQL` objects) it expected you to pass `Session` object created with Scylla Java Driver 3. In this release, creating a `Session` is now done by the scylla-cdc-java library itself. You should fill connection information (such as contact IP, username/password) using `CDCConsumer.builder()` object. You can learn more about this new API at ["Getting started" section](https://github.com/scylladb/scylla-cdc-java#getting-started) or in ["Do it yourself!" Printer documentation](https://github.com/scylladb/scylla-cdc-java/tree/master/scylla-cdc-printer#do-it-yourself).

### Major changes
- **Shading of Scylla Java Driver.** Java Driver package is now shaded in the library, allowing you to use our library alongside older versions of Scylla Java Driver, without a fear of incompatibility. Internally, all driver-specific code was moved to scylla-cdc-driver3 module.
- **Support for Scylla 4.4+.** In Scylla 4.4 [a new format of CDC generations](https://docs.scylladb.com/using-scylla/cdc/cdc-stream-generations/) was introduced (new `cdc_streams_descriptions_v2` and `cdc_generation_timestamps` tables), more efficient for larger clusters. The library now transparently supports this feature, while maintaining backwards comptability with older Scylla versions.
- **New configurable parameters.** You can now configure window query time and confidence window time (`withConfidenceWindowSizeMs()` and `withQueryTimeWindowSizeMs()`). Reducing the value of those parameters (by default both are 30 seconds), will allow you to see CDC changes with lower latency (the maximum latency is `confidenceWindowSizeMs + queryTimeWindowSizeMs`). It is necessary for the library to avoid reading too fresh data from the CDC log due to the eventual consistency of Scylla. `confidenceWindowSizeMs` is an "artificial" delay the library adds to mitigate this issue. You should *not* set it to a value smaller than `write_request_timeout_in_ms` (defined in your `scylla.yaml` file). The library reads changes in `queryTimeWindowSizeMs` periods. Lowering this value will reduce the latency, at a cost of larger number of queries.

### Fixes
- **Reduce reading unneccessary periods of time.** The library now checks the TTL configured on your CDC tables and skips directly to last point of time, where there could still be a CDC log change.
- **Fix window query size slightly too long.** Fix queries to the CDC log being 1ms too long, which could result in duplicate CDC changes.
- **Update the version of Scylla Java Driver 3.x.** This version of driver includes performance optimizations of reading CDC log tables (using CDC partitioner).
- **Robustness fixes.** Implemented retrying when discovering new CDC generations. Implemented exponential back-off with jitter when reading CDC log.
- Add `toString()` formatting of `CqlDuration`.
- In case of a failure in the middle of paginated query of CDC log table, the library now resumes from the last read position (instead of duplicate changes when it retried the query from the beginning).
- Fixed a problem in `CDCConsumer`, where it sometimes could not gracefully finish.
- Logging improvements. The library logs are now less spammy or moved to lower logging levels.

## 1.0.0 - 23 December 2020
First release 1.0.0 of scylla-cdc-java.

scylla-cdc-java is a library that makes it easy to develop Java applications consuming the Scylla CDC log. The library automatically and transparently handles errors and topology changes of the underlying Scylla cluster. It provides a simple API for reading the CDC log, as well as examples and ready-made tools, such as replicator.
