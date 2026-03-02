# Catch-Up Optimization: TODO

## Critical Bugs

- [x] **`readTimeoutMs` long-to-int truncation in `Driver3WorkerCQL.fetchFirstChangeTime`:** `setReadTimeoutMillis((int) readTimeoutMs)` silently truncates `long` to `int`. When `probeTimeoutSeconds` is very large (e.g., `Long.MAX_VALUE` which the test `testCatchUpConfigurationProbeTimeoutMaxValue` explicitly allows), `TimeUnit.SECONDS.toMillis(probeTimeoutSeconds)` overflows `long`, then the cast to `int` produces an unpredictable value. Add validation in `CatchUpConfiguration` constructor or `CatchUpProber` to cap `probeTimeoutSeconds` to a reasonable upper bound (e.g., `Integer.MAX_VALUE / 1000`), and add an explicit overflow check or `Math.toIntExact` call at the cast site.

## Missing Validation

- [x] **`CatchUpConfiguration.Builder.setCatchUpWindow` rejects sub-second Duration but not `Duration.ofNanos(1_000_000_000)`:** The check `catchUpWindow.getNano() == 0` is correct for `Duration.ofMillis(500)` but passes for durations constructed from nanos that happen to be a whole second. This is fine, but the error message says "use a whole-second Duration or withCatchUpWindowSizeSeconds() instead" which is misleading if someone passes `Duration.ofSeconds(1, 500)` — the nano part would be 500, not 0. This is a minor ergonomic issue but the message could be clearer.

- [x] **No validation that catch-up window and confidence window are compatible:** If `catchUpWindowSizeSeconds` is less than `confidenceWindowSizeMs / 1000`, the cutoff would be very close to "now" and the confidence window could cause the worker to wait before reading the first window anyway, partially negating the catch-up benefit. Consider warning when `catchUpWindowSizeSeconds < confidenceWindowSizeMs / 1000`.

## Test Coverage Gaps

- [x] **No unit test for `CatchUpProber` in isolation:** All probe tests go through the full `Worker` / `WorkerThread` stack. A direct unit test of `CatchUpProber.apply()` with mock inputs would be faster, more deterministic, and would test edge cases more precisely (e.g., exact replacement indices, multiple candidates with mixed results).

- [x] **No test for concurrent `prepareProbe` race in `Driver3WorkerCQL`:** The `prepareProbe` method uses `putIfAbsent` on `probeStmts` but if two threads call `prepareProbe` concurrently for the same table, both will issue `session.prepareAsync()`. The second prepare is wasted work. Add a test or use `computeIfAbsent` with a `ConcurrentHashMap` pattern to avoid redundant prepares.

- [x] **Integration test `testCatchUpWithTinyWindowSkipsOldData` has weak assertion:** The test asserts `changeCounter.get() <= 10` which is trivially true even without catch-up (there are only 10 changes total). The test comment acknowledges this limitation. Consider asserting `changeCounter.get() < 10` or better yet, restructuring the test to make the assertion meaningful.

- [x] **No test for catch-up with TTL interaction:** The `Worker.createTasksWithState` logs a warning when `catchUpSeconds > ttl`, but there is no test that verifies this warning is actually emitted. Add a test using a mock CQL that returns a TTL smaller than the catch-up window and verify the warning log (or at least verify the worker still functions correctly).

- [x] **No test for `MasterConfiguration.computeCatchUpCutoff`:** The master configuration has its own `computeCatchUpCutoff()` method which delegates to `catchUpConfig.computeCatchUpCutoff(clock)`, but there are no direct tests for this delegation in a `MasterConfiguration` context.

- [x] **No test for `TaskState.createForWindow` with edge timestamps:** The new `TaskState.createForWindow` method should be tested with `Timestamp` values at `new Date(0)` and `new Date(Long.MAX_VALUE)` to verify no overflow in `plus(windowSizeMs, ChronoUnit.MILLIS)`.

## API Design Concerns

- [x] **Inconsistent timeout units:** `probeTimeoutSeconds` is in seconds, `queryTimeWindowSizeMs` is in milliseconds, `confidenceWindowSizeMs` is in milliseconds. The catch-up API uses seconds (`withCatchUpWindowSizeSeconds`) but also offers `withCatchUpWindow(Duration)`. Consider adding `withProbeTimeout(Duration)` for consistency with `withCatchUpWindow(Duration)`.

- [x] **`CatchUpConfiguration` constructor is package-private but `CatchUpConfiguration.Builder` is public:** The constructor `CatchUpConfiguration(long, long)` has package-private visibility but is called directly in test code (`new CatchUpConfiguration(3600, Long.MAX_VALUE)`). This creates a fragile test that bypasses the builder validation. Tests should use the builder.

- [x] **`CatchUpProber` is package-private but tightly coupled to `Worker`:** The prober is instantiated directly in `Worker.createTasksWithState` with raw configuration fields. Consider making it injectable via `WorkerConfiguration` for easier testing and to support alternative probe strategies.

## Performance Concerns

- [x] **`MAX_CONCURRENT_PROBES = 64` is hardcoded:** In `CatchUpProber`, the concurrency limit is a static final. Clusters with different sizes may benefit from different values. Consider making this configurable via `WorkerConfiguration` or `CatchUpConfiguration`, with 64 as the default.

- [x] **Probe queries may cause coordinator-side timeouts even with `setReadTimeoutMillis`:** The `setReadTimeoutMillis` on the driver statement controls the _client-side_ timeout. If the coordinator takes longer than the server-side `read_request_timeout_in_ms`, it will return an error. The probe uses `LOCAL_ONE` which is correct, but on heavily loaded clusters, even `LIMIT 1` queries on large partitions with many tombstones can be slow. Consider documenting this risk more prominently and recommending `gc_grace_seconds` tuning for CDC log tables.

- [x] **`dispatchProbes` prepares statements lazily per-probe via `cql.fetchFirstChangeTime`:** Since `prepareProbe` in `Driver3WorkerCQL` prepares the statement on first use, the very first probe for a given table will pay an extra round-trip for `PREPARE`. If there are many tasks per table, this is amortized, but with many tables it could add latency. The `computeIfAbsent` pattern ensures only one prepare per table. Eager preparation adds complexity for minimal benefit — accepted as-is.

- [x] **All probe futures for a candidate are awaited with `allOf` before processing results:** In `dispatchProbes`, `CompletableFuture.allOf(streamProbes)` waits for all streams before determining the minimum. If one stream in a task is very slow (tombstone scan), the entire candidate waits even if another stream already found data. Consider using `anyOf` or a more sophisticated approach that returns early when the first result is found, then cancels the remaining probes.

## Code Quality

- [x] **`Worker.createTasksWithState` does too much:** This single method fetches TTLs, builds task list, applies catch-up probes, and sets transport state. Consider extracting the TTL-vs-catchup warning and the catch-up probing into separate helper methods for readability.

- [x] **`CatchUpProber.collectAndClampResults` mixes control flow concerns:** The method handles timeout, execution exception, interruption, result processing, and logging in a single loop. Consider extracting the per-candidate result handling into a separate method.

- [x] **Code duplication in `CatchUpConfiguration.Builder` and `CatchUpConfiguration` constructor:** Both call `CatchUpUtils.validateWindowSize` — the builder validates on `set`, and the constructor validates again. Kept in constructor as a defensive measure since the constructor is package-private and could be called directly from tests or other packages.

- [x] **`CatchUpProber` uses raw index tracking via `ProbeCandidate.index`:** The index-based replacement pattern (collecting into a map of `Integer -> Task` keyed by list index) is fragile and hard to follow. Consider using a `Task -> Task` mapping or a parallel structure that doesn't rely on positional indices.

## Documentation Gaps

- [x] **README catch-up section does not document `withProbeTimeoutSeconds`:** The README mentions `withCatchUpWindow` and `withCatchUpWindowSizeSeconds` but omits `withProbeTimeoutSeconds`, which is an important tuning knob for production use. Add it to the README with guidance on when to increase it.

- [x] **No documentation on catch-up behavior with tablet-based CDC:** The README does not mention that catch-up is currently only supported for vnode-based CDC. Users on tablet-based clusters will get no catch-up behavior with no warning.

- [x] **Javadoc for `WorkerCQL.fetchFirstChangeTime` says "strictly after" but the query uses `cdc$time > ?`:** The Javadoc is correct ("after"), but the `getProbeStmt` in `Driver3WorkerCQL` uses `gt(quoteIfNecessary("cdc$time"), bindMarker())` with `UUIDs.startOf(after.toDate().getTime())` which creates the _minimum_ timeuuid for that millisecond. Since `startOf` returns the smallest possible timeuuid at that millisecond, and the query uses `>`, it correctly finds changes _at or after_ the same millisecond (since real timeuuids are always larger than `startOf`). Document this subtlety explicitly in the Javadoc to prevent future confusion.

- [x] **No changelog or migration guide:** The catch-up feature is fully documented in the README. The project does not maintain a separate CHANGELOG file — release notes are provided in GitHub releases.

## Logging Improvements

- [x] **`CatchUpProber` logs probe results at INFO level:** The line `"Catch-up probe: advancing task %s from %s to %s"` is logged at INFO for every advanced task. In a cluster with hundreds of vnodes, this could produce hundreds of INFO log lines on startup. Consider logging individual advances at FINE and only the summary at INFO.

- [x] **No log when catch-up cutoff is computed:** Neither `MasterConfiguration.computeCatchUpCutoff` nor `WorkerConfiguration.computeCatchUpCutoff` log the computed cutoff date. Add a FINE-level log showing the cutoff value to aid debugging.

- [x] **No log when `CatchUpProber.apply` skips probing due to open generation:** The prober silently returns the original task list when the generation is open. Add a FINE-level log like "Catch-up probing skipped: generation %s is still open".

## Hardcoded Values

- [x] **`MAX_CATCH_UP_WINDOW_SIZE_SECONDS = 90 days`:** In `CatchUpConfiguration`, the maximum is 90 days. Some use cases may need longer windows (e.g., quarterly compliance audits). Consider making this configurable or increasing the limit, documenting the rationale for the current cap.

- [x] **`DEFAULT_PROBE_TIMEOUT_SECONDS = 30`:** The 30-second default may be too short for clusters with large CDC partitions and many tombstones, or too long for clusters where a quick failure is preferred. Document the default value prominently in the builder Javadoc.

- [x] **`LOCAL_ONE` consistency level hardcoded in probe query:** In `Driver3WorkerCQL.fetchFirstChangeTime`, the probe always uses `LOCAL_ONE`. This is intentional: probes are best-effort optimizations where stale data is safe (the worker simply reads from an earlier window). Using a stronger consistency level would add latency without benefit. This choice is documented in the code comments and WorkerCQL Javadoc.

## Miscellaneous

- [x] **`CatchUpConfiguration` is in the `com.scylladb.cdc.model` package while `CatchUpProber` is in `com.scylladb.cdc.model.worker`:** This separation is correct: `CatchUpConfiguration` is shared between `MasterConfiguration` and `WorkerConfiguration` (both use it), while `CatchUpProber` is worker-specific. Moving `CatchUpConfiguration` to the worker package would break the master's dependency.

- [x] **`probeStmts` map in `Driver3WorkerCQL` is never cleared:** Accepted. Each entry is a single `PreparedStatement` reference per table — negligible memory. The driver session also caches prepared statements internally. Clearing would only save a few bytes per removed table and would risk redundant re-preparation.
