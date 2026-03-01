# TODO: Catch-Up Feature Improvements

## Bugs / Correctness

- [x] **`CDCConsumer.Builder.withCatchUpWindow(Duration)` silently truncates sub-second precision** — Fixed: now delegates to `MasterConfiguration.Builder.withCatchUpWindow()` and `WorkerConfiguration.Builder.withCatchUpWindow()` which call `CatchUpConfiguration.Builder.setCatchUpWindow()` with proper sub-second validation.

- [x] **CQL probe read timeout is hardcoded independently of `probeTimeoutSeconds`** — Fixed: `WorkerCQL.fetchFirstChangeTime()` now accepts a `readTimeoutMs` parameter, and `CatchUpProber` passes `TimeUnit.SECONDS.toMillis(probeTimeoutSeconds)` to it. `Driver3WorkerCQL` uses this parameter instead of the hardcoded `30_000ms`.

- [x] **Legacy V1 `ORDER BY DESC` with `ALLOW FILTERING` query needs validation** — Added validation comment to `Driver3MasterCQL.getLegacyFetchLargestGenerationBeforeOrAt()` confirming the V1 table schema supports this query pattern. The existing runtime warning in `fetchLargestGenerationBeforeOrAt()` alerts operators to the performance implications.

- [x] **`CatchUpProber.apply()` continues stream iteration after `InterruptedException`** — Fixed: replaced stream().map().collect() with explicit for-loops that track an `interrupted` flag and break out of iteration on interrupt, preventing redundant failed futures.

## Code Quality

- [x] **Replace fully-qualified class names with imports** — Fixed: `java.util.HashMap` in `CatchUpProber.java` and `java.util.Date` in `Driver3WorkerCQL.java` replaced with standard imports.

- [x] **Extract `CatchUpProber.apply()` into smaller methods** — Refactored into: `collectCandidates()`, `dispatchProbes()`, `collectAndClampResults()`, `clampToGeneration()`, `cancelRemainingProbes()`, and `buildReplacementList()`.

- [x] **Rename `CatchUpConfiguration.BuilderHelper`** — Renamed to `CatchUpConfiguration.Builder` with updated Javadoc.

- [x] **Redundant validation in `CDCConsumer.Builder.withCatchUpWindow`** — Fixed as part of the sub-second precision fix: `withCatchUpWindow(Duration)` now delegates directly to the configuration builders, which perform validation once in `CatchUpConfiguration.Builder.setCatchUpWindow()`.

- [x] **`TableCDCController.getGenerationId` changed from `.join()` to `.get()`** — Verified: all callers declare `throws ExecutionException, InterruptedException` and handle checked exceptions consistently. No code relies on `CompletionException` wrapping.

## Test Coverage

- [x] **Add test for `InterruptedException` path in `CatchUpUtils.tryJumpToRecentGeneration`** — Added `testTryJumpInterruptedExceptionPropagates` in `CatchUpUtilsTest`.

- [x] **Add tests for `CatchUpConfiguration` edge cases** — Added tests: `testBuilderSetCatchUpWindowSubSecondThrows`, `testBuilderSetCatchUpWindowZeroDisables`, `testBuilderSetCatchUpWindowNullThrows`, `testCatchUpConfigurationProbeTimeoutZeroThrows`, `testCatchUpConfigurationProbeTimeoutMaxValue`.

- [ ] **Add integration test for `withProbeTimeoutSeconds()`** — The API is exposed in `CDCConsumer.Builder` but not exercised in `CatchUpIT`. Requires Docker.

- [ ] **Add integration test for `withCatchUpDisabled()`** — Only covered in unit tests, not in `CatchUpIT`. Requires Docker.

- [ ] **Improve `CatchUpIT.testCatchUpWithTinyWindowSkipsOldData` reliability** — Uses `Thread.sleep(3000)` for timing separation and the assertion `assertTrue(changeCounter.get() <= 10)` allows all 10 changes. Consider restructuring. Requires Docker.

- [x] **Add unit test for probe timeout behavior** — Added `testWorkerProbeTimeoutFallsBack` in `WorkerTest`: configures `probeTimeoutSeconds=1` with a probe delay of 10 seconds and verifies the worker falls back to the original window start.

- [x] **Add unit test for probe clamping to `generationEnd - 1ms`** — Already existed as `testWorkerProbeClampedToGenerationEnd` in `WorkerTest`.

## Design / Configuration

- [ ] **Make `CatchUpProber.MAX_CONCURRENT_PROBES` configurable** — The hardcoded value of 64 may be too aggressive for small clusters or too conservative for large ones. Expose via `CatchUpConfiguration`.

- [x] **Per-candidate vs per-probe timeout semantics** — Documented in `CatchUpProber` Javadoc: the timeout applies per candidate task (via `allOf` on all stream probes), not per individual stream probe. A candidate with many streams could wait longer in aggregate due to semaphore contention.

- [x] **`WorkerCQL.fetchFirstChangeTime` is a breaking interface addition** — Fixed: `fetchFirstChangeTime` on `WorkerCQL`, and `fetchFirstGenerationIdAfter`, `fetchLastGenerationBeforeOrAt`, `fetchLastTableGenerationBeforeOrAt`, `fetchFirstTableGenerationIdAfter` on `MasterCQL` are now `default` methods that throw `UnsupportedOperationException`, maintaining backward compatibility.

## Documentation

- [x] **Document `withProbeTimeoutSeconds()` in README** — Added "Probe timeout" section with code example and explanation.

- [x] **Document the 90-day maximum catch-up window limit** — Added "Maximum catch-up window" section in README.

- [x] **Add operational guidance for tombstone scanning** — Added "Tombstone scanning" section in README with mitigation strategies.

- [ ] **Document the `Clock` propagation fix** — `CDCConsumer.Builder.withClock()` now sets the clock on both worker and master configuration builders. This behavioral fix should be documented in changelog/release notes (not README).
