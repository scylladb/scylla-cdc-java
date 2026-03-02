package com.scylladb.cdc.model;

import com.scylladb.cdc.model.master.MasterConfiguration;
import com.scylladb.cdc.model.worker.WorkerConfiguration;
import com.scylladb.cdc.cql.MockWorkerCQL;
import com.scylladb.cdc.model.worker.Consumer;
import com.scylladb.cdc.transport.MockMasterTransport;
import com.scylladb.cdc.transport.MockWorkerTransport;
import com.scylladb.cdc.cql.MockMasterCQL;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

class CatchUpUtilsTest {

    @Test
    void testComputeCutoffDisabledWhenZero() {
        Clock clock = Clock.fixed(Instant.parse("2025-01-01T00:00:00Z"), ZoneOffset.UTC);
        Optional<Date> result = CatchUpUtils.computeCutoff(0, clock);
        assertFalse(result.isPresent());
    }

    @Test
    void testComputeCutoffDisabledWhenNegative() {
        Clock clock = Clock.fixed(Instant.parse("2025-01-01T00:00:00Z"), ZoneOffset.UTC);
        Optional<Date> result = CatchUpUtils.computeCutoff(-1, clock);
        assertFalse(result.isPresent());
    }

    @Test
    void testComputeCutoffReturnsCorrectValue() {
        Instant now = Instant.parse("2025-01-01T00:01:00Z");
        Clock clock = Clock.fixed(now, ZoneOffset.UTC);
        Optional<Date> result = CatchUpUtils.computeCutoff(60, clock);
        assertTrue(result.isPresent());
        // now - 60 seconds = 2025-01-01T00:00:00Z
        assertEquals(Date.from(Instant.parse("2025-01-01T00:00:00Z")), result.get());
    }

    @Test
    void testComputeCutoffLargeWindow() {
        Instant now = Instant.parse("2025-06-15T12:00:00Z");
        Clock clock = Clock.fixed(now, ZoneOffset.UTC);
        long ninetyDaysInSeconds = 90 * 24 * 3600L;
        Optional<Date> result = CatchUpUtils.computeCutoff(ninetyDaysInSeconds, clock);
        assertTrue(result.isPresent());
        long expectedMs = now.toEpochMilli() - ninetyDaysInSeconds * 1000;
        assertEquals(new Date(expectedMs), result.get());
    }

    @Test
    void testValidateWindowSizeValid() {
        // Should not throw
        CatchUpUtils.validateWindowSize(0, 100);
        CatchUpUtils.validateWindowSize(50, 100);
        CatchUpUtils.validateWindowSize(100, 100);
    }

    @Test
    void testValidateWindowSizeNegative() {
        assertThrows(IllegalArgumentException.class, () ->
                CatchUpUtils.validateWindowSize(-1, 100));
    }

    @Test
    void testValidateWindowSizeExceedsMax() {
        assertThrows(IllegalArgumentException.class, () ->
                CatchUpUtils.validateWindowSize(101, 100));
    }

    // --- tryJumpToRecentGeneration tests ---

    private static GenerationId makeGenId(long ms) {
        return new GenerationId(new Timestamp(new Date(ms)));
    }

    @Test
    void testTryJumpDisabledWhenCutoffEmpty() throws InterruptedException {
        Optional<GenerationId> result = CatchUpUtils.tryJumpToRecentGeneration(
                Optional.empty(),
                cutoff -> { throw new AssertionError("should not be called"); },
                Optional.empty());
        assertFalse(result.isPresent());
    }

    @Test
    void testTryJumpSuccessful() throws InterruptedException {
        Date cutoff = new Date(1000);
        GenerationId target = makeGenId(900);
        Optional<GenerationId> result = CatchUpUtils.tryJumpToRecentGeneration(
                Optional.of(cutoff),
                c -> CompletableFuture.completedFuture(Optional.of(target)),
                Optional.empty());
        assertTrue(result.isPresent());
        assertEquals(target, result.get());
    }

    @Test
    void testTryJumpNoGenerationFound() throws InterruptedException {
        Date cutoff = new Date(1000);
        Optional<GenerationId> result = CatchUpUtils.tryJumpToRecentGeneration(
                Optional.of(cutoff),
                c -> CompletableFuture.completedFuture(Optional.empty()),
                Optional.empty());
        assertFalse(result.isPresent());
    }

    @Test
    void testTryJumpExecutionExceptionFallsBack() throws InterruptedException {
        Date cutoff = new Date(1000);
        Optional<GenerationId> result = CatchUpUtils.tryJumpToRecentGeneration(
                Optional.of(cutoff),
                c -> {
                    CompletableFuture<Optional<GenerationId>> f = new CompletableFuture<>();
                    f.completeExceptionally(new RuntimeException("CQL error"));
                    return f;
                },
                Optional.empty());
        assertFalse(result.isPresent());
    }

    // --- CatchUpConfiguration tests ---

    @Test
    void testCatchUpConfigurationDisabledByDefault() {
        CatchUpConfiguration config = new CatchUpConfiguration.Builder().build();
        assertFalse(config.isEnabled());
        assertEquals(0, config.getCatchUpWindowSizeSeconds());
    }

    @Test
    void testCatchUpConfigurationDisabledOverridesEnabled() {
        CatchUpConfiguration.Builder helper = new CatchUpConfiguration.Builder();
        helper.setCatchUpWindowSizeSeconds(3600);
        // Simulate withCatchUpDisabled() — sets back to 0
        helper.setCatchUpWindowSizeSeconds(0);
        CatchUpConfiguration config = helper.build();
        assertFalse(config.isEnabled());
    }

    @Test
    void testCatchUpConfigurationEnabled() {
        CatchUpConfiguration.Builder helper = new CatchUpConfiguration.Builder();
        helper.setCatchUpWindowSizeSeconds(3600);
        CatchUpConfiguration config = helper.build();
        assertTrue(config.isEnabled());
        assertEquals(3600, config.getCatchUpWindowSizeSeconds());
    }

    @Test
    void testCatchUpConfigurationComputeCutoff() {
        CatchUpConfiguration.Builder helper = new CatchUpConfiguration.Builder();
        helper.setCatchUpWindowSizeSeconds(60);
        CatchUpConfiguration config = helper.build();
        Instant now = Instant.parse("2025-01-01T00:01:00Z");
        Clock clock = Clock.fixed(now, ZoneOffset.UTC);
        Optional<Date> cutoff = config.computeCatchUpCutoff(clock);
        assertTrue(cutoff.isPresent());
        assertEquals(Date.from(Instant.parse("2025-01-01T00:00:00Z")), cutoff.get());
    }

    @Test
    void testCatchUpConfigurationComputeCutoffDisabled() {
        CatchUpConfiguration config = new CatchUpConfiguration.Builder().build();
        Clock clock = Clock.fixed(Instant.parse("2025-01-01T00:00:00Z"), ZoneOffset.UTC);
        assertFalse(config.computeCatchUpCutoff(clock).isPresent());
    }

    @Test
    void testTryJumpInterruptedExceptionPropagates() {
        Date cutoff = new Date(1000);
        CompletableFuture<Optional<GenerationId>> neverCompletes = new CompletableFuture<>();
        Thread.currentThread().interrupt();
        assertThrows(InterruptedException.class, () ->
                CatchUpUtils.tryJumpToRecentGeneration(
                        Optional.of(cutoff),
                        c -> neverCompletes,
                        Optional.empty()));
        // Clear interrupt flag to not affect other tests
        Thread.interrupted();
    }

    // --- CatchUpConfiguration edge case tests ---

    @Test
    void testBuilderSetCatchUpWindowSubSecondThrows() {
        CatchUpConfiguration.Builder builder = new CatchUpConfiguration.Builder();
        assertThrows(IllegalArgumentException.class, () ->
                builder.setCatchUpWindow(Duration.ofMillis(500)));
    }

    @Test
    void testBuilderSetCatchUpWindowZeroDisables() {
        CatchUpConfiguration.Builder builder = new CatchUpConfiguration.Builder();
        builder.setCatchUpWindow(Duration.ZERO);
        CatchUpConfiguration config = builder.build();
        assertFalse(config.isEnabled());
    }

    @Test
    void testBuilderSetCatchUpWindowNullThrows() {
        CatchUpConfiguration.Builder builder = new CatchUpConfiguration.Builder();
        assertThrows(NullPointerException.class, () ->
                builder.setCatchUpWindow(null));
    }

    @Test
    void testCatchUpConfigurationProbeTimeoutZeroThrows() {
        CatchUpConfiguration.Builder builder = new CatchUpConfiguration.Builder();
        builder.setCatchUpWindowSizeSeconds(3600);
        assertThrows(IllegalArgumentException.class, () ->
                builder.setProbeTimeoutSeconds(0));
    }

    @Test
    void testCatchUpConfigurationProbeTimeoutMaxValue() {
        CatchUpConfiguration.Builder builder = new CatchUpConfiguration.Builder();
        builder.setCatchUpWindowSizeSeconds(3600);
        builder.setProbeTimeoutSeconds(CatchUpConfiguration.MAX_PROBE_TIMEOUT_SECONDS);
        CatchUpConfiguration config = builder.build();
        assertEquals(CatchUpConfiguration.MAX_PROBE_TIMEOUT_SECONDS, config.getProbeTimeoutSeconds());
    }

    @Test
    void testCatchUpConfigurationProbeTimeoutExceedsMaxThrows() {
        CatchUpConfiguration.Builder builder = new CatchUpConfiguration.Builder();
        builder.setCatchUpWindowSizeSeconds(3600);
        assertThrows(IllegalArgumentException.class, () ->
                builder.setProbeTimeoutSeconds(CatchUpConfiguration.MAX_PROBE_TIMEOUT_SECONDS + 1));
    }

    @Test
    void testTryJumpWithTableContext() throws InterruptedException {
        Date cutoff = new Date(1000);
        GenerationId target = makeGenId(900);
        TableName table = new TableName("ks", "tbl");
        Optional<GenerationId> result = CatchUpUtils.tryJumpToRecentGeneration(
                Optional.of(cutoff),
                c -> CompletableFuture.completedFuture(Optional.of(target)),
                Optional.of(table));
        assertTrue(result.isPresent());
        assertEquals(target, result.get());
    }

    // --- MasterConfiguration.computeCatchUpCutoff delegation test ---

    @Test
    void testMasterConfigurationComputeCatchUpCutoff() {
        Instant now = Instant.parse("2025-01-01T00:01:00Z");
        Clock clock = Clock.fixed(now, ZoneOffset.UTC);
        TableName table = new TableName("ks", "tbl");

        MasterConfiguration config = MasterConfiguration.builder()
                .withCQL(new MockMasterCQL())
                .withTransport(new MockMasterTransport())
                .addTable(table)
                .withClock(clock)
                .withCatchUpWindowSizeSeconds(60)
                .build();

        Optional<Date> cutoff = config.computeCatchUpCutoff();
        assertTrue(cutoff.isPresent());
        assertEquals(Date.from(Instant.parse("2025-01-01T00:00:00Z")), cutoff.get());
    }

    @Test
    void testMasterConfigurationComputeCatchUpCutoffDisabled() {
        Clock clock = Clock.fixed(Instant.parse("2025-01-01T00:00:00Z"), ZoneOffset.UTC);
        TableName table = new TableName("ks", "tbl");

        MasterConfiguration config = MasterConfiguration.builder()
                .withCQL(new MockMasterCQL())
                .withTransport(new MockMasterTransport())
                .addTable(table)
                .withClock(clock)
                .build();

        assertFalse(config.computeCatchUpCutoff().isPresent());
    }
}
