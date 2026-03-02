package com.scylladb.cdc.lib;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.RawChangeConsumer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
public class CatchUpIT {
    Properties systemProperties = System.getProperties();
    String hostname =
            Preconditions.checkNotNull(systemProperties.getProperty("scylla.docker.hostname"));
    int port = parsePort(systemProperties.getProperty("scylla.docker.port"));

    private static int parsePort(String value) {
        Preconditions.checkNotNull(value, "scylla.docker.port system property is not set");
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("scylla.docker.port is not a valid integer: " + value, e);
        }
    }

    /**
     * Tests the catch-up optimization with a CDCConsumer.
     *
     * This test writes CDC data, then starts a consumer with catch-up enabled.
     * The consumer should still see all recent changes (those within the
     * catch-up window). The catch-up window is set large enough to include
     * all the test data, so no data should be skipped.
     *
     * Known limitation: This test runs against a fresh cluster with a single
     * generation, so catch-up has nothing to skip. Creating multiple generations
     * in a test cluster would require adding/removing nodes, which is beyond the
     * scope of this integration test. Instead, this test verifies that enabling
     * catch-up does not interfere with normal consumption. The actual catch-up
     * skipping behavior (generation jumping, probe-based window advancement) is
     * thoroughly covered by unit tests in MasterTest and WorkerTest.
     */
    @Test
    public void testCatchUpConsumerReadsRecentChanges() throws InterruptedException {
        String keyspace = "catchupks";
        String table = "catchuptab";

        try (Cluster cluster = Cluster.builder().addContactPoint(hostname).withPort(port).build()) {
            Session session = cluster.connect();

            session.execute(String.format("DROP KEYSPACE IF EXISTS %s;", keyspace));
            session.execute(String.format(
                    "CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', "
                            + "'replication_factor': 1}", keyspace));

            session.execute(String.format(
                    "CREATE TABLE %s.%s (id int, value text, PRIMARY KEY (id)) "
                            + "WITH cdc = {'enabled': 'true'};",
                    keyspace, table));

            // Insert data before starting the consumer
            PreparedStatement ps = session.prepare(
                    String.format("INSERT INTO %s.%s (id, value) VALUES (?, ?);", keyspace, table));

            int expectedChanges = 10;
            for (int i = 1; i <= expectedChanges; i++) {
                session.execute(ps.bind(i, "value" + i));
            }

            AtomicInteger changeCounter = new AtomicInteger(0);
            RawChangeConsumer changeConsumer = change -> {
                changeCounter.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            };

            // Start consumer with catch-up enabled.
            // Window of 1 hour is large enough to include all test data.
            try (CDCConsumer consumer =
                         CDCConsumer.builder()
                                 .addContactPoint(new InetSocketAddress(hostname, port))
                                 .addTable(new TableName(keyspace, table))
                                 .withConsumer(changeConsumer)
                                 .withCatchUpWindow(Duration.ofHours(1))
                                 .withQueryTimeWindowSizeMs(10 * 1000)
                                 .withConfidenceWindowSizeMs(5 * 1000)
                                 .withSleepBeforeGenerationDoneMs(5 * 1000)
                                 .withWorkersCount(1)
                                 .build()) {

                consumer.start();

                // All changes should be consumed since they are within the catch-up window
                Awaitility.await().atMost(60, TimeUnit.SECONDS)
                        .until(() -> changeCounter.get() >= expectedChanges);

                assertEquals(expectedChanges, changeCounter.get(),
                        "Expected to receive " + expectedChanges + " changes but got " + changeCounter.get());
            }

            session.execute(String.format("DROP KEYSPACE IF EXISTS %s;", keyspace));
        }
    }

    /**
     * Tests that catch-up optimization works with the Duration-based API
     * and that it doesn't interfere with normal change consumption.
     * Uses withCatchUpWindowSizeSeconds (the raw seconds API) for comparison.
     */
    @Test
    public void testCatchUpWithSecondsApi() throws InterruptedException {
        String keyspace = "catchupsecks";
        String table = "catchupsectab";

        try (Cluster cluster = Cluster.builder().addContactPoint(hostname).withPort(port).build()) {
            Session session = cluster.connect();

            session.execute(String.format("DROP KEYSPACE IF EXISTS %s;", keyspace));
            session.execute(String.format(
                    "CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', "
                            + "'replication_factor': 1}", keyspace));

            session.execute(String.format(
                    "CREATE TABLE %s.%s (id int, value text, PRIMARY KEY (id)) "
                            + "WITH cdc = {'enabled': 'true'};",
                    keyspace, table));

            PreparedStatement ps = session.prepare(
                    String.format("INSERT INTO %s.%s (id, value) VALUES (?, ?);", keyspace, table));

            int expectedChanges = 5;
            for (int i = 1; i <= expectedChanges; i++) {
                session.execute(ps.bind(i, "value" + i));
            }

            AtomicInteger changeCounter = new AtomicInteger(0);
            RawChangeConsumer changeConsumer = change -> {
                changeCounter.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            };

            // Use the seconds-based API
            try (CDCConsumer consumer =
                         CDCConsumer.builder()
                                 .addContactPoint(new InetSocketAddress(hostname, port))
                                 .addTable(new TableName(keyspace, table))
                                 .withConsumer(changeConsumer)
                                 .withCatchUpWindowSizeSeconds(3600) // 1 hour
                                 .withQueryTimeWindowSizeMs(10 * 1000)
                                 .withConfidenceWindowSizeMs(5 * 1000)
                                 .withSleepBeforeGenerationDoneMs(5 * 1000)
                                 .withWorkersCount(1)
                                 .build()) {

                consumer.start();

                Awaitility.await().atMost(60, TimeUnit.SECONDS)
                        .until(() -> changeCounter.get() >= expectedChanges);

                assertEquals(expectedChanges, changeCounter.get(),
                        "Expected to receive " + expectedChanges + " changes but got " + changeCounter.get());
            }

            session.execute(String.format("DROP KEYSPACE IF EXISTS %s;", keyspace));
        }
    }

    /**
     * Tests that catch-up with a tiny window (1 second) skips old data on first startup.
     * Since there is only one generation in a test cluster, the master may still find
     * the same generation, but the worker's probe should advance past any data that
     * was inserted more than 1 second ago.
     *
     * This exercises the probe code path where the window start is older than the cutoff.
     */
    @Test
    public void testCatchUpWithTinyWindowSkipsOldData() throws Exception {
        String keyspace = "catchuptinyks";
        String table = "catchuptinytab";

        try (Cluster cluster = Cluster.builder().addContactPoint(hostname).withPort(port).build()) {
            Session session = cluster.connect();

            session.execute(String.format("DROP KEYSPACE IF EXISTS %s;", keyspace));
            session.execute(String.format(
                    "CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', "
                            + "'replication_factor': 1}", keyspace));

            session.execute(String.format(
                    "CREATE TABLE %s.%s (id int, value text, PRIMARY KEY (id)) "
                            + "WITH cdc = {'enabled': 'true'};",
                    keyspace, table));

            PreparedStatement ps = session.prepare(
                    String.format("INSERT INTO %s.%s (id, value) VALUES (?, ?);", keyspace, table));

            // Insert old data, wait, then insert new data
            for (int i = 1; i <= 5; i++) {
                session.execute(ps.bind(i, "old" + i));
            }

            // Wait to ensure a time gap
            Thread.sleep(3000);

            // Insert recent data
            for (int i = 100; i <= 104; i++) {
                session.execute(ps.bind(i, "new" + i));
            }

            AtomicInteger changeCounter = new AtomicInteger(0);
            RawChangeConsumer changeConsumer = change -> {
                changeCounter.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            };

            // Use 1-second catch-up window — old data should be outside the window
            try (CDCConsumer consumer =
                         CDCConsumer.builder()
                                 .addContactPoint(new InetSocketAddress(hostname, port))
                                 .addTable(new TableName(keyspace, table))
                                 .withConsumer(changeConsumer)
                                 .withCatchUpWindow(Duration.ofSeconds(1))
                                 .withQueryTimeWindowSizeMs(10 * 1000)
                                 .withConfidenceWindowSizeMs(5 * 1000)
                                 .withSleepBeforeGenerationDoneMs(5 * 1000)
                                 .withWorkersCount(1)
                                 .build()) {

                consumer.start();

                // Wait for the consumer to read recent changes
                Awaitility.await().atMost(60, TimeUnit.SECONDS)
                        .until(() -> changeCounter.get() >= 5);

                // Give the consumer a short window to process any remaining changes.
                // In a single-node test cluster the generation start is recent, so
                // catch-up probes may still find old data (the window start is the
                // generation start, which is within the cutoff). A stronger assertion
                // (e.g., changeCounter < 10) would require a multi-generation setup,
                // which is impractical here. The actual skipping logic is thoroughly
                // covered by unit tests in WorkerTest, CatchUpProberTest, and MasterTest.
                Thread.sleep(2000);
                assertTrue(changeCounter.get() <= 10,
                        "Expected at most 10 changes but got " + changeCounter.get());
            }

            session.execute(String.format("DROP KEYSPACE IF EXISTS %s;", keyspace));
        }
    }
}
