package com.scylladb.cdc.lib;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.RawChangeConsumer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests characterizing first-time CDC consumer startup latency.
 * Related to issue #172: first-time consumers scan from the very first
 * generation start timestamp, causing many empty CQL queries before
 * reaching recent data.
 *
 * These tests verify correctness and log timing metrics for analysis.
 * Timing values are not asserted to avoid flakiness.
 */
@Tag("integration")
public class FirstStartupLatencyIT {
    Properties systemProperties = System.getProperties();
    String hostname =
        Preconditions.checkNotNull(systemProperties.getProperty("scylla.docker.hostname"));
    int port = Integer.parseInt(systemProperties.getProperty("scylla.docker.port"));

    @Test
    public void testEmptyTableFirstStartupLatency() throws InterruptedException {
        String keyspace = "emptystartks";
        String table = "emptystarttab";

        try (Cluster cluster = Cluster.builder().addContactPoint(hostname).withPort(port).build()) {
            Session session = cluster.connect();

            session.execute(String.format("DROP KEYSPACE IF EXISTS %s;", keyspace));
            session.execute(String.format(
                "CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', "
                    + "'replication_factor': 1};", keyspace));
            session.execute(String.format(
                "CREATE TABLE %s.%s (pk int, ck int, v text, PRIMARY KEY (pk, ck)) "
                    + "WITH cdc = {'enabled': true};",
                keyspace, table));

            List<RawChange> collectedChanges = new CopyOnWriteArrayList<>();

            RawChangeConsumer changeConsumer = change -> {
                collectedChanges.add(change);
                return CompletableFuture.completedFuture(null);
            };

            try (CDCConsumer consumer = CDCConsumer.builder()
                    .addContactPoint(new InetSocketAddress(hostname, port))
                    .addTable(new TableName(keyspace, table))
                    .withConsumer(changeConsumer)
                    .withQueryTimeWindowSizeMs(5 * 1000)
                    .withConfidenceWindowSizeMs(3 * 1000)
                    .withWorkersCount(1)
                    .build()) {

                long consumerStartTime = System.currentTimeMillis();
                consumer.start();

                // Wait a bit then insert a record
                Thread.sleep(1000);

                long insertTime = System.currentTimeMillis();
                session.execute(String.format(
                    "INSERT INTO %s.%s (pk, ck, v) VALUES (1, 1, 'first_record');",
                    keyspace, table));

                Awaitility.await()
                    .atMost(120, TimeUnit.SECONDS)
                    .until(() -> collectedChanges.stream()
                        .anyMatch(c -> c.getOperationType() == RawChange.OperationType.ROW_INSERT));

                long firstChangeTime = System.currentTimeMillis();
                consumer.stop();

                // Log timing metrics for analysis
                System.out.println("[FirstStartupLatencyIT.testEmptyTableFirstStartupLatency]");
                System.out.println("  consumer.start() -> first change: "
                    + (firstChangeTime - consumerStartTime) + " ms");
                System.out.println("  INSERT -> first change: "
                    + (firstChangeTime - insertTime) + " ms");
            }

            // Verify correctness
            long insertCount = collectedChanges.stream()
                .filter(c -> c.getOperationType() == RawChange.OperationType.ROW_INSERT)
                .count();
            assertTrue(insertCount >= 1, "Expected at least 1 ROW_INSERT, got " + insertCount);

            RawChange insert = collectedChanges.stream()
                .filter(c -> c.getOperationType() == RawChange.OperationType.ROW_INSERT)
                .findFirst()
                .orElseThrow(() -> new AssertionError("No ROW_INSERT found"));
            assertEquals(1, insert.getCell("pk").getInt());
            assertEquals(1, insert.getCell("ck").getInt());
            assertEquals("first_record", insert.getCell("v").getString());

            // TODO (issue #172): After fix, assert that consumer.start() -> first change
            // is bounded (e.g., < confidenceWindow + 2 * queryTimeWindow)

            session.execute(String.format("DROP KEYSPACE %s;", keyspace));
        }
    }

    @Test
    public void testPastRecordsStartupLatency() throws InterruptedException {
        String keyspace = "pastrecks";
        String table = "pastrectab";

        try (Cluster cluster = Cluster.builder().addContactPoint(hostname).withPort(port).build()) {
            Session session = cluster.connect();

            session.execute(String.format("DROP KEYSPACE IF EXISTS %s;", keyspace));
            session.execute(String.format(
                "CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', "
                    + "'replication_factor': 1};", keyspace));
            session.execute(String.format(
                "CREATE TABLE %s.%s (pk int, ck int, v text, PRIMARY KEY (pk, ck)) "
                    + "WITH cdc = {'enabled': true};",
                keyspace, table));

            // Insert 5 records before starting the consumer
            for (int i = 1; i <= 5; i++) {
                session.execute(String.format(
                    "INSERT INTO %s.%s (pk, ck, v) VALUES (%d, %d, 'record_%d');",
                    keyspace, table, i, i, i));
            }

            // Wait for records to age past the confidence window
            Thread.sleep(15000);

            List<RawChange> collectedChanges = new CopyOnWriteArrayList<>();

            RawChangeConsumer changeConsumer = change -> {
                collectedChanges.add(change);
                return CompletableFuture.completedFuture(null);
            };

            try (CDCConsumer consumer = CDCConsumer.builder()
                    .addContactPoint(new InetSocketAddress(hostname, port))
                    .addTable(new TableName(keyspace, table))
                    .withConsumer(changeConsumer)
                    .withQueryTimeWindowSizeMs(5 * 1000)
                    .withConfidenceWindowSizeMs(3 * 1000)
                    .withWorkersCount(1)
                    .build()) {

                long consumerStartTime = System.currentTimeMillis();
                consumer.start();

                Awaitility.await()
                    .atMost(120, TimeUnit.SECONDS)
                    .until(() -> collectedChanges.stream()
                        .filter(c -> c.getOperationType() == RawChange.OperationType.ROW_INSERT)
                        .count() >= 5);

                long lastChangeTime = System.currentTimeMillis();
                consumer.stop();

                // Log timing metrics
                System.out.println("[FirstStartupLatencyIT.testPastRecordsStartupLatency]");
                System.out.println("  consumer.start() -> all 5 changes: "
                    + (lastChangeTime - consumerStartTime) + " ms");
            }

            // Verify all 5 records consumed
            long insertCount = collectedChanges.stream()
                .filter(c -> c.getOperationType() == RawChange.OperationType.ROW_INSERT)
                .count();
            assertEquals(5, insertCount, "Expected 5 ROW_INSERTs, got " + insertCount);

            // TODO (issue #172): After fix, assert that consumer.start() -> last change
            // is bounded (e.g., < confidenceWindow + N * queryTimeWindow for small N)

            session.execute(String.format("DROP KEYSPACE %s;", keyspace));
        }
    }

    @Test
    public void testSpreadRecordsStartupLatency() throws InterruptedException {
        String keyspace = "spreadrecks";
        String table = "spreadrectab";

        try (Cluster cluster = Cluster.builder().addContactPoint(hostname).withPort(port).build()) {
            Session session = cluster.connect();

            session.execute(String.format("DROP KEYSPACE IF EXISTS %s;", keyspace));
            session.execute(String.format(
                "CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', "
                    + "'replication_factor': 1};", keyspace));
            session.execute(String.format(
                "CREATE TABLE %s.%s (pk int, ck int, v text, PRIMARY KEY (pk, ck)) "
                    + "WITH cdc = {'enabled': true};",
                keyspace, table));

            // Insert 5 records spread across ~30s (6s intervals)
            for (int i = 1; i <= 5; i++) {
                session.execute(String.format(
                    "INSERT INTO %s.%s (pk, ck, v) VALUES (%d, %d, 'spread_%d');",
                    keyspace, table, i, i, i));
                if (i < 5) {
                    Thread.sleep(6000);
                }
            }

            // Wait for last record to age past confidence window + query window
            Thread.sleep(8000); // confidenceWindow(3s) + queryTimeWindow(5s)

            List<RawChange> collectedChanges = new CopyOnWriteArrayList<>();

            RawChangeConsumer changeConsumer = change -> {
                collectedChanges.add(change);
                return CompletableFuture.completedFuture(null);
            };

            try (CDCConsumer consumer = CDCConsumer.builder()
                    .addContactPoint(new InetSocketAddress(hostname, port))
                    .addTable(new TableName(keyspace, table))
                    .withConsumer(changeConsumer)
                    .withQueryTimeWindowSizeMs(5 * 1000)
                    .withConfidenceWindowSizeMs(3 * 1000)
                    .withWorkersCount(1)
                    .build()) {

                long consumerStartTime = System.currentTimeMillis();
                consumer.start();

                Awaitility.await()
                    .atMost(120, TimeUnit.SECONDS)
                    .until(() -> collectedChanges.stream()
                        .filter(c -> c.getOperationType() == RawChange.OperationType.ROW_INSERT)
                        .count() >= 5);

                long lastChangeTime = System.currentTimeMillis();
                consumer.stop();

                System.out.println("[FirstStartupLatencyIT.testSpreadRecordsStartupLatency]");
                System.out.println("  consumer.start() -> all 5 changes: "
                    + (lastChangeTime - consumerStartTime) + " ms");
            }

            // Verify all 5 records consumed
            List<RawChange> inserts = collectedChanges.stream()
                .filter(c -> c.getOperationType() == RawChange.OperationType.ROW_INSERT)
                .collect(Collectors.toList());
            assertEquals(5, inserts.size(), "Expected 5 ROW_INSERTs, got " + inserts.size());

            // Verify each pk value is present
            Set<Integer> pkValues = inserts.stream()
                .map(c -> c.getCell("pk").getInt())
                .collect(Collectors.toSet());
            for (int i = 1; i <= 5; i++) {
                assertTrue(pkValues.contains(i), "Missing pk value: " + i);
            }

            // TODO (issue #172): After fix, assert that consumer.start() -> last change
            // is bounded rather than proportional to the spread duration

            session.execute(String.format("DROP KEYSPACE %s;", keyspace));
        }
    }
}
