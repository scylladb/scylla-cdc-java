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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for verifying preimage and postimage CDC processing.
 * Tests that PRE_IMAGE and POST_IMAGE operation types are correctly received
 * when a table is configured with preimage and postimage enabled.
 */
@Tag("integration")
public class PreimagePostimageIT {
    Properties systemProperties = System.getProperties();
    String hostname =
        Preconditions.checkNotNull(systemProperties.getProperty("scylla.docker.hostname"));
    int port = Integer.parseInt(systemProperties.getProperty("scylla.docker.port"));

    @Test
    public void testPreimageAndPostimageOnUpdate() throws InterruptedException {
        String keyspace = "prepostks";
        String table = "preposttab";

        try (Cluster cluster = Cluster.builder().addContactPoint(hostname).withPort(port).build()) {
            Session session = cluster.connect();

            // Create keyspace
            session.execute(String.format("DROP KEYSPACE IF EXISTS %s;", keyspace));
            session.execute(String.format(
                "CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', "
                    + "'replication_factor': 1};", keyspace));

            // Create table with CDC, preimage, and postimage enabled
            session.execute(String.format(
                "CREATE TABLE %s.%s (pk int, ck int, v1 text, v2 int, PRIMARY KEY (pk, ck)) "
                    + "WITH cdc = {'enabled': true, 'preimage': true, 'postimage': true};",
                keyspace, table));

            List<RawChange> collectedChanges = Collections.synchronizedList(new ArrayList<>());

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

                consumer.start();

                // Insert a row
                session.execute(String.format(
                    "INSERT INTO %s.%s (pk, ck, v1, v2) VALUES (1, 1, 'initial', 100);",
                    keyspace, table));

                // Wait for the insert to be processed
                Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> collectedChanges.stream()
                        .anyMatch(c -> c.getOperationType() == RawChange.OperationType.ROW_INSERT
                            || c.getOperationType() == RawChange.OperationType.POST_IMAGE));

                // Update the row
                session.execute(String.format(
                    "UPDATE %s.%s SET v1 = 'updated', v2 = 200 WHERE pk = 1 AND ck = 1;",
                    keyspace, table));

                // Wait for preimage and postimage from the update
                Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> {
                        boolean hasPreimage = collectedChanges.stream()
                            .anyMatch(c -> c.getOperationType() == RawChange.OperationType.PRE_IMAGE);
                        boolean hasPostimage = collectedChanges.stream()
                            .filter(c -> c.getOperationType() == RawChange.OperationType.POST_IMAGE)
                            .count() >= 2; // One from insert, one from update
                        return hasPreimage && hasPostimage;
                    });

                consumer.stop();
            }

            // Verify we received preimage and postimage changes
            long preimageCount = collectedChanges.stream()
                .filter(c -> c.getOperationType() == RawChange.OperationType.PRE_IMAGE)
                .count();
            long postimageCount = collectedChanges.stream()
                .filter(c -> c.getOperationType() == RawChange.OperationType.POST_IMAGE)
                .count();

            assertTrue(preimageCount >= 1, "Expected at least 1 preimage change, got " + preimageCount);
            assertTrue(postimageCount >= 1, "Expected at least 1 postimage change, got " + postimageCount);

            // Verify preimage contains the old values
            RawChange preimage = collectedChanges.stream()
                .filter(c -> c.getOperationType() == RawChange.OperationType.PRE_IMAGE)
                .findFirst()
                .orElseThrow(() -> new AssertionError("No preimage found"));

            // The preimage should have pk=1, ck=1
            assertEquals(1, preimage.getCell("pk").getInt(), "Preimage pk should be 1");
            assertEquals(1, preimage.getCell("ck").getInt(), "Preimage ck should be 1");

            // Check preimage values - they should reflect the state before the update
            String preimageV1 = preimage.getCell("v1").getString();
            Integer preimageV2 = preimage.getCell("v2").getInt();
            assertEquals("initial", preimageV1, "Preimage v1 should be 'initial'");
            assertEquals(Integer.valueOf(100), preimageV2, "Preimage v2 should be 100");

            // Verify postimage from the update contains the new values
            // Find the postimage that has the updated values
            RawChange postimageAfterUpdate = collectedChanges.stream()
                .filter(c -> c.getOperationType() == RawChange.OperationType.POST_IMAGE)
                .filter(c -> {
                    String v1 = c.getCell("v1").getString();
                    return "updated".equals(v1);
                })
                .findFirst()
                .orElseThrow(() -> new AssertionError("No postimage with updated values found"));

            assertEquals(1, postimageAfterUpdate.getCell("pk").getInt(), "Postimage pk should be 1");
            assertEquals(1, postimageAfterUpdate.getCell("ck").getInt(), "Postimage ck should be 1");
            assertEquals("updated", postimageAfterUpdate.getCell("v1").getString(), "Postimage v1 should be 'updated'");
            assertEquals(Integer.valueOf(200), postimageAfterUpdate.getCell("v2").getInt(), "Postimage v2 should be 200");

            // Cleanup
            session.execute(String.format("DROP KEYSPACE %s;", keyspace));
        }
    }

    @Test
    public void testPreimageOnDelete() throws InterruptedException {
        String keyspace = "predelks";
        String table = "predeltab";

        try (Cluster cluster = Cluster.builder().addContactPoint(hostname).withPort(port).build()) {
            Session session = cluster.connect();

            // Create keyspace
            session.execute(String.format("DROP KEYSPACE IF EXISTS %s;", keyspace));
            session.execute(String.format(
                "CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', "
                    + "'replication_factor': 1};", keyspace));

            // Create table with CDC and preimage enabled
            session.execute(String.format(
                "CREATE TABLE %s.%s (pk int, ck int, v text, PRIMARY KEY (pk, ck)) "
                    + "WITH cdc = {'enabled': true, 'preimage': true};",
                keyspace, table));

            List<RawChange> collectedChanges = Collections.synchronizedList(new ArrayList<>());

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

                consumer.start();

                // Insert a row
                session.execute(String.format(
                    "INSERT INTO %s.%s (pk, ck, v) VALUES (1, 1, 'to_be_deleted');",
                    keyspace, table));

                // Wait for the insert to be processed
                Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> collectedChanges.stream()
                        .anyMatch(c -> c.getOperationType() == RawChange.OperationType.ROW_INSERT));

                // Delete the row
                session.execute(String.format(
                    "DELETE FROM %s.%s WHERE pk = 1 AND ck = 1;",
                    keyspace, table));

                // Wait for preimage from the delete
                Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> {
                        boolean hasDeletePreimage = collectedChanges.stream()
                            .anyMatch(c -> c.getOperationType() == RawChange.OperationType.PRE_IMAGE);
                        boolean hasRowDelete = collectedChanges.stream()
                            .anyMatch(c -> c.getOperationType() == RawChange.OperationType.ROW_DELETE);
                        return hasDeletePreimage && hasRowDelete;
                    });

                consumer.stop();
            }

            // Verify preimage from delete contains the deleted row's values
            RawChange preimage = collectedChanges.stream()
                .filter(c -> c.getOperationType() == RawChange.OperationType.PRE_IMAGE)
                .findFirst()
                .orElseThrow(() -> new AssertionError("No preimage found"));

            assertEquals(1, preimage.getCell("pk").getInt(), "Preimage pk should be 1");
            assertEquals(1, preimage.getCell("ck").getInt(), "Preimage ck should be 1");
            assertEquals("to_be_deleted", preimage.getCell("v").getString(), "Preimage v should be 'to_be_deleted'");

            // Verify the row delete operation is present
            boolean hasRowDelete = collectedChanges.stream()
                .anyMatch(c -> c.getOperationType() == RawChange.OperationType.ROW_DELETE);
            assertTrue(hasRowDelete, "Expected ROW_DELETE operation");

            // Cleanup
            session.execute(String.format("DROP KEYSPACE %s;", keyspace));
        }
    }

    @Test
    public void testPostimageOnInsert() throws InterruptedException {
        String keyspace = "postinsks";
        String table = "postinstab";

        try (Cluster cluster = Cluster.builder().addContactPoint(hostname).withPort(port).build()) {
            Session session = cluster.connect();

            // Create keyspace
            session.execute(String.format("DROP KEYSPACE IF EXISTS %s;", keyspace));
            session.execute(String.format(
                "CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', "
                    + "'replication_factor': 1};", keyspace));

            // Create table with CDC and postimage enabled (no preimage)
            session.execute(String.format(
                "CREATE TABLE %s.%s (pk int, ck int, v text, PRIMARY KEY (pk, ck)) "
                    + "WITH cdc = {'enabled': true, 'postimage': true};",
                keyspace, table));

            List<RawChange> collectedChanges = Collections.synchronizedList(new ArrayList<>());

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

                consumer.start();

                // Insert a row
                session.execute(String.format(
                    "INSERT INTO %s.%s (pk, ck, v) VALUES (1, 1, 'test_value');",
                    keyspace, table));

                // Wait for postimage from the insert
                Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> collectedChanges.stream()
                        .anyMatch(c -> c.getOperationType() == RawChange.OperationType.POST_IMAGE));

                consumer.stop();
            }

            // Verify postimage from insert contains the inserted row's values
            RawChange postimage = collectedChanges.stream()
                .filter(c -> c.getOperationType() == RawChange.OperationType.POST_IMAGE)
                .findFirst()
                .orElseThrow(() -> new AssertionError("No postimage found"));

            assertEquals(1, postimage.getCell("pk").getInt(), "Postimage pk should be 1");
            assertEquals(1, postimage.getCell("ck").getInt(), "Postimage ck should be 1");
            assertEquals("test_value", postimage.getCell("v").getString(), "Postimage v should be 'test_value'");

            // Verify there's no preimage (since we only enabled postimage)
            long preimageCount = collectedChanges.stream()
                .filter(c -> c.getOperationType() == RawChange.OperationType.PRE_IMAGE)
                .count();
            assertEquals(0, preimageCount, "Should have no preimage when preimage is not enabled");

            // Cleanup
            session.execute(String.format("DROP KEYSPACE %s;", keyspace));
        }
    }

    @Test
    public void testOperationTypeValues() {
        // Verify operation type byte values match documentation
        assertEquals((byte) 0, getOperationTypeByte(RawChange.OperationType.PRE_IMAGE));
        assertEquals((byte) 9, getOperationTypeByte(RawChange.OperationType.POST_IMAGE));
    }

    private byte getOperationTypeByte(RawChange.OperationType type) {
        // Access the operationId field via the enum's ordinal position
        // PRE_IMAGE is at ordinal 0, POST_IMAGE is at ordinal 9
        try {
            java.lang.reflect.Field field = RawChange.OperationType.class.getDeclaredField("operationId");
            field.setAccessible(true);
            return field.getByte(type);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Failed to access operationId field", e);
        }
    }
}
