package com.scylladb.cdc.lib;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.SyntaxError;
import com.google.common.base.Preconditions;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.RawChangeConsumer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * Per-row TTL expiry (ScyllaDB 2026.2+) is logged with cdc$operation -4
 * (service_partition_delete) for tables without a clustering key and -3
 * (service_row_delete) for tables with one. Expiry is driven by the Alternator
 * TTL scan, so the container runs with a short alternator_ttl_period_in_seconds.
 */
@Tag("integration")
public class PerRowTtlIT {
    Properties systemProperties = System.getProperties();
    String hostname =
        Preconditions.checkNotNull(systemProperties.getProperty("scylla.docker.hostname"));
    int port = Integer.parseInt(systemProperties.getProperty("scylla.docker.port"));

    @Test
    public void testPerRowTtlExpiryIsServicePartitionDelete() throws InterruptedException {
        assertExpiryLoggedAs("perrowttlpk",
            "(pk int PRIMARY KEY, v text, expiration timestamp TTL)",
            "INSERT INTO %s.%s (pk, v, expiration) VALUES (1, 'expires', '%s');",
            RawChange.OperationType.SERVICE_PARTITION_DELETE);
    }

    @Test
    public void testPerRowTtlExpiryIsServiceRowDelete() throws InterruptedException {
        assertExpiryLoggedAs("perrowttlck",
            "(pk int, ck int, v text, expiration timestamp TTL, PRIMARY KEY (pk, ck))",
            "INSERT INTO %s.%s (pk, ck, v, expiration) VALUES (1, 1, 'expires', '%s');",
            RawChange.OperationType.SERVICE_ROW_DELETE);
    }

    private void assertExpiryLoggedAs(String keyspace, String columns, String insertFormat,
                                      RawChange.OperationType expected) throws InterruptedException {
        String table = "tab";

        try (Cluster cluster = Cluster.builder().addContactPoint(hostname).withPort(port).build()) {
            Session session = cluster.connect();

            session.execute(String.format("DROP KEYSPACE IF EXISTS %s;", keyspace));
            TestKeyspaceUtils.createWithoutTablets(session, keyspace);

            try {
                session.execute(String.format(
                    "CREATE TABLE %s.%s %s WITH cdc = {'enabled': true};", keyspace, table, columns));
            } catch (SyntaxError e) {
                // Servers without per-row TTL reject the TTL column modifier at parse time.
                session.execute(String.format("DROP KEYSPACE %s;", keyspace));
                Assumptions.abort("Per-row TTL is not supported by this ScyllaDB version: " + e.getMessage());
            }

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

                consumer.start();

                Instant expiration = Instant.now().plusSeconds(3).truncatedTo(ChronoUnit.MILLIS);
                session.execute(String.format(insertFormat, keyspace, table, expiration));

                Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .until(() -> collectedChanges.stream()
                        .anyMatch(c -> c.getOperationType() == expected));
            }

            session.execute(String.format("DROP KEYSPACE %s;", keyspace));
        }
    }
}
