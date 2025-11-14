package com.scylladb.cdc.lib;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.worker.RawChangeConsumer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("integration")
public class EmptyGenerationIT {
  Properties systemProperties = System.getProperties();
  String hostname =
      Preconditions.checkNotNull(systemProperties.getProperty("scylla.docker.hostname"));
  int port = Integer.parseInt(systemProperties.getProperty("scylla.docker.port"));

  // This test pollutes system_distributed.cdc_generation_timestamps
  // Ideally it should be run in an isolated environment.
  // It should not be run in parallel.
  // If it fails then the fake generations may not be cleaned up.
  @Test
  public void consumeGenerationWithoutStreams() throws InterruptedException {
    String keyspace = "emptygenks";
    String table = "emptygentab";
    long recentGenerationTimestamp = 0;
    Session session;

    try (Cluster cluster = Cluster.builder().addContactPoint(hostname).withPort(port).build()) {
      session = cluster.connect();

      // Create keyspace without tablets enabled
      session.execute(String.format("DROP KEYSPACE IF EXISTS %s;", keyspace));
      session.execute(String.format(
          "CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', "
              + "'replication_factor': 1}", keyspace));

      session.execute(String.format("DROP TABLE IF EXISTS %s.%s;", keyspace, table));
      session.execute(
          String.format(
              "CREATE TABLE %s.%s (id int, value text, PRIMARY KEY (id)) "
                  + "WITH cdc = {'enabled': 'true'};",
              keyspace, table));

      AtomicInteger changeCounter = new AtomicInteger(0);
      RawChangeConsumer changeConsumer =
          change -> {
            changeCounter.incrementAndGet();
            return CompletableFuture.completedFuture(null);
          };

      try (CDCConsumer consumer =
               CDCConsumer.builder()
                   .addContactPoint(new InetSocketAddress(hostname, port))
                   .addTable(new TableName(keyspace, table))
                   .withConsumer(changeConsumer)
                   .withQueryTimeWindowSizeMs(10 * 1000)
                   .withConfidenceWindowSizeMs(5 * 1000)
                   .withSleepBeforeGenerationDoneMs(5 * 1000)
                   .withWorkersCount(1)
                   .build()) {

        // Perform inserts
        PreparedStatement ps = session.prepare(
            String.format("INSERT INTO %s.%s (id, value) VALUES (?, ?);", keyspace, table));

        int expectedChanges = 10;
        for (int i = 1; i <= expectedChanges; i++) {
          session.execute(ps.bind(i, "value" + i));
        }

        recentGenerationTimestamp = System.currentTimeMillis();
        // Insert empty generation earlier than any existing
        session.execute("INSERT INTO system_distributed.cdc_generation_timestamps (key, time) " +
            "VALUES ('timestamps', 946684799000);");
        // Insert empty generation after any existing
        session.execute("INSERT INTO system_distributed.cdc_generation_timestamps (key, time) " +
            "VALUES ('timestamps', " + recentGenerationTimestamp + ");");

        consumer.start();

        Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> (changeCounter.get() == expectedChanges));

        long finalRecentGenerationTimestamp = recentGenerationTimestamp;
        // Wait long enough for the consumer to encounter the last empty generation.
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> System.currentTimeMillis() > (finalRecentGenerationTimestamp + 10000L));

        assertEquals(expectedChanges, changeCounter.get(),
            "Expected to receive " + expectedChanges + " changes but got " + changeCounter.get());
      }

      session.execute(String.format("DROP KEYSPACE %s;", keyspace));
      session.execute("DELETE FROM system_distributed.cdc_generation_timestamps " +
          "WHERE key = 'timestamps' AND time = 946684799000;");
      session.execute("DELETE FROM system_distributed.cdc_generation_timestamps " +
          "WHERE key = 'timestamps' AND time = " + recentGenerationTimestamp + ";");
    }
  }
}
