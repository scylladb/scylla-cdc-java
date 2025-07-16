package com.scylladb.cdc.lib;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.abort;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.google.common.base.Preconditions;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.RawChangeConsumer;
import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
public class TabletsIT {
  Properties systemProperties = System.getProperties();
  String hostname =
      Preconditions.checkNotNull(systemProperties.getProperty("scylla.docker.hostname"));
  int port = Integer.parseInt(systemProperties.getProperty("scylla.docker.port"));
  String scyllaVersion =
      Preconditions.checkNotNull(systemProperties.getProperty("scylla.docker.version"));

  @Test
  public void consumeFromTabletsKeyspace() throws InterruptedException {
    String keyspace = "tabletsks";
    String table = "tabletstest";
    Session session;

    try (Cluster cluster = Cluster.builder().addContactPoint(hostname).withPort(port).build()) {
      session = cluster.connect();

      // Create keyspace with tablets enabled
      session.execute(String.format("DROP KEYSPACE IF EXISTS %s;", keyspace));
      tryCreateKeyspace(session, String.format(
          "CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', "
              + "'replication_factor': 1} AND tablets = {'initial': 8};", keyspace));

      session.execute(String.format("DROP TABLE IF EXISTS %s.%s;", keyspace, table));
      tryCreateTable(session,
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
              .withWorkersCount(1)
              .build()) {

        consumer.start();

        // Perform inserts
        PreparedStatement ps = session.prepare(
            String.format("INSERT INTO %s.%s (id, value) VALUES (?, ?);", keyspace, table));

        int expectedChanges = 10;
        for (int i = 1; i <= expectedChanges; i++) {
          session.execute(ps.bind(i, "value" + i));
        }

        // Wait for all changes to be consumed
        long timeoutMs = 60 * 1000;
        long startTime = System.currentTimeMillis();
        long pollIntervalMs = 500; // Check every 500ms

        while (changeCounter.get() < expectedChanges &&
               (System.currentTimeMillis() - startTime) < timeoutMs) {
          Thread.sleep(pollIntervalMs);
        }

        // Verify we received all expected changes
        assertEquals(expectedChanges, changeCounter.get(),
            "Expected to receive " + expectedChanges + " changes but got " + changeCounter.get());
      }

      session.execute(String.format("DROP KEYSPACE %s;", keyspace));
    }
  }

  @Test
  public void consumeFromMultipleTablesInTabletsKeyspace() throws InterruptedException {
    String keyspace = "tabletsks_multi";
    String table1 = "tabletstest1";
    String table2 = "tabletstest2";
    Session session;

    try (Cluster cluster = Cluster.builder().addContactPoint(hostname).withPort(port).build()) {
      session = cluster.connect();

      // Create keyspace with tablets enabled
      session.execute(String.format("DROP KEYSPACE IF EXISTS %s;", keyspace));
      tryCreateKeyspace(session, String.format(
          "CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', "
              + "'replication_factor': 1} AND tablets = {'initial': 8};", keyspace));

      // Create two tables
      session.execute(String.format("DROP TABLE IF EXISTS %s.%s;", keyspace, table1));
      session.execute(String.format("DROP TABLE IF EXISTS %s.%s;", keyspace, table2));

      tryCreateTable(session,
          String.format(
              "CREATE TABLE %s.%s (id int, value text, PRIMARY KEY (id)) "
                  + "WITH cdc = {'enabled': 'true'};",
              keyspace, table1));

      tryCreateTable(session,
          String.format(
              "CREATE TABLE %s.%s (id int, name text, PRIMARY KEY (id)) "
                  + "WITH cdc = {'enabled': 'true'};",
              keyspace, table2));

      AtomicInteger changeCounter = new AtomicInteger(0);
      RawChangeConsumer changeConsumer =
          change -> {
            changeCounter.incrementAndGet();
            return CompletableFuture.completedFuture(null);
          };

      try (CDCConsumer consumer =
          CDCConsumer.builder()
              .addContactPoint(new InetSocketAddress(hostname, port))
              .addTable(new TableName(keyspace, table1))
              .addTable(new TableName(keyspace, table2))
              .withConsumer(changeConsumer)
              .withQueryTimeWindowSizeMs(10 * 1000)
              .withConfidenceWindowSizeMs(5 * 1000)
              .withWorkersCount(1)
              .build()) {

        consumer.start();

        // Perform inserts to both tables
        PreparedStatement ps1 = session.prepare(
            String.format("INSERT INTO %s.%s (id, value) VALUES (?, ?);", keyspace, table1));
        PreparedStatement ps2 = session.prepare(
            String.format("INSERT INTO %s.%s (id, name) VALUES (?, ?);", keyspace, table2));

        int changesPerTable = 5;
        int expectedTotalChanges = changesPerTable * 2;

        for (int i = 1; i <= changesPerTable; i++) {
          session.execute(ps1.bind(i, "value" + i));
          session.execute(ps2.bind(i, "name" + i));
        }

        // Wait for all changes to be consumed
        long timeoutMs = 60 * 1000;
        long startTime = System.currentTimeMillis();
        long pollIntervalMs = 500; // Check every 500ms

        while (changeCounter.get() < expectedTotalChanges &&
               (System.currentTimeMillis() - startTime) < timeoutMs) {
          Thread.sleep(pollIntervalMs);
        }

        // Verify we received all expected changes
        assertEquals(expectedTotalChanges, changeCounter.get(),
            "Expected to receive " + expectedTotalChanges + " changes but got " + changeCounter.get());
      }

      session.execute(String.format("DROP KEYSPACE %s;", keyspace));
    }
  }

  @Test
  public void consumeFromTabletsKeyspaceDuringTabletAlteration() throws InterruptedException {
    String keyspace = "tabletsks";
    String table = "tabletstest";
    Session session;

    try (Cluster cluster = Cluster.builder().addContactPoint(hostname).withPort(port).build()) {
      session = cluster.connect();

      // Create keyspace with tablets enabled
      session.execute(String.format("DROP KEYSPACE IF EXISTS %s;", keyspace));
      tryCreateKeyspace(session, String.format(
          "CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', "
              + "'replication_factor': 1} AND tablets = {'initial': 8};", keyspace));

      session.execute(String.format("DROP TABLE IF EXISTS %s.%s;", keyspace, table));
      tryCreateTable(session,
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
              .withWorkersCount(1)
              .build()) {

        consumer.start();

        // Start writing in a separate thread
        AtomicBoolean stopWriting = new AtomicBoolean(false);
        AtomicInteger totalWrites = new AtomicInteger(0);

        Thread writerThread = new Thread(() -> {
          PreparedStatement ps = session.prepare(
              String.format("INSERT INTO %s.%s (id, value) VALUES (?, ?);", keyspace, table));

          int id = 1;
          while (!stopWriting.get()) {
            try {
              session.execute(ps.bind(id, "value" + id));
              totalWrites.incrementAndGet();
              id++;
              Thread.sleep(100); // Write every 100ms
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              break;
            }
          }
        });

        writerThread.start();

        // Let some writes happen before altering tablets
        Thread.sleep(2000);

        // Get the most recent generation timestamp
        String generationQuery = String.format(
            "SELECT timestamp FROM system.cdc_timestamps WHERE keyspace_name='%s' AND table_name='%s' LIMIT 1;",
            keyspace, table);
        ResultSet rs = session.execute(generationQuery);
        Row row = rs.one();
        java.util.Date origGenerationTimestamp = row != null ? row.getTimestamp("timestamp") : null;

        // Alter tablet configuration
        session.execute(String.format(
            "ALTER TABLE %s.%s WITH tablets={'min_tablet_count':16};", keyspace, table));

        long timeoutMs = 300 * 1000;
        long startTime = System.currentTimeMillis();
        java.util.Date newGenerationTimestamp = origGenerationTimestamp;
        while (newGenerationTimestamp.equals(origGenerationTimestamp)) {
            rs = session.execute(generationQuery);
            row = rs.one();
            newGenerationTimestamp = row.getTimestamp("timestamp");
            if (System.currentTimeMillis() - startTime > timeoutMs) {
                break;
            }
            Thread.sleep(1000); // Check every second
        }
        // Verify that a new generation timestamp appeared
        assertEquals(false, newGenerationTimestamp == null || newGenerationTimestamp.equals(origGenerationTimestamp),
            "Expected a new generation timestamp after tablet alteration, but got: orig=" + origGenerationTimestamp + ", new=" + newGenerationTimestamp);

        // Continue writing until nodeTimestamp is greater than newGenerationTimestamp by a few seconds
        long continueWritingMs = 3000; // 3 seconds
        while (true) {
            ResultSet tsRs = session.execute("SELECT totimestamp(now()) FROM system.local;");
            Row tsRow = tsRs.one();
            java.util.Date nodeTimestamp = tsRow.getTimestamp(0);
            if (nodeTimestamp.getTime() > newGenerationTimestamp.getTime() + continueWritingMs) {
                break;
            } else {
                Thread.sleep(1000);
            }
        }

        // Stop the writer
        stopWriting.set(true);
        writerThread.join();

        int expectedChanges = totalWrites.get();

        // Wait for all changes to be consumed
        timeoutMs = 60 * 1000; // 60 seconds timeout
        startTime = System.currentTimeMillis();
        long pollIntervalMs = 500; // Check every 500ms

        while (changeCounter.get() < expectedChanges &&
               (System.currentTimeMillis() - startTime) < timeoutMs) {
          Thread.sleep(pollIntervalMs);
        }

        // Verify we received all expected changes
        assertEquals(expectedChanges, changeCounter.get(),
            "Expected to receive " + expectedChanges + " changes but got " + changeCounter.get());
      }

      session.execute(String.format("DROP KEYSPACE %s;", keyspace));
    }
  }

  public void tryCreateKeyspace(Session session, String query) {
      try {
          session.execute(query);
      } catch (Exception e) {
          if (e.getMessage().contains("Unknown property 'tablets'")) {
              abort("Test aborted: This version of Scylla doesn't support CDC with tablets. " +
                              "Error message: " + e.getMessage());
          }
          throw e;
      }
  }

  public void tryCreateTable(Session session, String query) throws InvalidQueryException {
      try {
          session.execute(query);
      } catch (InvalidQueryException e) {
          if (e.getMessage().contains("Cannot create CDC log for a table") &&
                  e.getMessage().contains("because keyspace uses tablets")) {
              abort("Test aborted: This version of Scylla doesn't support CDC with tablets. " +
                              "Error message: " + e.getMessage());
          }
          throw e;
      }
  }
}
