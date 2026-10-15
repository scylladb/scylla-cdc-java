package com.scylladb.cdc.lib;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.abort;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.google.common.base.Preconditions;
import com.scylladb.cdc.cql.WorkerCQL;
import com.scylladb.cdc.cql.driver3.Driver3WorkerCQL;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.RawChangeConsumer;
import com.scylladb.cdc.model.worker.Task;
import com.scylladb.cdc.model.worker.TaskAndRawChangeConsumer;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
@Tag("tablets")
public class TabletsIT {
  Properties systemProperties = System.getProperties();
  String hostname =
      Preconditions.checkNotNull(systemProperties.getProperty("scylla.docker.hostname"));
  int port = Integer.parseInt(systemProperties.getProperty("scylla.docker.port"));
  String scyllaVersion =
      Preconditions.checkNotNull(systemProperties.getProperty("scylla.docker.version"));

  @BeforeEach
  public void checkTabletsCdcSupport() {
    try (Cluster cluster = Cluster.builder().addContactPoint(hostname).withPort(port).build();
         Session session = cluster.connect()) {
      // Drop probe keyspace in case it was left from a prior interrupted run.
        session.execute("DROP KEYSPACE IF EXISTS cdc_tablets_probe;");

      // Gate 1: skip if this ScyllaDB version doesn't support the 'tablets' property at all.
      try {
        session.execute("CREATE KEYSPACE cdc_tablets_probe " +
            "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} " +
            "AND tablets = {'initial': 2};");
      } catch (Exception e) {
        if (e.getMessage().contains("Unknown property 'tablets'")) {
          abort("Test skipped: this ScyllaDB version does not support the tablets property. " +
              "Error: " + e.getMessage());
        }
        throw e;
      }

      // Gate 2: skip if this ScyllaDB version doesn't support CDC on tablets keyspaces.
      try {
        session.execute(
            "CREATE TABLE cdc_tablets_probe.t (pk int PRIMARY KEY) WITH cdc = {'enabled': true};");
      } catch (InvalidQueryException e) {
        if (e.getMessage().contains("Cannot create CDC log for a table") &&
            e.getMessage().contains("because keyspace uses tablets")) {
          abort("Test skipped: this ScyllaDB version does not support CDC with tablets. " +
              "Error: " + e.getMessage());
        }
        throw e;
      } finally {
      session.execute("DROP KEYSPACE IF EXISTS cdc_tablets_probe;");
      }
    }
  }

  @Test
  public void consumeFromTabletsKeyspace() throws InterruptedException {
    String keyspace = "tabletsks";
    String table = "tabletstest";
    Session session;

    try (Cluster cluster = Cluster.builder().addContactPoint(hostname).withPort(port).build()) {
      session = cluster.connect();

      // Create keyspace with tablets enabled
      session.execute(String.format("DROP KEYSPACE IF EXISTS %s;", keyspace));
      session.execute(String.format(
          "CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', "
              + "'replication_factor': 1} AND tablets = {'initial': 8};", keyspace));

      session.execute(String.format("DROP TABLE IF EXISTS %s.%s;", keyspace, table));
      session.execute(String.format(
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
      session.execute(String.format(
          "CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', "
              + "'replication_factor': 1} AND tablets = {'initial': 8};", keyspace));

      // Create two tables
      session.execute(String.format("DROP TABLE IF EXISTS %s.%s;", keyspace, table1));
      session.execute(String.format("DROP TABLE IF EXISTS %s.%s;", keyspace, table2));

      session.execute(String.format(
              "CREATE TABLE %s.%s (id int, value text, PRIMARY KEY (id)) "
                  + "WITH cdc = {'enabled': 'true'};",
              keyspace, table1));

      session.execute(String.format(
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
  public void consumeFromManyTablesAddedWhileTasksActive() throws InterruptedException {
    String keyspace = "tabletsks_many";
    // The master adds tables one by one; 26 crosses the default HashMap resizes
    // (13th and 25th put) while earlier tables' tasks call createReader.
    int tableCount = 26;
    Session session;

    try (Cluster cluster = Cluster.builder().addContactPoint(hostname).withPort(port).build()) {
      session = cluster.connect();

      session.execute(String.format("DROP KEYSPACE IF EXISTS %s;", keyspace));
      session.execute(String.format(
          "CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', "
              + "'replication_factor': 1} AND tablets = {'initial': 2};", keyspace));

      for (int t = 0; t < tableCount; t++) {
        session.execute(String.format(
            "CREATE TABLE %s.t%d (id int, value text, PRIMARY KEY (id)) "
                + "WITH cdc = {'enabled': 'true'};",
            keyspace, t));
      }

      AtomicInteger changeCounter = new AtomicInteger(0);
      RawChangeConsumer changeConsumer =
          change -> {
            changeCounter.incrementAndGet();
            return CompletableFuture.completedFuture(null);
          };
      AtomicInteger createReaderFailures = new AtomicInteger(0);

      CDCConsumer.Builder builder =
          CDCConsumer.builder()
              .addContactPoint(new InetSocketAddress(hostname, port))
              .withConsumer(changeConsumer)
              .withWorkerCQLProvider(driverSession -> new FailureCountingWorkerCQL(
                  new Driver3WorkerCQL(driverSession), createReaderFailures))
              .withQueryTimeWindowSizeMs(10 * 1000)
              .withConfidenceWindowSizeMs(5 * 1000)
              .withWorkersCount(1);
      for (int t = 0; t < tableCount; t++) {
        builder.addTable(new TableName(keyspace, "t" + t));
      }

      try (CDCConsumer consumer = builder.build()) {
        consumer.start();

        for (int t = 0; t < tableCount; t++) {
          session.execute(String.format(
              "INSERT INTO %s.t%d (id, value) VALUES (1, 'value');", keyspace, t));
        }

        // Wait for all changes to be consumed
        long timeoutMs = 60 * 1000;
        long startTime = System.currentTimeMillis();
        long pollIntervalMs = 500; // Check every 500ms

        while (changeCounter.get() < tableCount &&
               (System.currentTimeMillis() - startTime) < timeoutMs) {
          Thread.sleep(pollIntervalMs);
        }

        assertEquals(tableCount, changeCounter.get(),
            "Expected to receive " + tableCount + " changes but got " + changeCounter.get());
        // Retries still deliver every change, so only this catches a missed cache lookup.
        assertEquals(0, createReaderFailures.get(),
            "createReader failed while later tables were being prepared");
      }

      session.execute(String.format("DROP KEYSPACE %s;", keyspace));
    }
  }

  private static final class FailureCountingWorkerCQL implements WorkerCQL {
    private final WorkerCQL delegate;
    private final AtomicInteger createReaderFailures;

    FailureCountingWorkerCQL(WorkerCQL delegate, AtomicInteger createReaderFailures) {
      this.delegate = delegate;
      this.createReaderFailures = createReaderFailures;
    }

    @Override
    public void prepare(Set<TableName> tables) throws InterruptedException, ExecutionException {
      delegate.prepare(tables);
    }

    @Override
    public CompletableFuture<Reader> createReader(Task task) {
      try {
        return delegate.createReader(task);
      } catch (RuntimeException e) {
        createReaderFailures.incrementAndGet();
        throw e;
      }
    }

    @Override
    public CompletableFuture<Optional<Long>> fetchTableTTL(TableName tableName) {
      return delegate.fetchTableTTL(tableName);
    }

    @Override
    public boolean isNoisyException(Throwable exception) {
      return delegate.isNoisyException(exception);
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
      session.execute(String.format(
          "CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', "
              + "'replication_factor': 1} AND tablets = {'initial': 8};", keyspace));

      session.execute(String.format("DROP TABLE IF EXISTS %s.%s;", keyspace, table));
      session.execute(String.format(
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

  @Test
  public void consumeTabletStreamsInParallel() throws Exception {
    String keyspace = "tabletsks_parallel";
    String table = "tabletstest";

    try (Cluster cluster = Cluster.builder().addContactPoint(hostname).withPort(port).build()) {
      Session session = cluster.connect();

      session.execute(String.format("DROP KEYSPACE IF EXISTS %s;", keyspace));
      session.execute(String.format(
          "CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', "
              + "'replication_factor': 1} AND tablets = {'initial': 8};", keyspace));
      session.execute(String.format(
              "CREATE TABLE %s.%s (id int, value text, PRIMARY KEY (id)) "
                  + "WITH cdc = {'enabled': 'true'};",
              keyspace, table));

      // The first change is held open until a change from a different task arrives. With all
      // tablet streams in one task, nothing else can be delivered meanwhile and the hold times
      // out; with one task per stream, another task delivers while the first is still pending.
      // The hold is a future, not a blocked thread, so no driver or worker thread is parked.
      Set<TaskId> seenTasks = ConcurrentHashMap.newKeySet();
      // Delivery is at-least-once, so count distinct rows rather than deliveries.
      Set<Integer> seenIds = ConcurrentHashMap.newKeySet();
      AtomicBoolean multiStreamTask = new AtomicBoolean(false);
      AtomicReference<TaskId> heldTask = new AtomicReference<>();
      CompletableFuture<Void> release = new CompletableFuture<>();
      AtomicBoolean overlapped = new AtomicBoolean(false);
      TaskAndRawChangeConsumer changeConsumer =
          (task, change) -> {
            seenTasks.add(task.id);
            seenIds.add(change.getCell("id").getInt());
            if (task.streams.size() != 1) {
              multiStreamTask.set(true);
            }
            if (release.isDone()) {
              return CompletableFuture.completedFuture(null);
            }
            if (heldTask.compareAndSet(null, task.id)) {
              return release.completeOnTimeout(null, 30, TimeUnit.SECONDS);
            }
            if (!heldTask.get().equals(task.id)) {
              overlapped.set(true);
              release.complete(null);
            }
            return CompletableFuture.completedFuture(null);
          };

      // A resource rather than a finally block: if the drop fails, its exception is suppressed
      // onto the assertion failure instead of replacing it.
      try (AutoCloseable dropKeyspace = () ->
               session.execute(String.format("DROP KEYSPACE IF EXISTS %s;", keyspace));
           CDCConsumer consumer =
          CDCConsumer.builder()
              .addContactPoint(new InetSocketAddress(hostname, port))
              .addTable(new TableName(keyspace, table))
              .withTaskAndRawChangeConsumer(changeConsumer)
              .withQueryTimeWindowSizeMs(10 * 1000)
              .withConfidenceWindowSizeMs(5 * 1000)
              .withWorkersCount(4)
              .build()) {

        consumer.start();

        PreparedStatement ps = session.prepare(
            String.format("INSERT INTO %s.%s (id, value) VALUES (?, ?);", keyspace, table));

        // Enough partitions to very likely land in every tablet.
        int expectedChanges = 200;
        for (int i = 1; i <= expectedChanges; i++) {
          session.execute(ps.bind(i, "value" + i));
        }

        long timeoutMs = 90 * 1000;
        long startTime = System.currentTimeMillis();
        long pollIntervalMs = 500;

        while (seenIds.size() < expectedChanges &&
               (System.currentTimeMillis() - startTime) < timeoutMs) {
          Thread.sleep(pollIntervalMs);
        }

        assertEquals(expectedChanges, seenIds.size(),
            "Expected to receive " + expectedChanges + " rows but got " + seenIds.size());
        // Overlap needs two distinct tasks, so this also covers "more than one task".
        assertTrue(overlapped.get(),
            "Expected a second task to deliver while the first was held; tasks seen: "
                + seenTasks);
        assertTrue(seenTasks.stream().allMatch(TaskId::isTabletStreamTask),
            "Expected only per-stream tablet tasks but got " + seenTasks);
        assertFalse(multiStreamTask.get(),
            "Expected every tablet task to own exactly one stream");
      }
    }
  }

}
