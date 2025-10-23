package com.scylladb.cdc.lib;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;
import com.google.common.flogger.FluentLogger;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scylladb.cdc.cql.driver3.Driver3Session;
import com.scylladb.cdc.cql.driver3.MockDriver3WorkerCQL;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.RawChangeConsumer;
import com.scylladb.cdc.model.worker.WorkerConfiguration;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
public class AlterTableIT {
  private static final FluentLogger log = FluentLogger.forEnclosingClass();
  Properties systemProperties = System.getProperties();
  String hostname =
      Preconditions.checkNotNull(systemProperties.getProperty("scylla.docker.hostname"));
  int port = Integer.parseInt(systemProperties.getProperty("scylla.docker.port"));
  String scyllaVersion =
      Preconditions.checkNotNull(systemProperties.getProperty("scylla.docker.version"));

  @Test
  public void alterBaseTableAtRuntime() {
    String keyspace = "testks";
    String table = "altertest";
    Session session;
    try (Cluster cluster = Cluster.builder().addContactPoint(hostname).withPort(port).build()) {
      session = cluster.connect();
      session.execute(
          String.format(
              "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', "
                  + "'replication_factor': 1};",
              keyspace));
      session.execute(String.format("DROP TABLE IF EXISTS %s.%s;", keyspace, table));
      session.execute(
          String.format(
              "CREATE TABLE IF NOT EXISTS %s.%s (column1 int, column2 int, column3 int, PRIMARY"
                  + " KEY (column1, column2)) WITH cdc = {'enabled': 'true'};",
              keyspace, table));

      AtomicInteger counter = new AtomicInteger(0);
      AtomicBoolean taskShouldStop = new AtomicBoolean(false);
      // Start continuously populating the base table in the background
      Runnable task =
          () -> {
            PreparedStatement ps =
                session.prepare(
                    String.format(
                        "INSERT INTO %s.%s (column1, column2, column3) VALUES (?, ?, ?);",
                        keyspace, table));
            while (!taskShouldStop.get()) {
              int current = counter.incrementAndGet();
              session.execute(ps.bind(current, current, current));
              Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            }
          };
      Thread thread = new Thread(task);
      thread.start();

      AtomicBoolean failedDueToInvalidTypeEx = new AtomicBoolean(false);
      AtomicReference<RawChange> firstChange, lastChange;
      firstChange = new AtomicReference<>(null);
      lastChange = new AtomicReference<>(null);
      RawChangeConsumer changeConsumer =
          change -> {
            try {
              firstChange.compareAndSet(null, change);
              lastChange.set(change);
              String toString =
                  change.toString(); // forces Driver3RawChange to go through all ColumnDefinitions
            } catch (Exception ex) {
              // Meant to catch InvalidTypeException, but it's shaded as part of driver3 and not
              // exposed.
              failedDueToInvalidTypeEx.set(true);
            }
            return CompletableFuture.completedFuture(null);
          };

      try (CDCConsumer consumer =
          CDCConsumer.builder()
              .addContactPoint(new InetSocketAddress(hostname, port))
              .addTable(new TableName(keyspace, table))
              .withConsumer(changeConsumer)
              .withQueryTimeWindowSizeMs(15 * 1000)
              .withConfidenceWindowSizeMs(10 * 1000)
              .withWorkersCount(1)
              .build()) {
        consumer.start();
        Thread.sleep(35 * 1000);
        session.execute(String.format("ALTER TABLE %s.%s " + "ADD column4 int;", keyspace, table));
        Thread.sleep(20 * 1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      taskShouldStop.set(true);
      try {
        thread.join();
      } catch (InterruptedException e) {
        throw new RuntimeException("Failed to join the background thread", e);
      }

      // Verify the schema of first and last RawChange
      RawChange sample = firstChange.get();
      assertEquals("cdc$stream_id", sample.getSchema().getColumnDefinition(0).getColumnName());
      assertEquals("cdc$time", sample.getSchema().getColumnDefinition(1).getColumnName());
      assertEquals("cdc$batch_seq_no", sample.getSchema().getColumnDefinition(2).getColumnName());
      assertEquals(
          "cdc$deleted_column3", sample.getSchema().getColumnDefinition(3).getColumnName());
      assertEquals("cdc$end_of_batch", sample.getSchema().getColumnDefinition(4).getColumnName());
      assertEquals("cdc$operation", sample.getSchema().getColumnDefinition(5).getColumnName());
      assertEquals("cdc$ttl", sample.getSchema().getColumnDefinition(6).getColumnName());
      assertEquals("column1", sample.getSchema().getColumnDefinition(7).getColumnName());
      assertEquals("column2", sample.getSchema().getColumnDefinition(8).getColumnName());
      assertEquals("column3", sample.getSchema().getColumnDefinition(9).getColumnName());

      sample = lastChange.get();
      assertEquals("cdc$stream_id", sample.getSchema().getColumnDefinition(0).getColumnName());
      assertEquals("cdc$time", sample.getSchema().getColumnDefinition(1).getColumnName());
      assertEquals("cdc$batch_seq_no", sample.getSchema().getColumnDefinition(2).getColumnName());
      assertEquals(
          "cdc$deleted_column3", sample.getSchema().getColumnDefinition(3).getColumnName());
      assertEquals(
          "cdc$deleted_column4", sample.getSchema().getColumnDefinition(4).getColumnName());
      assertEquals("cdc$end_of_batch", sample.getSchema().getColumnDefinition(5).getColumnName());
      assertEquals("cdc$operation", sample.getSchema().getColumnDefinition(6).getColumnName());
      assertEquals("cdc$ttl", sample.getSchema().getColumnDefinition(7).getColumnName());
      assertEquals("column1", sample.getSchema().getColumnDefinition(8).getColumnName());
      assertEquals("column2", sample.getSchema().getColumnDefinition(9).getColumnName());
      assertEquals("column3", sample.getSchema().getColumnDefinition(10).getColumnName());
      assertEquals("column4", sample.getSchema().getColumnDefinition(11).getColumnName());

      assertFalse(failedDueToInvalidTypeEx.get());
    }
  }

  private synchronized void printDetails(RawChange change) {
    List<ChangeSchema.ColumnDefinition> list = change.getSchema().getAllColumnDefinitions();
    System.out.println("RawChange details:");
    for (ChangeSchema.ColumnDefinition cdef : list) {
      StringBuilder sb = new StringBuilder();
      sb.append("column name: ")
          .append(cdef.getColumnName())
          .append(" column index: ")
          .append(cdef.getIndex())
          .append(" datatype: ")
          .append(cdef.getCdcLogDataType());
      if (cdef.isCdcColumn()) {
        sb.append(" basedatatype: isCdcColumn");
      } else {
        sb.append(" basedatatype: ").append(cdef.getBaseTableDataType());
      }
      System.out.println(sb);
    }
    System.out.println(change.getAsObject("column1"));
    System.out.flush();
  }

  @Test
  public void alterTableBeforeNextPage() {
    // A variation that alters the table right before the next page is fetched.
    String keyspace = "testks";
    String table = "alternextpage";
    Session session;
    int confidenceWindowSeconds = 5;
    try (Cluster cluster = Cluster.builder().addContactPoint(hostname).withPort(port).build()) {
      session = cluster.connect();
      session.execute(
          String.format(
              "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', "
                  + "'replication_factor': 1};",
              keyspace));
      session.execute(String.format("DROP TABLE IF EXISTS %s.%s;", keyspace, table));
      session.execute(
          String.format(
              "CREATE TABLE IF NOT EXISTS %s.%s (column1 int, column2 int, column3 int, PRIMARY"
                  + " KEY (column1, column2)) WITH cdc = {'enabled': 'true'};",
              keyspace, table));

      AtomicInteger counter = new AtomicInteger(0);
      AtomicBoolean taskShouldStop = new AtomicBoolean(false);
      // Start continuously populating the base table in the background
      Runnable task =
          () -> {
            PreparedStatement ps =
                session.prepare(
                    String.format(
                        "INSERT INTO %s.%s (column1, column2, column3) VALUES (?, ?, ?);",
                        keyspace, table));
            while (!taskShouldStop.get()) {
              int current = counter.incrementAndGet();
              session.execute(ps.bind(current, current, current));
              Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            }
          };
      Thread thread = new Thread(task);
      thread.start();

      AtomicBoolean failedDueToInvalidTypeEx = new AtomicBoolean(false);
      AtomicBoolean liftedProceedGate = new AtomicBoolean(false);
      AtomicReference<RawChange> firstChange, firstChangeAfterFetchingNewPage;
      firstChange = new AtomicReference<>(null);
      firstChangeAfterFetchingNewPage = new AtomicReference<>(null);
      RawChangeConsumer changeConsumer =
          change -> {
            try {
              firstChange.compareAndSet(null, change);
              if (liftedProceedGate.get()) {
                firstChangeAfterFetchingNewPage.compareAndSet(null, change);
              }
              String toString =
                  change.toString(); // forces Driver3RawChange to go through all ColumnDefinitions
            } catch (Exception ex) {
              // Meant to catch InvalidTypeException, but it's shaded as part of driver3 and not
              // exposed.
              failedDueToInvalidTypeEx.set(true);
            }
            return CompletableFuture.completedFuture(null);
          };

      try (CDCConsumer consumer =
               CDCConsumer.builder()
                   .addContactPoint(new InetSocketAddress(hostname, port))
                   .addTable(new TableName(keyspace, table))
                   .withConsumer(changeConsumer)
                   .withQueryTimeWindowSizeMs(15 * 1000)
                   .withConfidenceWindowSizeMs(confidenceWindowSeconds * 1000)
                   .withWorkersCount(1)
                   .withQueryOptionsFetchSize(1)
                   .build()) {
        Semaphore proceedGate = null;
        try {
          // Access the LocalTransport field in CDCConsumer
          java.lang.reflect.Field transportField = CDCConsumer.class.getDeclaredField("transport");
          transportField.setAccessible(true);
          LocalTransport transport = (LocalTransport) transportField.get(consumer);

          // Access the WorkerConfiguration.Builder field in LocalTransport
          java.lang.reflect.Field builderField = LocalTransport.class.getDeclaredField("workerConfigurationBuilder");
          builderField.setAccessible(true);
          WorkerConfiguration.Builder builder = (WorkerConfiguration.Builder) builderField.get(transport);

          // Access the session field in CDCConsumer
          java.lang.reflect.Field sessionField = CDCConsumer.class.getDeclaredField("session");
          sessionField.setAccessible(true);
          Driver3Session driver3Session = (Driver3Session) sessionField.get(consumer);

          // Create MockDriver3WorkerCQL and replace the cql field in the builder
          MockDriver3WorkerCQL mockWorkerCQL = new MockDriver3WorkerCQL(driver3Session);
          // Grab the Semaphore to control it later on
          proceedGate = mockWorkerCQL.getProceedGate();
          java.lang.reflect.Field cqlField = WorkerConfiguration.Builder.class.getDeclaredField("cql");
          cqlField.setAccessible(true);
          cqlField.set(builder, mockWorkerCQL);
        } catch (Exception e) {
          throw new RuntimeException("Failed to replace Driver3WorkerCQL with MockDriver3WorkerCQL", e);
        }
        // Let some records be generated first
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> {
          if (counter.get() > 15) {
            return true;
          }
          return false;
        });
        Uninterruptibles.sleepUninterruptibly(confidenceWindowSeconds, TimeUnit.SECONDS);
        consumer.start();
        Semaphore finalProceedGate = proceedGate;
        // Wait until something needs to iterate to the next page
        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(finalProceedGate::hasQueuedThreads);
        ResultSet rs = session.execute(String.format("ALTER TABLE %s.%s " + "ADD column4 int;", keyspace, table));
        Awaitility.await().atMost(15, TimeUnit.SECONDS).until(() ->
        {
          try {
            return session.getCluster().getMetadata().getKeyspace(keyspace).getTable(table).asCQLQuery().contains("column4");
          } catch (Exception e) {
          return false;
          }
        });
        liftedProceedGate.set(true);
        proceedGate.release(100);
        Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        log.atInfo().withCause(e).log("Caught InterruptedException, likely from CDCConsumer");
      }
      taskShouldStop.set(true);
      try {
        thread.join();
      } catch (InterruptedException e) {
        throw new RuntimeException("Failed to join the background thread", e);
      }

      // Verify the schema of first and firstNewPage RawChange
      RawChange sample = firstChange.get();
      assertEquals("cdc$stream_id", sample.getSchema().getColumnDefinition(0).getColumnName());
      assertEquals("cdc$time", sample.getSchema().getColumnDefinition(1).getColumnName());
      assertEquals("cdc$batch_seq_no", sample.getSchema().getColumnDefinition(2).getColumnName());
      assertEquals(
          "cdc$deleted_column3", sample.getSchema().getColumnDefinition(3).getColumnName());
      assertEquals("cdc$end_of_batch", sample.getSchema().getColumnDefinition(4).getColumnName());
      assertEquals("cdc$operation", sample.getSchema().getColumnDefinition(5).getColumnName());
      assertEquals("cdc$ttl", sample.getSchema().getColumnDefinition(6).getColumnName());
      assertEquals("column1", sample.getSchema().getColumnDefinition(7).getColumnName());
      assertEquals("column2", sample.getSchema().getColumnDefinition(8).getColumnName());
      assertEquals("column3", sample.getSchema().getColumnDefinition(9).getColumnName());

      sample = firstChangeAfterFetchingNewPage.get();
      assertEquals("cdc$stream_id", sample.getSchema().getColumnDefinition(0).getColumnName());
      assertEquals("cdc$time", sample.getSchema().getColumnDefinition(1).getColumnName());
      assertEquals("cdc$batch_seq_no", sample.getSchema().getColumnDefinition(2).getColumnName());
      assertEquals(
          "cdc$deleted_column3", sample.getSchema().getColumnDefinition(3).getColumnName());
      assertEquals(
          "cdc$deleted_column4", sample.getSchema().getColumnDefinition(4).getColumnName());
      assertEquals("cdc$end_of_batch", sample.getSchema().getColumnDefinition(5).getColumnName());
      assertEquals("cdc$operation", sample.getSchema().getColumnDefinition(6).getColumnName());
      assertEquals("cdc$ttl", sample.getSchema().getColumnDefinition(7).getColumnName());
      assertEquals("column1", sample.getSchema().getColumnDefinition(8).getColumnName());
      assertEquals("column2", sample.getSchema().getColumnDefinition(9).getColumnName());
      assertEquals("column3", sample.getSchema().getColumnDefinition(10).getColumnName());
      assertEquals("column4", sample.getSchema().getColumnDefinition(11).getColumnName());

      assertFalse(failedDueToInvalidTypeEx.get());
    }
  }
}
