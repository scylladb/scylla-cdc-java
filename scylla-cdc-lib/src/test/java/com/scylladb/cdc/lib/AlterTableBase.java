package com.scylladb.cdc.lib;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;
import com.google.common.flogger.FluentLogger;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scylladb.cdc.cql.driver3.MockDriver3WorkerCQL;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.RawChangeConsumer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class AlterTableBase {
  protected static final FluentLogger log = FluentLogger.forEnclosingClass();
  protected Properties systemProperties = System.getProperties();
  protected String hostname = Preconditions.checkNotNull(systemProperties.getProperty("scylla.docker.hostname"));
  protected int port = Integer.parseInt(systemProperties.getProperty("scylla.docker.port"));
  protected String scyllaVersion = Preconditions.checkNotNull(systemProperties.getProperty("scylla.docker.version"));

  private static Session driverSession;
  private static Cluster cluster;

  public abstract String testKeyspace();

  public abstract String testTable();

  public void wipeKeyspace() {
    Session session = getDriverSession();
    String dropKeyspaceQuery = String.format("DROP KEYSPACE IF EXISTS %s;", testKeyspace());
    session.execute(dropKeyspaceQuery);
  }

  public String createKeyspaceQuery() {
    return String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', " + "'replication_factor" +
        "': 1};", testKeyspace());
  }

  public abstract String createTableQuery();

  public abstract void applyAlteration();

  public abstract void verifyRawChangeBeforeAlter(RawChange change);

  public abstract void verifyRawChangeAfterAlter(RawChange change);

  public abstract Runnable createDatagenTask();

  protected AtomicBoolean isAfterAlter;
  protected AtomicBoolean consumedChangeAfterAlter;
  protected AtomicLong lastConsumedTime;
  protected AtomicBoolean datagenShouldStop;
  protected AtomicInteger datagenCounter;
  protected List<RawChange> rawChangesBeforeAlter;
  protected List<RawChange> rawChangesAfterAlter;

  public RawChangeConsumer createChangeConsumer() {
    return change -> {
      lastConsumedTime.set(System.currentTimeMillis());
      if (!isAfterAlter.get()) {
        rawChangesBeforeAlter.add(change);
      } else {
        consumedChangeAfterAlter.set(true);
        rawChangesAfterAlter.add(change);
      }
      return CompletableFuture.completedFuture(null);
    };
  }

  protected int confidenceWindowSeconds = 15;
  protected ReentrantReadWriteLock nextPageLock;
  protected ReentrantReadWriteLock nextRowLock;

  public CDCConsumer.Builder defaultCDCConsumerBuilder() {
    return CDCConsumer.builder()
        .addContactPoint(new InetSocketAddress(hostname, port))
        .addTable(new TableName(testKeyspace(), testTable()))
        .withConsumer(createChangeConsumer())
        .withQueryTimeWindowSizeMs(15 * 1000)
        .withConfidenceWindowSizeMs(confidenceWindowSeconds * 1000)
        .withWorkersCount(1)
        .withWorkerCQLProvider((driver3Session) -> new MockDriver3WorkerCQL(driver3Session, nextPageLock, nextRowLock));
  }

  public Session getDriverSession() {
    if (cluster == null || cluster.isClosed()) {
      cluster = Cluster.builder().addContactPoint(hostname).withPort(port).build();
    }
    if (driverSession == null || driverSession.isClosed()) {
      driverSession = cluster.connect();
    }
    return driverSession;
  }

  protected void clearSharedVariables() {
    isAfterAlter = new AtomicBoolean(false);
    consumedChangeAfterAlter = new AtomicBoolean(false);
    lastConsumedTime = new AtomicLong(0);
    datagenShouldStop = new AtomicBoolean(false);
    datagenCounter = new AtomicInteger(0);
    rawChangesBeforeAlter = new ArrayList<>();
    rawChangesAfterAlter = new ArrayList<>();
    confidenceWindowSeconds = 15;
    nextPageLock = new ReentrantReadWriteLock();
    nextRowLock = new ReentrantReadWriteLock();
  }

  protected void createKeyspaceAndTable() {
    wipeKeyspace();
    getDriverSession().execute(createKeyspaceQuery());
    getDriverSession().execute(createTableQuery());
  }

  public void alterBeforeNextPageTestBody() {
    clearSharedVariables();
    createKeyspaceAndTable();
    Thread datagenThread = new Thread(createDatagenTask());
    datagenThread.setName("DatagenThread");
    datagenThread.start();

    try (CDCConsumer consumer = defaultCDCConsumerBuilder().withQueryOptionsFetchSize(1).build()) {
      Uninterruptibles.sleepUninterruptibly(confidenceWindowSeconds, TimeUnit.SECONDS);
      Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> {
        if (datagenCounter.get() > 15) {
          return true;
        }
        return false;
      });
      nextPageLock.writeLock().lock();
      consumer.start();
      Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> nextPageLock.hasQueuedThreads() && (System.currentTimeMillis() - lastConsumedTime.get() > 10000));
      applyAlteration();
      isAfterAlter.set(true);
      nextPageLock.writeLock().unlock();
      Awaitility.await().atMost(125, TimeUnit.SECONDS).until(consumedChangeAfterAlter::get);
      Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);
      consumer.stop();
    } catch (InterruptedException e) {
      log.atInfo().withCause(e).log("Caught InterruptedException");
    }

    datagenShouldStop.set(true);
    try {
      datagenThread.join(5000);
    } catch (InterruptedException e) {
      throw new RuntimeException("Failed to join the datagen thread", e);
    }

    verifyAllRawChanges();
  }

  public void alterBeforeNextRowTestBody() {
    clearSharedVariables();
    createKeyspaceAndTable();
    Thread datagenThread = new Thread(createDatagenTask());
    datagenThread.start();

    try (CDCConsumer consumer = defaultCDCConsumerBuilder().build()) {
      Uninterruptibles.sleepUninterruptibly(confidenceWindowSeconds, TimeUnit.SECONDS);
      Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> {
        if (datagenCounter.get() > 15) {
          return true;
        }
        return false;
      });
      consumer.start();
      // Let consumer reach the first RawChange. Otherwise, everything will lock at the very start.
      Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> (System.currentTimeMillis() - lastConsumedTime.get() < 10000));
      nextRowLock.writeLock().lock();
      Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> nextRowLock.hasQueuedThreads() && (System.currentTimeMillis() - lastConsumedTime.get() > 10000));
      applyAlteration();
      isAfterAlter.set(true);
      nextRowLock.writeLock().unlock();
      Awaitility.await().atMost(125, TimeUnit.SECONDS).until(consumedChangeAfterAlter::get);
      Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);
      consumer.stop();
    } catch (InterruptedException e) {
      log.atInfo().withCause(e).log("Caught InterruptedException");
    }
    datagenShouldStop.set(true);
    try {
      datagenThread.join(5000);
    } catch (InterruptedException e) {
      throw new RuntimeException("Failed to join the datagen thread", e);
    }

    verifyAllRawChanges();
  }

  protected void verifyAllRawChanges() {
    for (RawChange c : rawChangesBeforeAlter) {
      c.toString(); // forces Driver3RawChange to go through all ColumnDefinitions
      verifyRawChangeBeforeAlter(c);
    }
    for (RawChange c : rawChangesAfterAlter) {
      c.toString();
      verifyRawChangeAfterAlter(c);
    }
  }

  @AfterAll
  public static void closeSession() {
    if (driverSession != null && !driverSession.isClosed()) {
      driverSession.close();
    }
    if (cluster != null && !cluster.isClosed()) {
      cluster.close();
    }
  }

  protected synchronized void printDetails(RawChange change) {
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

}
