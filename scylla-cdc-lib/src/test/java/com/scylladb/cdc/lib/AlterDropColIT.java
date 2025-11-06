package com.scylladb.cdc.lib;

import com.datastax.driver.core.PreparedStatement;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scylladb.cdc.model.worker.RawChange;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("integration")
public class AlterDropColIT extends AlterTableBase {
  @Override
  public String testKeyspace() {
    return "AlterDropColIT".toLowerCase();
  }

  @Override
  public String testTable() {
    return "test";
  }

  @Override
  public String createTableQuery() {
    return String.format(
        "CREATE TABLE %s.%s (column1 int, column2 int, column3 int, column4 int, PRIMARY"
            + " KEY (column1, column2)) WITH cdc = {'enabled': 'true'};",
        testKeyspace(), testTable());
  }

  @Override
  public void applyAlteration() {
    getDriverSession().execute(String.format("ALTER TABLE %s.%s " + "DROP column4;", testKeyspace(), testTable()));
  }


  @Override
  public void verifyRawChangeBeforeAlter(RawChange change) {
    assertEquals("cdc$stream_id", change.getSchema().getColumnDefinition(0).getColumnName());
    assertEquals("cdc$time", change.getSchema().getColumnDefinition(1).getColumnName());
    assertEquals("cdc$batch_seq_no", change.getSchema().getColumnDefinition(2).getColumnName());
    assertEquals(
        "cdc$deleted_column3", change.getSchema().getColumnDefinition(3).getColumnName());
    assertEquals(
        "cdc$deleted_column4", change.getSchema().getColumnDefinition(4).getColumnName());
    assertEquals("cdc$end_of_batch", change.getSchema().getColumnDefinition(5).getColumnName());
    assertEquals("cdc$operation", change.getSchema().getColumnDefinition(6).getColumnName());
    assertEquals("cdc$ttl", change.getSchema().getColumnDefinition(7).getColumnName());
    assertEquals("column1", change.getSchema().getColumnDefinition(8).getColumnName());
    assertEquals("column2", change.getSchema().getColumnDefinition(9).getColumnName());
    assertEquals("column3", change.getSchema().getColumnDefinition(10).getColumnName());
    assertEquals("column4", change.getSchema().getColumnDefinition(11).getColumnName());
  }

  @Override
  public void verifyRawChangeAfterAlter(RawChange change) {
    assertEquals("cdc$stream_id", change.getSchema().getColumnDefinition(0).getColumnName());
    assertEquals("cdc$time", change.getSchema().getColumnDefinition(1).getColumnName());
    assertEquals("cdc$batch_seq_no", change.getSchema().getColumnDefinition(2).getColumnName());
    assertEquals(
        "cdc$deleted_column3", change.getSchema().getColumnDefinition(3).getColumnName());
    assertEquals("cdc$end_of_batch", change.getSchema().getColumnDefinition(4).getColumnName());
    assertEquals("cdc$operation", change.getSchema().getColumnDefinition(5).getColumnName());
    assertEquals("cdc$ttl", change.getSchema().getColumnDefinition(6).getColumnName());
    assertEquals("column1", change.getSchema().getColumnDefinition(7).getColumnName());
    assertEquals("column2", change.getSchema().getColumnDefinition(8).getColumnName());
    assertEquals("column3", change.getSchema().getColumnDefinition(9).getColumnName());
  }

  @Override
  public Runnable createDatagenTask() {
    return () -> {
      PreparedStatement psBeforeAlter = null;
      PreparedStatement psAfterAlter = null;
      while (!datagenShouldStop.get()) {
        int current = datagenCounter.incrementAndGet();
        if (!isAfterAlter.get()) {
          if (psBeforeAlter == null) {
            psBeforeAlter = getDriverSession().prepare(
                String.format(
                    "INSERT INTO %s.%s (column1, column2, column3, column4) VALUES (?, ?, ?, ?);",
                    testKeyspace(), testTable()));
          }
          getDriverSession().execute(psBeforeAlter.bind(1, 1, current, current));
        } else {
          if (psAfterAlter == null) {
            psAfterAlter = getDriverSession().prepare(
                String.format(
                    "INSERT INTO %s.%s (column1, column2, column3) VALUES (?, ?, ?);",
                    testKeyspace(), testTable()));
          }
          getDriverSession().execute(psAfterAlter.bind(1, 1, current));
        }
        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      }
    };
  }

  @Test
  public void alterBeforeNextPageTestBody() {
    super.alterBeforeNextPageTestBody();
  }
}
