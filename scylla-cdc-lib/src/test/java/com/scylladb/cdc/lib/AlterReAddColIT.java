package com.scylladb.cdc.lib;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.RawChange;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

// DROP followed by ADD with different type
@Tag("integration")
public class AlterReAddColIT extends AlterTableBase {
  @Override
  public String testKeyspace() {
    return "AlterReAddColIT".toLowerCase();
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
    Uninterruptibles.sleepUninterruptibly(6, TimeUnit.SECONDS);
    getDriverSession().execute(String.format("ALTER TABLE %s.%s " + "ADD column4 text;", testKeyspace(), testTable()));
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
    assertEquals(ChangeSchema.CqlType.INT, change.getSchema().getColumnDefinition(11).getBaseTableDataType().getCqlType());
  }

  @Override
  public void verifyRawChangeAfterAlter(RawChange change) {
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
    assertEquals(ChangeSchema.CqlType.VARCHAR, change.getSchema().getColumnDefinition(11).getBaseTableDataType().getCqlType());
  }

  @Override
  public Runnable createDatagenTask() {
    return () -> {
      while (!datagenShouldStop.get()) {
        int current = datagenCounter.incrementAndGet();
        try {
          if (!isAfterAlter.get()) {
            getDriverSession().execute(String.format(
                "INSERT INTO %s.%s (column1, column2, column3, column4) VALUES (%d, %d, %d, %d);",
                testKeyspace(), testTable(), 1, 1, current, current));
          } else {
            getDriverSession().execute(String.format(
                "INSERT INTO %s.%s (column1, column2, column3, column4) VALUES (%d, %d, %d, '%s');",
                testKeyspace(), testTable(), 1, 1, current, Integer.toString(current)));
          }
        } catch (Exception e) {
          // It is possible to send a query right when column is being dropped or re-added.
          // In such case the exception here should not fail the test.
          log.atWarning().withCause(e).log("Datagen task exception encountered");
        }
        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      }
    };
  }

  @Test
  public void alterBeforeNextPageTestBody(TestInfo testInfo) {
    setTestTableName(testInfo.getTestMethod().get().getName().toLowerCase());
    super.alterBeforeNextPageTestBody();
  }
}
