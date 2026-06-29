package com.scylladb.cdc.lib;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.RawChange;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("integration")
public class AlterUpdateUdtIT extends AlterTableBase {

  @Override
  public String testKeyspace() {
    return "AlterUpdateUdtIT".toLowerCase();
  }

  @Override
  protected void createKeyspaceAndTable() {
    wipeKeyspace();
    TestKeyspaceUtils.createWithoutTablets(getDriverSession(), testKeyspace());
    getDriverSession().execute(createUdtQuery());
    getDriverSession().execute(createTableQuery());
  }

  private String createUdtQuery() {
    return String.format(
        "CREATE TYPE IF NOT EXISTS %s.simple_udt (a int, b text);",
        testKeyspace());
  }

  @Override
  public String createTableQuery() {
    return String.format(
        "CREATE TABLE %s.%s (column1 int, column2 int, column3 simple_udt, PRIMARY KEY (column1, column2)) WITH cdc = {'enabled': 'true'};",
        testKeyspace(), testTable());
  }

  @Override
  public void applyAlteration() {
    getDriverSession().execute(String.format(
        "ALTER TYPE %s.simple_udt ADD c int;",
        testKeyspace()));
  }


  @Override
  public void verifyRawChangeBeforeAlter(RawChange change) {
    assertEquals("cdc$stream_id", change.getSchema().getColumnDefinition(0).getColumnName());
    assertEquals("cdc$time", change.getSchema().getColumnDefinition(1).getColumnName());
    assertEquals("cdc$batch_seq_no", change.getSchema().getColumnDefinition(2).getColumnName());
    assertEquals("cdc$deleted_column3", change.getSchema().getColumnDefinition(3).getColumnName());
    assertEquals("cdc$deleted_elements_column3", change.getSchema().getColumnDefinition(4).getColumnName());
    assertEquals("cdc$end_of_batch", change.getSchema().getColumnDefinition(5).getColumnName());
    assertEquals("cdc$operation", change.getSchema().getColumnDefinition(6).getColumnName());
    assertEquals("cdc$ttl", change.getSchema().getColumnDefinition(7).getColumnName());
    assertEquals("column1", change.getSchema().getColumnDefinition(8).getColumnName());
    assertEquals("column2", change.getSchema().getColumnDefinition(9).getColumnName());
    assertEquals("column3", change.getSchema().getColumnDefinition(10).getColumnName());
    assertEquals(ChangeSchema.CqlType.UDT, change.getSchema().getColumnDefinition(10).getBaseTableDataType().getCqlType());
    assertEquals("UDT(alterupdateudtit.simple_udt){a INT, b VARCHAR}", change.getSchema().getColumnDefinition(10).getBaseTableDataType().toString());
    assertEquals(
        "FROZEN<UDT(alterupdateudtit.simple_udt){a INT, b VARCHAR}>"
        , change.getSchema().getColumnDefinition(10).getCdcLogDataType().toString()
    );
  }

  @Override
  public void verifyRawChangeAfterAlter(RawChange change) {

    assertEquals("cdc$stream_id", change.getSchema().getColumnDefinition(0).getColumnName());
    assertEquals("cdc$time", change.getSchema().getColumnDefinition(1).getColumnName());
    assertEquals("cdc$batch_seq_no", change.getSchema().getColumnDefinition(2).getColumnName());
    assertEquals("cdc$deleted_column3", change.getSchema().getColumnDefinition(3).getColumnName());
    assertEquals("cdc$deleted_elements_column3", change.getSchema().getColumnDefinition(4).getColumnName());
    assertEquals("cdc$end_of_batch", change.getSchema().getColumnDefinition(5).getColumnName());
    assertEquals("cdc$operation", change.getSchema().getColumnDefinition(6).getColumnName());
    assertEquals("cdc$ttl", change.getSchema().getColumnDefinition(7).getColumnName());
    assertEquals("column1", change.getSchema().getColumnDefinition(8).getColumnName());
    assertEquals("column2", change.getSchema().getColumnDefinition(9).getColumnName());
    assertEquals("column3", change.getSchema().getColumnDefinition(10).getColumnName());
    assertEquals(ChangeSchema.CqlType.UDT, change.getSchema().getColumnDefinition(10).getBaseTableDataType().getCqlType());
    assertEquals("UDT(alterupdateudtit.simple_udt){a INT, b VARCHAR, c INT}", change.getSchema().getColumnDefinition(10).getBaseTableDataType().toString());
    assertEquals(
        "FROZEN<UDT(alterupdateudtit.simple_udt){a INT, b VARCHAR, c INT}>"
        , change.getSchema().getColumnDefinition(10).getCdcLogDataType().toString()
    );
  }

  @Override
  public Runnable createDatagenTask() {
    return () -> {
      while (!datagenShouldStop.get()) {
        int current = datagenCounter.incrementAndGet();
        if (!isAfterAlter.get()) {
          // Before alter: UDT has fields a int, b text
          getDriverSession().execute(String.format(
              "INSERT INTO %s.%s (column1, column2, column3) VALUES (%d, %d, {a: %d, b: 'val%d'});",
              testKeyspace(), testTable(), 1, 1, current, current));
        } else {
          // After alter: UDT gains field c int
          getDriverSession().execute(String.format(
              "INSERT INTO %s.%s (column1, column2, column3) VALUES (%d, %d, {a: %d, b: 'val%d', c: %d});",
              testKeyspace(), testTable(), 1, 1, current, current, current * 10));
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
