package com.scylladb.cdc.cql.driver3;

import com.datastax.driver.core.PreparedStatement;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.worker.RawChange;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
public class Driver3TabletsIntegrationIT extends BaseScyllaTabletsIntegrationTest {

    @Test
    public void testChangeInTabletsMode() throws ExecutionException, InterruptedException, TimeoutException {
        tryCreateTable("CREATE TABLE ks.test(pk int, ck int, v text, PRIMARY KEY(pk, ck)) " +
                "WITH cdc = {'enabled': true}");

        // Insert a test row
        final int pk = 1;
        final int ck = 2;
        final String value = "test_value_" + UUID.randomUUID();

        PreparedStatement insertStatement = driverSession.prepare(
                "INSERT INTO ks.test (pk, ck, v) VALUES (?, ?, ?)");
        driverSession.execute(insertStatement.bind(pk, ck, value));

        // Get the change from the CDC log
        RawChange change = getFirstRawChange(new TableName("ks", "test"));

        // Verify the change
        assertNotNull(change, "CDC change should not be null");
        assertEquals("ROW_INSERT", change.getOperationType().toString(),
                "Operation type should be ROW_INSERT");

        // Verify the column values
        Object pkValue = change.getAsObject("pk");
        Object ckValue = change.getAsObject("ck");
        Object valueObj = change.getAsObject("v");

        assertEquals(pk, pkValue, "pk column value should match");
        assertEquals(ck, ckValue, "ck column value should match");
        assertEquals(value, valueObj, "v column value should match");
    }
}
