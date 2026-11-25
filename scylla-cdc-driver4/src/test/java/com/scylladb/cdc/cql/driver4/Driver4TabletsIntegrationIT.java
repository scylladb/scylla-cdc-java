package com.scylladb.cdc.cql.driver4;

import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.worker.RawChange;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("integration")
@Tag("tablets")
public class Driver4TabletsIntegrationIT extends BaseScyllaTabletsIntegrationTest {
    @Test
    public void testChangeInTabletsMode() throws Exception {
        driverSession.execute("CREATE TABLE ks.test(pk int, ck int, v text, PRIMARY KEY(pk, ck)) "
                + "WITH cdc = {'enabled': true}");

        final int pk = 1;
        final int ck = 2;
        final String value = "test_value_" + UUID.randomUUID();
        PreparedStatement insertStatement = driverSession.prepare("INSERT INTO ks.test (pk, ck, v) VALUES (?, ?, ?)");
        driverSession.execute(insertStatement.bind(pk, ck, value));

        RawChange change = getFirstRawChange(new TableName("ks", "test"));

        assertNotNull(change, "CDC change should not be null");
        assertEquals("ROW_INSERT", change.getOperationType().toString(), "Operation type should be ROW_INSERT");
        assertEquals(pk, change.getAsObject("pk"), "pk column value should match");
        assertEquals(ck, change.getAsObject("ck"), "ck column value should match");
        assertEquals(value, change.getAsObject("v"), "v column value should match");
    }
}
