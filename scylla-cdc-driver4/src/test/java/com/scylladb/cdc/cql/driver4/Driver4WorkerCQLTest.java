package com.scylladb.cdc.cql.driver4;

import com.scylladb.cdc.model.TableName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class Driver4WorkerCQLTest {
    // Each expected query is what 3.x QueryBuilder prints for the same table.
    @Test
    public void testQueryQuotesNamesAs3x() {
        assertQuery("SELECT * FROM ks.t_scylla_cdc_log WHERE \"cdc$stream_id\"=? AND \"cdc$time\">? AND \"cdc$time\"<=?;",
                "ks", "t");
        assertQuery("SELECT * FROM \"select\".order_scylla_cdc_log WHERE \"cdc$stream_id\"=? AND \"cdc$time\">? AND \"cdc$time\"<=?;",
                "select", "order");
        assertQuery("SELECT * FROM \"Ks\".\"Tbl_scylla_cdc_log\" WHERE \"cdc$stream_id\"=? AND \"cdc$time\">? AND \"cdc$time\"<=?;",
                "Ks", "Tbl");
        assertQuery("SELECT * FROM ks.\"1events_scylla_cdc_log\" WHERE \"cdc$stream_id\"=? AND \"cdc$time\">? AND \"cdc$time\"<=?;",
                "ks", "1events");
        assertQuery("SELECT * FROM ks.\"with space_scylla_cdc_log\" WHERE \"cdc$stream_id\"=? AND \"cdc$time\">? AND \"cdc$time\"<=?;",
                "ks", "with space");
        assertQuery("SELECT * FROM ks.\"\"\"Q\"\"_scylla_cdc_log\" WHERE \"cdc$stream_id\"=? AND \"cdc$time\">? AND \"cdc$time\"<=?;",
                "ks", "\"Q\"");
    }

    private static void assertQuery(String expected, String keyspace, String table) {
        assertEquals(expected, Driver4WorkerCQL.getStmt(new TableName(keyspace, table)));
    }
}
