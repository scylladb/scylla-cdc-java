package com.scylladb.cdc.cql.driver4;

import com.datastax.oss.driver.api.core.cql.Row;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("integration")
@Tag("tablets")
public class Driver4SessionTabletsIT extends BaseScyllaTabletsIntegrationTest {

    @Test
    public void testSessionReadsCdcTable() {
        driverSession.execute("CREATE TABLE ks.t (pk int PRIMARY KEY, v text) WITH cdc = {'enabled': true}");

        Row keyspace = driverSession.execute(
                "SELECT initial_tablets FROM system_schema.scylla_keyspaces WHERE keyspace_name = 'ks'").one();
        assertNotNull(keyspace, "ks uses tablets");
        assertEquals(2, keyspace.getInt("initial_tablets"));

        Driver4SessionIT.assertSessionReadsCdcTable(buildLibrarySession());
    }
}
