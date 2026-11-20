package com.scylladb.cdc.cql.driver4;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
public class Driver4SessionIT extends BaseScyllaIntegrationTest {

    /**
     * Uses a library session on the CDC table {@code ks.t}.
     */
    static void assertSessionReadsCdcTable(Driver4Session librarySession) {
        CqlSession session = librarySession.getDriverSession();

        assertEquals(DefaultProtocolVersion.V4, session.getContext().getProtocolVersion());
        assertTrue(session.getMetadata().getKeyspace("ks")
                .flatMap(keyspace -> keyspace.getTable("t_scylla_cdc_log"))
                .isPresent(), "the CDC log table is in the session's metadata");

        session.execute(SimpleStatement.newInstance("INSERT INTO ks.t (pk, v) VALUES (?, ?)", 1, "one")
                .setConsistencyLevel(librarySession.getConsistencyLevel()));

        Row row = session.execute(SimpleStatement.newInstance("SELECT v FROM ks.t WHERE pk = ?", 1)
                .setConsistencyLevel(librarySession.getConsistencyLevel())).one();
        assertNotNull(row);
        assertEquals("one", row.getString("v"));

        Row change = session.execute(SimpleStatement.newInstance("SELECT v FROM ks.t_scylla_cdc_log")
                .setConsistencyLevel(librarySession.getConsistencyLevel())).one();
        assertNotNull(change, "the insert is in the CDC log");
        assertEquals("one", change.getString("v"));
    }

    @Test
    public void testSessionReadsCdcTable() {
        driverSession.execute("CREATE TABLE ks.t (pk int PRIMARY KEY, v text) WITH cdc = {'enabled': true}");

        assertSessionReadsCdcTable(buildLibrarySession());
    }
}
