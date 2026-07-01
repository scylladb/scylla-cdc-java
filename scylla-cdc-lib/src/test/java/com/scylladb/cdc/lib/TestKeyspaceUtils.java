package com.scylladb.cdc.lib;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.QueryValidationException;

/**
 * Utility methods for managing keyspaces in integration tests.
 */
class TestKeyspaceUtils {

    private TestKeyspaceUtils() {}

    /**
     * Creates a keyspace with tablet replication explicitly disabled.
     *
     * ScyllaDB 6.x enables tablet replication by default for NetworkTopologyStrategy
     * keyspaces. CDC is incompatible with tablets (issue #16317). This method
     * attempts to create the keyspace with {@code AND tablets = {'enabled': false}}
     * to opt out of tablets. On ScyllaDB versions that do not support the
     * {@code tablets} property (< 6.0), it falls back to creating the keyspace
     * without the clause; tablets are not active on those versions anyway.
     *
     * @param session   the driver session to use
     * @param keyspace  the keyspace name
     */
    static void createWithoutTablets(Session session, String keyspace) {
        try {
            session.execute(String.format(
                "CREATE KEYSPACE %s WITH replication = " +
                "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1} " +
                "AND tablets = {'enabled': false}",
                keyspace));
        } catch (QueryValidationException e) {
            if (e.getMessage() != null && e.getMessage().contains("Unknown property 'tablets'")) {
                // ScyllaDB < 6.0: tablets not supported; NTS without the clause is fine.
                session.execute(String.format(
                    "CREATE KEYSPACE %s WITH replication = " +
                    "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}",
                    keyspace));
            } else {
                throw e;
            }
        }
    }
}
