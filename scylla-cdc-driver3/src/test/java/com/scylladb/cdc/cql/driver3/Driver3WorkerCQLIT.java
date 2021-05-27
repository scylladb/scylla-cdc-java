package com.scylladb.cdc.cql.driver3;

import com.datastax.driver.core.*;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class Driver3WorkerCQLIT extends BaseScyllaIntegrationTest {
    @Test
    public void testPreparedStatementSameSchemaBetweenPages() throws ExecutionException, InterruptedException, TimeoutException {
        // Tests an assumption used in the implementation
        // that given a PreparedStatement, it will always
        // return the rows with the same schema - in this
        // case: when a schema change happens between
        // pages of a query.

        // Note that Driver3Session deliberately sets
        // a protocol version to V4, while these
        // tests do not. This is on purpose:
        // if Scylla in the future implements a
        // support for new protocol version, this
        // test might fail and you (are you investing
        // this failure now?) should decide how to proceed.
        // See Driver3WorkerCQL for more context.

        driverSession.execute("CREATE TABLE ks.prepared_statement(pk int, v varchar," +
                "PRIMARY KEY(pk))");
        driverSession.execute("INSERT INTO ks.prepared_statement(pk, v) VALUES (1, 'a')");
        driverSession.execute("INSERT INTO ks.prepared_statement(pk, v) VALUES (2, 'b')");
        driverSession.execute("INSERT INTO ks.prepared_statement(pk, v) VALUES (3, 'c')");
        driverSession.execute("INSERT INTO ks.prepared_statement(pk, v) VALUES (4, 'd')");

        PreparedStatement preparedStatement = driverSession.prepare("SELECT pk, v FROM ks.prepared_statement");

        // Execute this query with 1-row paging
        // so that we can change the schema
        // between pages.
        ResultSet resultSet = driverSession.execute(preparedStatement.bind().setFetchSize(1));

        Row row1 = resultSet.one();
        assertEquals(DataType.Name.VARCHAR, row1.getColumnDefinitions().getType("v").getName());
        assertEquals(0, resultSet.getAvailableWithoutFetching());
        resultSet.fetchMoreResults().get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        Row row2 = resultSet.one();
        assertEquals(DataType.Name.VARCHAR, row2.getColumnDefinitions().getType("v").getName());
        assertEquals(0, resultSet.getAvailableWithoutFetching());

        // Change the schema in the middle of prepared statement query.
        driverSession.execute("ALTER TABLE ks.prepared_statement ALTER v TYPE blob");

        // The driver should have the newer schema now:
        assertEquals(DataType.Name.BLOB, driverSession.getCluster().getMetadata()
                .getKeyspace("ks").getTable("prepared_statement")
                .getColumn("v").getType().getName());

        // The column definitions should stay the same within the PreparedStatement.
        resultSet.fetchMoreResults().get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        Row row3 = resultSet.one();
        assertEquals(DataType.Name.VARCHAR, row3.getColumnDefinitions().getType("v").getName());
        assertTrue(row3.getObject("v") instanceof String);
        assertEquals(0, resultSet.getAvailableWithoutFetching());
        resultSet.fetchMoreResults().get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        Row row4 = resultSet.one();
        assertEquals(DataType.Name.VARCHAR, row4.getColumnDefinitions().getType("v").getName());
        assertTrue(row4.getObject("v") instanceof String);
        assertEquals(0, resultSet.getAvailableWithoutFetching());
    }

    @Test
    public void testPreparedStatementOldSchemaAfterAlter() throws ExecutionException, InterruptedException, TimeoutException {
        // Tests an assumption used in the implementation
        // that given a PreparedStatement, it will always
        // return the rows with the same schema - in this
        // case: when a query is prepared, then a schema
        // change happens and afterwards the query is started
        // it will still return the results with the
        // schema as of the time of preparing the query.

        driverSession.execute("CREATE TABLE ks.prepared_statement(pk int, v varchar," +
                "PRIMARY KEY(pk))");
        driverSession.execute("INSERT INTO ks.prepared_statement(pk, v) VALUES (1, 'a')");
        driverSession.execute("INSERT INTO ks.prepared_statement(pk, v) VALUES (2, 'b')");

        PreparedStatement preparedStatement = driverSession.prepare("SELECT pk, v FROM ks.prepared_statement");

        // After preparing the statement, change the schema.
        driverSession.execute("ALTER TABLE ks.prepared_statement ALTER v TYPE blob");

        // The driver should have the newer schema now:
        assertEquals(DataType.Name.BLOB, driverSession.getCluster().getMetadata()
                .getKeyspace("ks").getTable("prepared_statement")
                .getColumn("v").getType().getName());

        ResultSet resultSet = driverSession.execute(preparedStatement.bind().setFetchSize(1));

        // The schema should be the one at a time of preparing
        // the statement, not the one after ALTER TABLE.
        Row row1 = resultSet.one();
        assertEquals(DataType.Name.VARCHAR, row1.getColumnDefinitions().getType("v").getName());
        assertTrue(row1.getObject("v") instanceof String);
        assertEquals(0, resultSet.getAvailableWithoutFetching());
        resultSet.fetchMoreResults().get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        Row row2 = resultSet.one();
        assertEquals(DataType.Name.VARCHAR, row2.getColumnDefinitions().getType("v").getName());
        assertTrue(row2.getObject("v") instanceof String);
        assertEquals(0, resultSet.getAvailableWithoutFetching());
    }
}
