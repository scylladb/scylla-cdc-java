package com.scylladb.cdc.cql.driver4;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.scylladb.cdc.cql.CQLConfiguration;
import com.scylladb.cdc.cql.WorkerCQL;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.worker.Task;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
public class Driver4WorkerCQLConsistencyIT extends BaseScyllaIntegrationTest {
    @Test
    public void defaultConsistencyReachesExecutedCdcReads() throws Exception {
        assertExecutedConsistency(libraryConfigurationBuilder().build(), CQLConfiguration.ConsistencyLevel.LOCAL_QUORUM);
    }

    @ParameterizedTest
    @EnumSource(value = CQLConfiguration.ConsistencyLevel.class,
            names = {"ONE", "LOCAL_ONE", "QUORUM", "LOCAL_QUORUM", "ALL"})
    public void explicitConsistencyReachesExecutedCdcReads(
            CQLConfiguration.ConsistencyLevel consistencyLevel) throws Exception {
        // TWO and THREE require more replicas than this single-node fixture provides.
        assertExecutedConsistency(libraryConfigurationBuilder().withConsistencyLevel(consistencyLevel).build(),
                consistencyLevel);
    }

    private void assertExecutedConsistency(CQLConfiguration configuration,
                                           CQLConfiguration.ConsistencyLevel expected) throws Exception {
        TableName table = new TableName("ks", "read_consistency");
        driverSession.execute("CREATE TABLE ks.read_consistency (pk int PRIMARY KEY, value int) "
                + "WITH cdc = {'enabled': true}");
        driverSession.execute("INSERT INTO ks.read_consistency (pk, value) VALUES (1, 42)");
        Task task = getTaskWithFirstRow(table);

        try (Driver4Session session = new Driver4Session(configuration)) {
            RecordingWorker worker = new RecordingWorker(session);
            worker.prepare(Collections.singleton(table));
            WorkerCQL.Reader reader = worker.createReader(task).get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            assertEquals(1, worker.results.size());
            ExecutionInfo executionInfo = worker.results.get(0).get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                    .getExecutionInfo();
            assertEquals(expected.name(), executionInfo.getStatement().getConsistencyLevel().name());
            // 4.x reports no achieved consistency; a read retried at another level would list an error.
            assertEquals(Collections.emptyList(), executionInfo.getErrors(), "The read succeeds on the first attempt");
            assertTrue(reader.nextChange().get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS).isPresent());
        }
    }

    private static class RecordingWorker extends Driver4WorkerCQL {
        private List<CompletableFuture<AsyncResultSet>> results;

        RecordingWorker(Driver4Session session) {
            super(session);
        }

        @Override
        protected CompletableFuture<Reader> wrapResults(Task task, List<CompletableFuture<AsyncResultSet>> futures) {
            results = futures;
            return super.wrapResults(task, futures);
        }
    }
}
