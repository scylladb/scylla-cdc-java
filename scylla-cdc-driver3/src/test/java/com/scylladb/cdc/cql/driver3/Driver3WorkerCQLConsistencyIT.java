package com.scylladb.cdc.cql.driver3;

import com.datastax.driver.core.ResultSetFuture;
import com.scylladb.cdc.cql.CQLConfiguration;
import com.scylladb.cdc.cql.WorkerCQL;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.worker.Task;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class Driver3WorkerCQLConsistencyIT extends BaseScyllaIntegrationTest {
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
        driverSession.execute("CREATE TABLE ks.read_consistency (pk int PRIMARY KEY, value int) " +
                "WITH cdc = {'enabled': true}");
        driverSession.execute("INSERT INTO ks.read_consistency (pk, value) VALUES (1, 42)");
        Task task = getTaskWithFirstRow(table);

        try (Driver3Session session = new Driver3Session(configuration)) {
            RecordingWorker worker = new RecordingWorker(session);
            worker.prepare(Collections.singleton(table));
            WorkerCQL.Reader reader = worker.createReader(task).get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            assertEquals(1, worker.results.size());
            Object result = worker.results.get(0).get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            Object executionInfo = invoke(result, "getExecutionInfo");
            Object statement = invoke(executionInfo, "getStatement");
            assertEquals(expected.name(), invoke(statement, "getConsistencyLevel").toString());
            assertNull(invoke(executionInfo, "getAchievedConsistencyLevel"),
                    "The read must succeed without a consistency downgrade");
            assertTrue(reader.nextChange().get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS).isPresent());
        }
    }

    // Failsafe loads the packaged backend, whose driver classes are relocated by shading.
    // Inspect its executed-statement metadata without casting to the fixture's unshaded types.
    private static Object invoke(Object target, String methodName) throws Exception {
        Method method = target.getClass().getMethod(methodName);
        method.setAccessible(true);
        return method.invoke(target);
    }

    private static class RecordingWorker extends Driver3WorkerCQL {
        private List<? extends Future<?>> results;

        RecordingWorker(Driver3Session session) {
            super(session);
        }

        @Override
        protected CompletableFuture<Reader> wrapResults(Task task, List<ResultSetFuture> futures) {
            results = futures;
            return super.wrapResults(task, futures);
        }
    }
}
