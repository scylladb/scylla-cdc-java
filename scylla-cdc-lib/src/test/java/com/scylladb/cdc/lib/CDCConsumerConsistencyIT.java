package com.scylladb.cdc.lib;

import com.scylladb.cdc.cql.CQLConfiguration;
import com.scylladb.cdc.cql.WorkerCQL;
import com.scylladb.cdc.cql.driver3.BaseScyllaIntegrationTest;
import com.scylladb.cdc.cql.driver3.Driver3Session;
import com.scylladb.cdc.cql.driver3.Driver3WorkerCQL;
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
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CDCConsumerConsistencyIT extends BaseScyllaIntegrationTest {
    @Test
    public void defaultConsistencyReachesExecutedCdcReads() throws Exception {
        assertExecutedConsistency(CDCConsumer.builder(), CQLConfiguration.ConsistencyLevel.LOCAL_QUORUM);
    }

    @ParameterizedTest
    @EnumSource(value = CQLConfiguration.ConsistencyLevel.class, names = {"ONE", "QUORUM"})
    public void explicitConsistencyReachesExecutedCdcReads(
            CQLConfiguration.ConsistencyLevel consistencyLevel) throws Exception {
        assertExecutedConsistency(CDCConsumer.builder().withConsistencyLevel(consistencyLevel), consistencyLevel);
    }

    private void assertExecutedConsistency(CDCConsumer.Builder builder,
                                          CQLConfiguration.ConsistencyLevel expected) throws Exception {
        TableName table = new TableName("ks", "consumer_read_consistency");
        driverSession.execute("CREATE TABLE ks.consumer_read_consistency (pk int PRIMARY KEY, value int) " +
                "WITH cdc = {'enabled': true}");
        driverSession.execute("INSERT INTO ks.consumer_read_consistency (pk, value) VALUES (1, 42)");
        Task task = getTaskWithFirstRow(table);
        String datacenter = driverCluster.getMetadata().getAllHosts().iterator().next().getDatacenter();
        AtomicReference<RecordingWorker> recordedWorker = new AtomicReference<>();

        try (CDCConsumer consumer = builder
                .addContactPoint(libraryConfigurationBuilder().build().contactPoints.get(0))
                .addTable(table)
                .withLocalDCName(datacenter)
                .withRawChangeConsumer(change -> CompletableFuture.completedFuture(null))
                .withWorkerCQLProvider(session -> {
                    RecordingWorker worker = new RecordingWorker(session);
                    recordedWorker.set(worker);
                    return worker;
                })
                .build()) {
            RecordingWorker worker = recordedWorker.get();
            Object policies = invoke(invoke(invoke(worker.driverSession(), "getCluster"), "getConfiguration"),
                    "getPolicies");
            Object policy = invoke(policies, "getLoadBalancingPolicy");
            // The driver can add wrappers around the configured token-aware policy.
            while (true) {
                try {
                    policy = invoke(policy, "getChildPolicy");
                } catch (NoSuchMethodException e) {
                    break;
                }
            }
            assertEquals("DCAwareRoundRobinPolicy", policy.getClass().getSimpleName());

            worker.prepare(Collections.singleton(table));
            WorkerCQL.Reader reader = worker.createReader(task).get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            assertEquals(1, worker.results.size());
            Object result = worker.results.get(0).get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            Object executionInfo = invoke(result, "getExecutionInfo");
            Object statement = invoke(executionInfo, "getStatement");
            assertEquals(expected.name(), invoke(statement, "getConsistencyLevel").toString());
            assertEquals(datacenter, invoke(invoke(executionInfo, "getQueriedHost"), "getDatacenter"));
            assertNull(invoke(executionInfo, "getAchievedConsistencyLevel"));
            assertTrue(reader.nextChange().get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS).isPresent());
        }
    }

    // Failsafe uses the packaged backend; its relocated driver types cannot be cast to fixture types.
    private static Object invoke(Object target, String methodName) throws Exception {
        Method method = target.getClass().getMethod(methodName);
        method.setAccessible(true);
        return method.invoke(target);
    }

    private static class RecordingWorker extends Driver3WorkerCQL {
        private final Driver3Session session;
        private List<? extends Future<?>> results;

        RecordingWorker(Driver3Session session) {
            super(session);
            this.session = session;
        }

        Object driverSession() throws Exception {
            Method method = Driver3Session.class.getDeclaredMethod("getDriverSession");
            method.setAccessible(true);
            return method.invoke(session);
        }

        @Override
        @SuppressWarnings({"rawtypes", "unchecked"})
        protected CompletableFuture<Reader> wrapResults(Task task, List futures) {
            // The backend's future type is relocated when this module compiles after packaging.
            results = futures;
            return super.wrapResults(task, futures);
        }
    }
}
