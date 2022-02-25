package com.scylladb.cdc.cql.driver3;

import com.datastax.driver.core.*;
import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;
import com.scylladb.cdc.cql.WorkerCQL;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.worker.ChangeId;
import com.scylladb.cdc.model.worker.ChangeTime;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.Task;
import com.scylladb.cdc.model.worker.TaskState;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

    private StreamId parseStreamId(String streamId) {
        byte[] parsedBytes = BaseEncoding.base16().decode(streamId.replace("0x", "").toUpperCase());
        return new StreamId(ByteBuffer.wrap(parsedBytes));
    }

    private List<RawChange> fetchAllChangesForTask(Task task) throws ExecutionException, InterruptedException {
        Driver3WorkerCQL driver3WorkerCQL = new Driver3WorkerCQL(buildLibrarySession());
        driver3WorkerCQL.prepare(Collections.singleton(new TableName("ks", "test_last_consumed_id")));
        WorkerCQL.Reader reader = driver3WorkerCQL.createReader(task).get();

        List<RawChange> changes = new ArrayList<>();

        Optional<RawChange> rawChange;
        while ((rawChange = reader.nextChange().get()).isPresent()) {
            System.out.println(rawChange.get());

            changes.add(rawChange.get());
        }
        return changes;
    }

    @Test
    public void testReadingTasksWithLastConsumedId() throws ExecutionException, InterruptedException {
        // Test that reading Tasks with specified lastConsumedId
        // works correctly - a correct number of rows are returned.
        //
        // The test checks reading a single VNode, with:
        // - lastConsumedId non-present
        // - lastConsumedId in a middle of some stream
        // - lastConsumedId in a middle of some stream (and a small window)
        // - lastConsumedId at the end of some stream
        // - lastConsumedId at the end of last stream

        driverSession.execute("CREATE TABLE ks.test_last_consumed_id(pk int, ck int, PRIMARY KEY(pk, ck)) WITH cdc = {'enabled': true}");

        // Manually insert mock data in _scylla_cdc_log
        // to be agnostic to specific stream randomization
        // and current timestamp.

        UUID[] timestamps = { // Sorted ascending
                UUID.fromString("65c04ef6-93e5-11ec-8089-4c1a8b8e2b3c"), // 1645537400553
                UUID.fromString("6707be16-93e5-11ec-5f6e-d467d5eea7ab"), // 1645537402699
                UUID.fromString("69b41088-93e5-11ec-62f5-54f709fba607"), // 1645537407184
                UUID.fromString("6b75378a-93e5-11ec-ec22-c04dc85d438d"), // 1645537410128
                UUID.fromString("6cde9f6c-93e5-11ec-b8b6-8ff6d0b15a0e"), // 1645537412496
                UUID.fromString("6e679c9e-93e5-11ec-b977-9147c9e1fac5"), // 1645537415072
                UUID.fromString("6fd97174-93e5-11ec-400e-306eff318188"), // 1645537417495
                UUID.fromString("714b51b2-93e5-11ec-d21d-462760c16efd"), // 1645537419919
                UUID.fromString("72c0fab0-93e5-11ec-8b70-85d546bb40a4"), // 1645537422368
                UUID.fromString("746e71b2-93e5-11ec-1d21-e618840da799"), // 1645537425183
                UUID.fromString("76b9a95a-93e5-11ec-e1ca-4881e795f97f"), // 1645537429031
                UUID.fromString("7769abde-93e5-11ec-3b7a-daf7f5370c86"), // 1645537430185
                UUID.fromString("7848f780-93e5-11ec-1fc2-33e4baf89ab6"), // 1645537431648
                UUID.fromString("7903b8d6-93e5-11ec-470a-8250708da159")  // 1645537432872
        };

        StreamId[] streamIds = { // Sorted ascending
                parseStreamId("0xd9837affa7f3f06cf2a0771f7c0005d1"),
                parseStreamId("0xd98400000000000001be99612c0005d1"),
                parseStreamId("0xd985555555555556ac230701d40005d1"),
                parseStreamId("0xd986aaaaaaaaaaab7b9aa0401c0005d1"),
        };
        // In the same VNode:
        assertEquals(1, Arrays.stream(streamIds).map(streamId -> streamId.getVNodeId().getIndex()).distinct().count());

        // Mock data per stream and sorted by time (T1, T2, T3, ...):
        //
        // Stream 1   T1               T7         T11        T14
        // Stream 2      T2 T3    T5                  T12
        // Stream 3            T4   T6                    T13
        // Stream 4                      T8 T9 T10
        //

        PreparedStatement cdcInsertStatement = driverSession.prepare(
                "INSERT INTO ks.test_last_consumed_id_scylla_cdc_log(\"cdc$stream_id\", \"cdc$time\", \"cdc$batch_seq_no\", \"cdc$end_of_batch\", \"cdc$operation\", pk, ck) VALUES (?, ?, 0, true, 2, ?, ?)");

        driverSession.execute(cdcInsertStatement.bind(streamIds[0].getValue(), timestamps[0], 1, 1));
        driverSession.execute(cdcInsertStatement.bind(streamIds[1].getValue(), timestamps[1], 1, 2));
        driverSession.execute(cdcInsertStatement.bind(streamIds[1].getValue(), timestamps[2], 1, 3));
        driverSession.execute(cdcInsertStatement.bind(streamIds[2].getValue(), timestamps[3], 1, 4));
        driverSession.execute(cdcInsertStatement.bind(streamIds[1].getValue(), timestamps[4], 1, 5));
        driverSession.execute(cdcInsertStatement.bind(streamIds[2].getValue(), timestamps[5], 1, 6));
        driverSession.execute(cdcInsertStatement.bind(streamIds[0].getValue(), timestamps[6], 1, 7));
        driverSession.execute(cdcInsertStatement.bind(streamIds[3].getValue(), timestamps[7], 1, 8));
        driverSession.execute(cdcInsertStatement.bind(streamIds[3].getValue(), timestamps[8], 1, 9));
        driverSession.execute(cdcInsertStatement.bind(streamIds[3].getValue(), timestamps[9], 1, 10));
        driverSession.execute(cdcInsertStatement.bind(streamIds[0].getValue(), timestamps[10], 1, 11));
        driverSession.execute(cdcInsertStatement.bind(streamIds[1].getValue(), timestamps[11], 1, 12));
        driverSession.execute(cdcInsertStatement.bind(streamIds[2].getValue(), timestamps[12], 1, 13));
        driverSession.execute(cdcInsertStatement.bind(streamIds[0].getValue(), timestamps[13], 1, 14));

        // Task with lastConsumedId non-present
        TaskId taskId = mock(TaskId.class);
        when(taskId.getvNodeId()).thenReturn(streamIds[0].getVNodeId());
        when(taskId.getTable()).thenReturn(new TableName("ks", "test_last_consumed_id"));

        {
            TaskState taskState = new TaskState(new Timestamp(new Date(1645537400553L - 1)), new Timestamp(new Date(1645537432872L + 1)), Optional.empty());
            Task task = new Task(taskId, Arrays.stream(streamIds).collect(Collectors.toCollection(TreeSet::new)), taskState);

            List<RawChange> changes = fetchAllChangesForTask(task);

            assertEquals(Lists.newArrayList(1, 7, 11, 14, 2, 3, 5, 12, 4, 6, 13, 8, 9, 10),
                    changes.stream().map(c -> c.getCell("ck").getInt()).collect(Collectors.toList()));
        }

        // Task with lastConsumedId in a middle of some stream
        {
            TaskState taskState = new TaskState(new Timestamp(new Date(1645537400553L - 1)), new Timestamp(new Date(1645537432872L + 1)),
                    Optional.of(new ChangeId(streamIds[1], new ChangeTime(timestamps[4]))));
            Task task = new Task(taskId, Arrays.stream(streamIds).collect(Collectors.toCollection(TreeSet::new)), taskState);

            List<RawChange> changes = fetchAllChangesForTask(task);

            assertEquals(Lists.newArrayList(12, 4, 6, 13, 8, 9, 10),
                    changes.stream().map(c -> c.getCell("ck").getInt()).collect(Collectors.toList()));
        }

        // Task with lastConsumedId in a middle of some stream (and a small window from T4 to T11)
        {
            TaskState taskState = new TaskState(new Timestamp(new Date(1645537410128L - 1)), new Timestamp(new Date(1645537429031L + 1)),
                    Optional.of(new ChangeId(streamIds[1], new ChangeTime(timestamps[4]))));
            Task task = new Task(taskId, Arrays.stream(streamIds).collect(Collectors.toCollection(TreeSet::new)), taskState);

            List<RawChange> changes = fetchAllChangesForTask(task);

            assertEquals(Lists.newArrayList(4, 6, 8, 9, 10),
                    changes.stream().map(c -> c.getCell("ck").getInt()).collect(Collectors.toList()));
        }

        // Task with lastConsumedId in a middle of some stream (and a small window from T4 to T11),
        // but lastConsumedId is before the window. A window start should take precedence.
        {
            TaskState taskState = new TaskState(new Timestamp(new Date(1645537410128L - 1)), new Timestamp(new Date(1645537429031L + 1)),
                    Optional.of(new ChangeId(streamIds[1], new ChangeTime(timestamps[1]))));
            Task task = new Task(taskId, Arrays.stream(streamIds).collect(Collectors.toCollection(TreeSet::new)), taskState);

            List<RawChange> changes = fetchAllChangesForTask(task);

            assertEquals(Lists.newArrayList(5, 4, 6, 8, 9, 10),
                    changes.stream().map(c -> c.getCell("ck").getInt()).collect(Collectors.toList()));
        }

        // Task with lastConsumedId at the end of some stream
        {
            TaskState taskState = new TaskState(new Timestamp(new Date(1645537400553L - 1)), new Timestamp(new Date(1645537432872L + 1)),
                    Optional.of(new ChangeId(streamIds[1], new ChangeTime(timestamps[11]))));
            Task task = new Task(taskId, Arrays.stream(streamIds).collect(Collectors.toCollection(TreeSet::new)), taskState);

            List<RawChange> changes = fetchAllChangesForTask(task);

            assertEquals(Lists.newArrayList(4, 6, 13, 8, 9, 10),
                    changes.stream().map(c -> c.getCell("ck").getInt()).collect(Collectors.toList()));
        }

        // Task at the end of last stream
        {
            TaskState taskState = new TaskState(new Timestamp(new Date(1645537400553L - 1)), new Timestamp(new Date(1645537432872L + 1)),
                    Optional.of(new ChangeId(streamIds[3], new ChangeTime(timestamps[9]))));
            Task task = new Task(taskId, Arrays.stream(streamIds).collect(Collectors.toCollection(TreeSet::new)), taskState);

            List<RawChange> changes = fetchAllChangesForTask(task);

            assertEquals(Collections.emptyList(),
                    changes.stream().map(c -> c.getCell("ck").getInt()).collect(Collectors.toList()));
        }
    }
}
