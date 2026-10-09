package com.scylladb.cdc.cql.driver3;

import com.scylladb.cdc.cql.WorkerCQL;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.worker.ChangeId;
import com.scylladb.cdc.model.worker.ChangeTime;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.Task;
import com.scylladb.cdc.model.worker.TaskState;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
public class Driver3TimeUuidResumeIT extends BaseScyllaIntegrationTest {
    @Test
    public void matchesScyllaOrderingAtSignedByteBoundaries() {
        driverSession.execute("CREATE TABLE ks.timeuuid_order (pk int, t timeuuid, " +
                "PRIMARY KEY (pk, t))");

        long mostSignificantBits = UUID.fromString("00000000-0000-1000-8000-000000000000")
                .getMostSignificantBits();
        List<UUID> expected = Arrays.asList(
                new UUID(mostSignificantBits, 0x8080000000000000L),
                new UUID(mostSignificantBits, 0x8000000000000080L),
                new UUID(mostSignificantBits, 0x800000000000007fL),
                new UUID(mostSignificantBits, 0x8001000000000080L),
                new UUID(mostSignificantBits, 0x807f000000000000L));
        for (UUID time : expected) {
            driverSession.execute("INSERT INTO ks.timeuuid_order (pk, t) VALUES (1, " + time + ")");
        }

        List<UUID> actual = new ArrayList<>();
        driverSession.execute("SELECT t FROM ks.timeuuid_order WHERE pk = 1")
                .forEach(row -> actual.add(row.getUUID("t")));
        assertEquals(expected, actual);
        for (int i = 1; i < actual.size(); i++) {
            assertTrue(new ChangeTime(actual.get(i - 1)).compareTo(new ChangeTime(actual.get(i))) < 0);
        }
    }

    @Test
    public void resumesInScyllaTimeUuidOrder() throws Exception {
        TableName table = new TableName("ks", "timeuuid_resume");
        driverSession.execute("CREATE TABLE ks.timeuuid_resume (pk int, ck int, value int, " +
                "PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true}");

        long writeTimestamp = (System.currentTimeMillis() - 1000) * 1000;
        for (int ck = 0; ck < 4; ck++) {
            driverSession.execute("INSERT INTO ks.timeuuid_resume (pk, ck, value) " +
                    "VALUES (1, " + ck + ", " + ck + ") USING TIMESTAMP " + writeTimestamp);
        }

        Task initial = getTaskWithFirstRow(table);
        initial = initial.updateState(new TaskState(initial.state.getWindowStartTimestamp(),
                new Timestamp(new Date(System.currentTimeMillis() + 10_000)), Optional.empty()));
        WorkerCQL workerCQL = new Driver3WorkerCQL(buildLibrarySession());
        workerCQL.prepare(Collections.singleton(table));

        List<ChangeId> ordered = readIds(workerCQL, initial);
        assertEquals(4, ordered.size());
        for (int i = 1; i < ordered.size(); i++) {
            assertEquals(ordered.get(0).getChangeTime().getUUID().timestamp(),
                    ordered.get(i).getChangeTime().getUUID().timestamp());
            assertTrue(ordered.get(i - 1).compareTo(ordered.get(i)) < 0);
        }

        Task resumed = initial.updateState(ordered.get(1));
        assertEquals(ordered.subList(2, ordered.size()), readIds(workerCQL, resumed));
    }

    private static List<ChangeId> readIds(WorkerCQL workerCQL, Task task) throws Exception {
        WorkerCQL.Reader reader = workerCQL.createReader(task).get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        List<ChangeId> ids = new ArrayList<>();
        Optional<RawChange> change;
        while ((change = reader.nextChange().get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS)).isPresent()) {
            ids.add(change.get().getId());
        }
        return ids;
    }
}
