package com.scylladb.cdc.cql.driver4;

import com.scylladb.cdc.cql.WorkerCQL;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.cql.CqlDate;
import com.scylladb.cdc.model.worker.cql.CqlDuration;
import com.scylladb.cdc.model.worker.cql.Field;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("integration")
public class Driver4WorkerCQLIT extends BaseScyllaIntegrationTest {
    private static final TableName PAGED = new TableName("ks", "paged");

    @Test
    public void testReaderPagesThroughTheWindow() throws Exception {
        createPagedTable();
        for (int ck = 0; ck < 5; ck++) {
            driverSession.execute("INSERT INTO ks.paged (pk, ck, v) VALUES (1, " + ck + ", " + ck + ")");
        }

        try (Driver4Session session = new Driver4Session(libraryConfigurationBuilder().withQueryOptionsFetchSize(2).build())) {
            List<RawChange> changes = Driver4TimeUuidResumeIT.readAll(preparedWorker(session), getTaskWithFirstRow(PAGED));

            assertEquals(5, changes.size());
            for (int ck = 0; ck < 5; ck++) {
                assertEquals(ck, changes.get(ck).getAsObject("ck"));
                // Three pages of one schema build it once.
                assertSame(changes.get(0).getSchema(), changes.get(ck).getSchema());
            }
        }
    }

    @Test
    public void testExhaustedReaderStaysEmpty() throws Exception {
        createPagedTable();
        driverSession.execute("INSERT INTO ks.paged (pk, ck, v) VALUES (1, 0, 0)");

        WorkerCQL.Reader reader = preparedWorker(buildLibrarySession()).createReader(getTaskWithFirstRow(PAGED))
                .get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertEquals(0, next(reader).getAsObject("ck"));
        assertEquals(Optional.empty(), reader.nextChange().get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS));
        assertEquals(Optional.empty(), reader.nextChange().get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testReaderQueriesAreIdempotent() throws Exception {
        createPagedTable();
        driverSession.execute("INSERT INTO ks.paged (pk, ck, v) VALUES (1, 0, 0)");

        try (Driver4MasterCQLIT.RecordingDriver4Session session = Driver4MasterCQLIT.RecordingDriver4Session.connect()) {
            assertEquals(1, Driver4TimeUuidResumeIT.readAll(preparedWorker(session), getTaskWithFirstRow(PAGED)).size());
            session.assertAllIdempotent();
        }
    }

    @Test
    public void testSchemaChangeBetweenPagesRebuildsTheSchema() throws Exception {
        createPagedTable();
        for (int ck = 0; ck < 3; ck++) {
            driverSession.execute("INSERT INTO ks.paged (pk, ck, v) VALUES (1, " + ck + ", " + ck + ")");
        }

        try (Driver4Session session = new Driver4Session(libraryConfigurationBuilder().withQueryOptionsFetchSize(2).build())) {
            WorkerCQL.Reader reader = preparedWorker(session).createReader(getTaskWithFirstRow(PAGED))
                    .get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            RawChange first = next(reader);
            RawChange second = next(reader);
            // The second page is fetched only now, after the schema change.
            driverSession.execute("ALTER TABLE ks.paged ADD added int");
            RawChange third = next(reader);

            assertSame(first.getSchema(), second.getSchema());
            assertThrows(IllegalArgumentException.class, () -> first.getSchema().getColumnDefinition("added"));
            assertEquals(ChangeSchema.ColumnKind.REGULAR, third.getSchema().getColumnDefinition("added").getBaseTableColumnKind());
            assertNull(third.getAsObject("added"));
            assertEquals(2, third.getAsObject("ck"));
        }
    }

    @Test
    public void testSchemaChangeAfterPrepareIsRead() throws Exception {
        createPagedTable();
        driverSession.execute("INSERT INTO ks.paged (pk, ck, v) VALUES (1, 0, 0)");
        WorkerCQL worker = preparedWorker(buildLibrarySession());

        driverSession.execute("ALTER TABLE ks.paged ADD added int");
        driverSession.execute("INSERT INTO ks.paged (pk, ck, v, added) VALUES (1, 1, 1, 7)");

        List<RawChange> changes = Driver4TimeUuidResumeIT.readAll(worker, getTaskWithFirstRow(PAGED));
        assertEquals(2, changes.size());
        assertNull(changes.get(0).getAsObject("added"));
        assertEquals(7, changes.get(1).getAsObject("added"));
    }

    @Test
    public void testValuesHaveThe3xTypes() throws Exception {
        driverSession.execute("CREATE TYPE ks.point(\"At\" timestamp, at int)");
        driverSession.execute("CREATE TABLE ks.values_table(pk int PRIMARY KEY, a ascii, b bigint, bl blob, bo boolean, "
                + "d date, de decimal, do double, du duration, f float, i inet, s smallint, t text, ti time, ts timestamp, "
                + "tu timeuuid, tiny tinyint, u uuid, vi varint, dates frozen<list<date>>, p frozen<point>, "
                + "tup frozen<tuple<time, int>>) WITH cdc = {'enabled': true}");
        UUID timeuuid = UUID.fromString("e2a0d5a0-ca45-11ef-8000-000000000000");
        UUID uuid = UUID.fromString("3c3b7c8a-4c6e-4a8e-9a3b-7a4b8f0c1d2e");
        driverSession.execute("INSERT INTO ks.values_table(pk, a, b, bl, bo, d, de, do, du, f, i, s, t, ti, ts, tu, tiny, u, vi, "
                + "dates, p, tup) VALUES (1, 'ascii', 2, 0xab, true, '2026-02-28', 1.5, 2.5, 1mo2d3ns, 3.5, '127.0.0.1', 4, "
                + "'text', '13:14:15.000000016', '2026-11-24T10:15:30.123Z', " + timeuuid + ", 5, " + uuid + ", 6, "
                + "['2026-03-01'], {\"At\": '2026-11-24T10:15:30.123Z', at: 8}, ('00:00:01', 9))");

        RawChange change = getFirstRawChange(new TableName("ks", "values_table"));

        Date timestamp = Date.from(Instant.parse("2026-11-24T10:15:30.123Z"));
        assertEquals("ascii", change.getAsObject("a"));
        assertEquals(2L, change.getAsObject("b"));
        assertEquals(ByteBuffer.wrap(new byte[] {(byte) 0xab}), change.getAsObject("bl"));
        assertEquals(true, change.getAsObject("bo"));
        assertEquals(new CqlDate(2026, 2, 28), change.getAsObject("d"));
        assertEquals(new BigDecimal("1.5"), change.getAsObject("de"));
        assertEquals(2.5d, change.getAsObject("do"));
        assertEquals(new CqlDuration(1, 2, 3), change.getAsObject("du"));
        assertEquals(3.5f, change.getAsObject("f"));
        assertEquals(InetAddress.getByName("127.0.0.1"), change.getAsObject("i"));
        assertEquals((short) 4, change.getAsObject("s"));
        assertEquals("text", change.getAsObject("t"));
        assertEquals(LocalTime.of(13, 14, 15, 16).toNanoOfDay(), change.getAsObject("ti"));
        assertEquals(timestamp, change.getAsObject("ts"));
        assertEquals(timeuuid, change.getAsObject("tu"));
        assertEquals((byte) 5, change.getAsObject("tiny"));
        assertEquals(uuid, change.getAsObject("u"));
        assertEquals(BigInteger.valueOf(6), change.getAsObject("vi"));

        assertEquals(Collections.singletonList(new CqlDate(2026, 3, 1)), values((List<?>) change.getAsObject("dates")));
        Map<?, ?> point = (Map<?, ?>) change.getAsObject("p");
        assertEquals(Arrays.asList("At", "at"), new ArrayList<>(point.keySet()));
        assertEquals(timestamp, ((Field) point.get("At")).getAsObject());
        assertEquals(8, ((Field) point.get("at")).getAsObject());
        assertEquals(Arrays.asList(LocalTime.of(0, 0, 1).toNanoOfDay(), 9), values((List<?>) change.getAsObject("tup")));
    }

    private void createPagedTable() {
        driverSession.execute("CREATE TABLE ks.paged (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true}");
    }

    private static WorkerCQL preparedWorker(Driver4Session session) throws Exception {
        WorkerCQL worker = new Driver4WorkerCQL(session);
        worker.prepare(Collections.singleton(PAGED));
        return worker;
    }

    private static RawChange next(WorkerCQL.Reader reader) throws Exception {
        return reader.nextChange().get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS).get();
    }

    private static List<Object> values(List<?> fields) {
        List<Object> values = new ArrayList<>();
        fields.forEach(f -> values.add(((Field) f).getAsObject()));
        return values;
    }
}
