package com.scylladb.cdc.cql.driver4;

import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.ChangeSchema.CqlType;
import com.scylladb.cdc.model.worker.cql.CqlDate;
import com.scylladb.cdc.model.worker.cql.CqlDuration;
import com.scylladb.cdc.model.worker.cql.Field;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

public class Driver4ToLibraryTranslatorTest {
    private static final ChangeSchema.DataType TIMESTAMP = new ChangeSchema.DataType(CqlType.TIMESTAMP);
    private static final ChangeSchema.DataType TIME = new ChangeSchema.DataType(CqlType.TIME);
    private static final ChangeSchema.DataType DATE = new ChangeSchema.DataType(CqlType.DATE);
    private static final ChangeSchema.DataType INT = new ChangeSchema.DataType(CqlType.INT);
    private static final ChangeSchema.DataType TEXT = new ChangeSchema.DataType(CqlType.VARCHAR);

    private static final Instant INSTANT = Instant.parse("2026-11-24T10:15:30.123Z");
    private static final LocalTime LOCAL_TIME = LocalTime.of(13, 14, 15, 16);
    private static final LocalDate LOCAL_DATE = LocalDate.of(2026, 2, 28);

    @Test
    public void testNativeValuesHaveThe3xTypes() {
        assertEquals(Date.from(INSTANT), translate(INSTANT, TIMESTAMP, Date.class));
        assertEquals(LOCAL_TIME.toNanoOfDay(), translate(LOCAL_TIME, TIME, Long.class));
        assertEquals(new CqlDate(2026, 2, 28), translate(LOCAL_DATE, DATE, CqlDate.class));
        assertEquals(new CqlDuration(1, 2, 3), translate(
                com.datastax.oss.driver.api.core.data.CqlDuration.newInstance(1, 2, 3),
                new ChangeSchema.DataType(CqlType.DURATION), CqlDuration.class));
        assertNull(Driver4ToLibraryTranslator.translate(null, TIMESTAMP));
    }

    @Test
    public void testOtherNativeValuesAreUnchanged() throws Exception {
        for (Object value : Arrays.asList(1, 2L, (short) 3, (byte) 4, 5.0f, 6.0d, true, "text",
                BigInteger.TEN, UUID.randomUUID(), InetAddress.getByName("127.0.0.1"), ByteBuffer.wrap(new byte[] {1}))) {
            assertSame(value, Driver4ToLibraryTranslator.translate(value, INT));
        }
    }

    @Test
    public void testCollectionsTranslateTheirElements() {
        ChangeSchema.DataType listType = new ChangeSchema.DataType(CqlType.LIST, ImmutableList.of(TIMESTAMP), true);
        assertEquals(Collections.singletonList(new Driver4Field(TIMESTAMP, Date.from(INSTANT))),
                translate(Collections.singletonList(INSTANT), listType, List.class));

        // Sets and maps keep the driver's order.
        ChangeSchema.DataType setType = new ChangeSchema.DataType(CqlType.SET, ImmutableList.of(DATE), true);
        Set<LocalDate> dates = new LinkedHashSet<>(Arrays.asList(LOCAL_DATE.plusDays(1), LOCAL_DATE));
        assertEquals(Arrays.asList(new Driver4Field(DATE, new CqlDate(2026, 3, 1)), new Driver4Field(DATE, new CqlDate(2026, 2, 28))),
                new ArrayList<>(translate(dates, setType, LinkedHashSet.class)));

        ChangeSchema.DataType mapType = new ChangeSchema.DataType(CqlType.MAP, ImmutableList.of(TIME, DATE), true);
        Map<LocalTime, LocalDate> map = new LinkedHashMap<>();
        map.put(LOCAL_TIME.plusNanos(1), LOCAL_DATE);
        map.put(LOCAL_TIME, LOCAL_DATE.plusDays(1));
        Map<Field, Field> expected = new LinkedHashMap<>();
        expected.put(new Driver4Field(TIME, LOCAL_TIME.toNanoOfDay() + 1), new Driver4Field(DATE, new CqlDate(2026, 2, 28)));
        expected.put(new Driver4Field(TIME, LOCAL_TIME.toNanoOfDay()), new Driver4Field(DATE, new CqlDate(2026, 3, 1)));
        assertEquals(new ArrayList<>(expected.entrySet()),
                new ArrayList<>(translate(map, mapType, LinkedHashMap.class).entrySet()));
    }

    @Test
    public void testUdtFieldsAreTranslatedByPosition() {
        // Field names that differ only in case: a lookup by name can pick the wrong one.
        UserDefinedType type = new UserDefinedTypeBuilder("ks", "udt")
                .withField("\"At\"", DataTypes.TIMESTAMP)
                .withField("at", DataTypes.INT)
                .withField("dates", DataTypes.listOf(DataTypes.DATE))
                .build();
        UdtValue value = type.newValue(INSTANT, 5, Collections.singletonList(LOCAL_DATE));
        ChangeSchema.DataType datesType = new ChangeSchema.DataType(CqlType.LIST, ImmutableList.of(DATE), true);
        ChangeSchema.DataType udtType = new ChangeSchema.DataType(CqlType.UDT, new ChangeSchema.DataType.UdtType(
                ImmutableMap.of("At", TIMESTAMP, "at", INT, "dates", datesType), "ks", "udt"), true);

        Map<?, ?> translated = translate(value, udtType, LinkedHashMap.class);
        assertEquals(Arrays.asList("At", "at", "dates"), new ArrayList<>(translated.keySet()));
        assertEquals(new Driver4Field(TIMESTAMP, Date.from(INSTANT)), translated.get("At"));
        assertEquals(new Driver4Field(INT, 5), translated.get("at"));
        assertEquals(new Driver4Field(datesType, Collections.singletonList(new Driver4Field(DATE, new CqlDate(2026, 2, 28)))),
                translated.get("dates"));
    }

    @Test
    public void testTupleComponentsAreTranslated() {
        TupleValue value = DataTypes.tupleOf(DataTypes.INT, DataTypes.TIME, DataTypes.TEXT).newValue(7, LOCAL_TIME, null);
        ChangeSchema.DataType tupleType = new ChangeSchema.DataType(CqlType.TUPLE, ImmutableList.of(INT, TIME, TEXT), true);

        assertEquals(Arrays.asList(new Driver4Field(INT, 7), new Driver4Field(TIME, LOCAL_TIME.toNanoOfDay()),
                new Driver4Field(TEXT, null)), translate(value, tupleType, List.class));
    }

    @Test
    public void testBlobFieldPrintsOnlyItsBytes() {
        ByteBuffer slice = ByteBuffer.wrap(new byte[] {0x01, (byte) 0xAB, 0x02}, 1, 1).slice();
        assertEquals("AB", new Driver4Field(new ChangeSchema.DataType(CqlType.BLOB), slice).toString());
    }

    private static <T> T translate(Object value, ChangeSchema.DataType type, Class<T> expectedClass) {
        Object translated = Driver4ToLibraryTranslator.translate(value, type);
        return assertInstanceOf(expectedClass, translated);
    }
}
