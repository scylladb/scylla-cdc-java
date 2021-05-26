package com.scylladb.cdc.model.worker;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ChangeSchemaTest {
    // CDC table for:
    // CREATE TABLE ks.simple(pk int, ck int, v int, PRIMARY KEY(pk, ck)) WITH cdc = {'enabled': true};
    //
    // CREATE TABLE ks.simple_scylla_cdc_log (
    //    "cdc$stream_id" blob,
    //    "cdc$time" timeuuid,
    //    "cdc$batch_seq_no" int,
    //    "cdc$deleted_v" boolean,
    //    "cdc$end_of_batch" boolean,
    //    "cdc$operation" tinyint,
    //    "cdc$ttl" bigint,
    //    ck int,
    //    pk int,
    //    v int,
    //    PRIMARY KEY ("cdc$stream_id", "cdc$time", "cdc$batch_seq_no")
    // )
    public static final ChangeSchema TEST_SCHEMA_SIMPLE = new ChangeSchema(Lists.newArrayList(
            new ChangeSchema.ColumnDefinition("cdc$stream_id", 0, new ChangeSchema.DataType(ChangeSchema.CqlType.BLOB), null, null),
            new ChangeSchema.ColumnDefinition("cdc$time", 1, new ChangeSchema.DataType(ChangeSchema.CqlType.TIMEUUID), null, null),
            new ChangeSchema.ColumnDefinition("cdc$batch_seq_no", 2, new ChangeSchema.DataType(ChangeSchema.CqlType.INT), null, null),
            new ChangeSchema.ColumnDefinition("cdc$deleted_v", 3, new ChangeSchema.DataType(ChangeSchema.CqlType.BOOLEAN), null, null),
            new ChangeSchema.ColumnDefinition("cdc$end_of_batch", 4, new ChangeSchema.DataType(ChangeSchema.CqlType.BOOLEAN), null, null),
            new ChangeSchema.ColumnDefinition("cdc$operation", 5, new ChangeSchema.DataType(ChangeSchema.CqlType.TINYINT), null, null),
            new ChangeSchema.ColumnDefinition("cdc$ttl", 6, new ChangeSchema.DataType(ChangeSchema.CqlType.BIGINT), null, null),
            new ChangeSchema.ColumnDefinition("ck", 7, new ChangeSchema.DataType(ChangeSchema.CqlType.INT), new ChangeSchema.DataType(ChangeSchema.CqlType.INT), ChangeSchema.ColumnKind.CLUSTERING_KEY),
            new ChangeSchema.ColumnDefinition("pk", 8, new ChangeSchema.DataType(ChangeSchema.CqlType.INT), new ChangeSchema.DataType(ChangeSchema.CqlType.INT), ChangeSchema.ColumnKind.PARTITION_KEY),
            new ChangeSchema.ColumnDefinition("v", 9, new ChangeSchema.DataType(ChangeSchema.CqlType.INT), new ChangeSchema.DataType(ChangeSchema.CqlType.INT), ChangeSchema.ColumnKind.REGULAR)
    ));

    // CDC table for:
    // CREATE TABLE ks.frozen_collections(pk int, ck int, v frozen<set<int>>, v2 frozen<list<int>>, v3 frozen<map<double, text>>, v4 frozen<tuple<inet, int>>, PRIMARY KEY(pk, ck)) WITH cdc = {'enabled': true};
    //
    // CREATE TABLE ks.frozen_collections_scylla_cdc_log (
    //    "cdc$stream_id" blob,
    //    "cdc$time" timeuuid,
    //    "cdc$batch_seq_no" int,
    //    "cdc$deleted_v" boolean,
    //    "cdc$deleted_v2" boolean,
    //    "cdc$deleted_v3" boolean,
    //    "cdc$deleted_v4" boolean,
    //    "cdc$end_of_batch" boolean,
    //    "cdc$operation" tinyint,
    //    "cdc$ttl" bigint,
    //    ck int,
    //    pk int,
    //    v frozen<set<int>>,
    //    v2 frozen<list<int>>,
    //    v3 frozen<map<double, text>>,
    //    v4 frozen<tuple<inet, int>>,
    //    PRIMARY KEY ("cdc$stream_id", "cdc$time", "cdc$batch_seq_no")
    // )
    public static final ChangeSchema.DataType FROZEN_SET_INT = new ChangeSchema.DataType(ChangeSchema.CqlType.SET, Collections.singletonList(new ChangeSchema.DataType(ChangeSchema.CqlType.INT)), true);
    public static final ChangeSchema.DataType FROZEN_LIST_INT = new ChangeSchema.DataType(ChangeSchema.CqlType.LIST, Collections.singletonList(new ChangeSchema.DataType(ChangeSchema.CqlType.INT)), true);
    public static final ChangeSchema.DataType FROZEN_MAP_DOUBLE_TEXT = new ChangeSchema.DataType(ChangeSchema.CqlType.MAP, Lists.newArrayList(
            new ChangeSchema.DataType(ChangeSchema.CqlType.DOUBLE), new ChangeSchema.DataType(ChangeSchema.CqlType.TEXT)), true);
    public static final ChangeSchema.DataType FROZEN_TUPLE_INET_INT = new ChangeSchema.DataType(ChangeSchema.CqlType.TUPLE, Lists.newArrayList(
            new ChangeSchema.DataType(ChangeSchema.CqlType.INET), new ChangeSchema.DataType(ChangeSchema.CqlType.INT)), true);

    public static final ChangeSchema TEST_SCHEMA_FROZEN_COLLECTIONS = new ChangeSchema(Lists.newArrayList(
            new ChangeSchema.ColumnDefinition("cdc$stream_id", 0, new ChangeSchema.DataType(ChangeSchema.CqlType.BLOB), null, null),
            new ChangeSchema.ColumnDefinition("cdc$time", 1, new ChangeSchema.DataType(ChangeSchema.CqlType.TIMEUUID), null, null),
            new ChangeSchema.ColumnDefinition("cdc$batch_seq_no", 2, new ChangeSchema.DataType(ChangeSchema.CqlType.INT), null, null),
            new ChangeSchema.ColumnDefinition("cdc$deleted_v", 3, new ChangeSchema.DataType(ChangeSchema.CqlType.BOOLEAN), null, null),
            new ChangeSchema.ColumnDefinition("cdc$deleted_v2", 3, new ChangeSchema.DataType(ChangeSchema.CqlType.BOOLEAN), null, null),
            new ChangeSchema.ColumnDefinition("cdc$deleted_v3", 3, new ChangeSchema.DataType(ChangeSchema.CqlType.BOOLEAN), null, null),
            new ChangeSchema.ColumnDefinition("cdc$deleted_v4", 3, new ChangeSchema.DataType(ChangeSchema.CqlType.BOOLEAN), null, null),
            new ChangeSchema.ColumnDefinition("cdc$end_of_batch", 4, new ChangeSchema.DataType(ChangeSchema.CqlType.BOOLEAN), null, null),
            new ChangeSchema.ColumnDefinition("cdc$operation", 5, new ChangeSchema.DataType(ChangeSchema.CqlType.TINYINT), null, null),
            new ChangeSchema.ColumnDefinition("cdc$ttl", 6, new ChangeSchema.DataType(ChangeSchema.CqlType.BIGINT), null, null),
            new ChangeSchema.ColumnDefinition("ck", 7, new ChangeSchema.DataType(ChangeSchema.CqlType.INT), new ChangeSchema.DataType(ChangeSchema.CqlType.INT), ChangeSchema.ColumnKind.CLUSTERING_KEY),
            new ChangeSchema.ColumnDefinition("pk", 8, new ChangeSchema.DataType(ChangeSchema.CqlType.INT), new ChangeSchema.DataType(ChangeSchema.CqlType.INT), ChangeSchema.ColumnKind.PARTITION_KEY),
            new ChangeSchema.ColumnDefinition("v", 9, FROZEN_SET_INT, FROZEN_SET_INT, ChangeSchema.ColumnKind.REGULAR),
            new ChangeSchema.ColumnDefinition("v2", 10, FROZEN_LIST_INT, FROZEN_LIST_INT, ChangeSchema.ColumnKind.REGULAR),
            new ChangeSchema.ColumnDefinition("v3", 11, FROZEN_MAP_DOUBLE_TEXT, FROZEN_MAP_DOUBLE_TEXT, ChangeSchema.ColumnKind.REGULAR),
            new ChangeSchema.ColumnDefinition("v4", 12, FROZEN_TUPLE_INET_INT, FROZEN_TUPLE_INET_INT, ChangeSchema.ColumnKind.REGULAR)
    ));

    // CDC table for:
    // CREATE TABLE ks.nonfrozen_collections(pk int, ck int, v set<int>, v2 list<int>, v3 map<double, text>, v4 tuple<inet, int>, PRIMARY KEY(pk, ck)) WITH cdc = {'enabled': true};
    //
    // CREATE TABLE ks.nonfrozen_collections_scylla_cdc_log (
    //    "cdc$stream_id" blob,
    //    "cdc$time" timeuuid,
    //    "cdc$batch_seq_no" int,
    //    "cdc$deleted_elements_v" frozen<set<int>>,
    //    "cdc$deleted_elements_v2" frozen<set<timeuuid>>,
    //    "cdc$deleted_elements_v3" frozen<set<double>>,
    //    "cdc$deleted_v" boolean,
    //    "cdc$deleted_v2" boolean,
    //    "cdc$deleted_v3" boolean,
    //    "cdc$deleted_v4" boolean,
    //    "cdc$end_of_batch" boolean,
    //    "cdc$operation" tinyint,
    //    "cdc$ttl" bigint,
    //    ck int,
    //    pk int,
    //    v frozen<set<int>>,
    //    v2 frozen<map<timeuuid, int>>,
    //    v3 frozen<map<double, text>>,
    //    v4 frozen<tuple<inet, int>>,
    //    PRIMARY KEY ("cdc$stream_id", "cdc$time", "cdc$batch_seq_no")
    // )
    public static final ChangeSchema.DataType FROZEN_SET_TIMEUUID = new ChangeSchema.DataType(ChangeSchema.CqlType.SET, Collections.singletonList(new ChangeSchema.DataType(ChangeSchema.CqlType.TIMEUUID)), true);
    public static final ChangeSchema.DataType FROZEN_SET_DOUBLE = new ChangeSchema.DataType(ChangeSchema.CqlType.SET, Collections.singletonList(new ChangeSchema.DataType(ChangeSchema.CqlType.DOUBLE)), true);

    public static final ChangeSchema.DataType NONFROZEN_SET_INT = new ChangeSchema.DataType(ChangeSchema.CqlType.SET, Collections.singletonList(new ChangeSchema.DataType(ChangeSchema.CqlType.INT)), false);
    public static final ChangeSchema.DataType NONFROZEN_LIST_INT = new ChangeSchema.DataType(ChangeSchema.CqlType.LIST, Collections.singletonList(new ChangeSchema.DataType(ChangeSchema.CqlType.INT)), false);
    public static final ChangeSchema.DataType NONFROZEN_MAP_DOUBLE_TEXT = new ChangeSchema.DataType(ChangeSchema.CqlType.MAP, Lists.newArrayList(
            new ChangeSchema.DataType(ChangeSchema.CqlType.DOUBLE), new ChangeSchema.DataType(ChangeSchema.CqlType.TEXT)), false);
    public static final ChangeSchema.DataType NONFROZEN_TUPLE_INET_INT = new ChangeSchema.DataType(ChangeSchema.CqlType.TUPLE, Lists.newArrayList(
            new ChangeSchema.DataType(ChangeSchema.CqlType.INET), new ChangeSchema.DataType(ChangeSchema.CqlType.INT)), false);

    public static final ChangeSchema TEST_SCHEMA_NONFROZEN_COLLECTIONS = new ChangeSchema(Lists.newArrayList(
            new ChangeSchema.ColumnDefinition("cdc$stream_id", 0, new ChangeSchema.DataType(ChangeSchema.CqlType.BLOB), null, null),
            new ChangeSchema.ColumnDefinition("cdc$time", 1, new ChangeSchema.DataType(ChangeSchema.CqlType.TIMEUUID), null, null),
            new ChangeSchema.ColumnDefinition("cdc$batch_seq_no", 2, new ChangeSchema.DataType(ChangeSchema.CqlType.INT), null, null),
            new ChangeSchema.ColumnDefinition("cdc$deleted_elements_v", 3, FROZEN_SET_INT, null, null),
            new ChangeSchema.ColumnDefinition("cdc$deleted_elements_v2", 4, FROZEN_SET_TIMEUUID, null, null),
            new ChangeSchema.ColumnDefinition("cdc$deleted_elements_v3", 5, FROZEN_SET_DOUBLE, null, null),
            new ChangeSchema.ColumnDefinition("cdc$deleted_v", 6, new ChangeSchema.DataType(ChangeSchema.CqlType.BOOLEAN), null, null),
            new ChangeSchema.ColumnDefinition("cdc$deleted_v2", 7, new ChangeSchema.DataType(ChangeSchema.CqlType.BOOLEAN), null, null),
            new ChangeSchema.ColumnDefinition("cdc$deleted_v3", 8, new ChangeSchema.DataType(ChangeSchema.CqlType.BOOLEAN), null, null),
            new ChangeSchema.ColumnDefinition("cdc$deleted_v4", 9, new ChangeSchema.DataType(ChangeSchema.CqlType.BOOLEAN), null, null),
            new ChangeSchema.ColumnDefinition("cdc$end_of_batch", 10, new ChangeSchema.DataType(ChangeSchema.CqlType.BOOLEAN), null, null),
            new ChangeSchema.ColumnDefinition("cdc$operation", 11, new ChangeSchema.DataType(ChangeSchema.CqlType.TINYINT), null, null),
            new ChangeSchema.ColumnDefinition("cdc$ttl", 12, new ChangeSchema.DataType(ChangeSchema.CqlType.BIGINT), null, null),
            new ChangeSchema.ColumnDefinition("ck", 13, new ChangeSchema.DataType(ChangeSchema.CqlType.INT), new ChangeSchema.DataType(ChangeSchema.CqlType.INT), ChangeSchema.ColumnKind.CLUSTERING_KEY),
            new ChangeSchema.ColumnDefinition("pk", 14, new ChangeSchema.DataType(ChangeSchema.CqlType.INT), new ChangeSchema.DataType(ChangeSchema.CqlType.INT), ChangeSchema.ColumnKind.PARTITION_KEY),
            new ChangeSchema.ColumnDefinition("v", 15, FROZEN_SET_INT, NONFROZEN_SET_INT, ChangeSchema.ColumnKind.REGULAR),
            new ChangeSchema.ColumnDefinition("v2", 16, FROZEN_LIST_INT, NONFROZEN_LIST_INT, ChangeSchema.ColumnKind.REGULAR),
            new ChangeSchema.ColumnDefinition("v3", 17, FROZEN_MAP_DOUBLE_TEXT, NONFROZEN_MAP_DOUBLE_TEXT, ChangeSchema.ColumnKind.REGULAR),
            new ChangeSchema.ColumnDefinition("v4", 18, FROZEN_TUPLE_INET_INT, NONFROZEN_TUPLE_INET_INT, ChangeSchema.ColumnKind.REGULAR)
    ));

    @Test
    public void testDataTypeIsAtomic() {
        // Simple types:

        // int
        assertTrue(TEST_SCHEMA_SIMPLE.getColumnDefinition("pk").getCdcLogDataType().isAtomic());
        // timeuuid
        assertTrue(TEST_SCHEMA_SIMPLE.getColumnDefinition("cdc$time").getCdcLogDataType().isAtomic());
        // blob
        assertTrue(TEST_SCHEMA_SIMPLE.getColumnDefinition("cdc$stream_id").getCdcLogDataType().isAtomic());

        // Frozen types:

        // frozen<set<int>>
        assertTrue(FROZEN_SET_INT.isAtomic());
        // frozen<list<int>>
        assertTrue(FROZEN_LIST_INT.isAtomic());
        // frozen<map<double, text>>
        assertTrue(FROZEN_MAP_DOUBLE_TEXT.isAtomic());
        // frozen<tuple<inet, int>>
        assertTrue(FROZEN_TUPLE_INET_INT.isAtomic());

        // Nonfrozen types:

        // set<int>
        assertFalse(NONFROZEN_SET_INT.isAtomic());
        // list<int>
        assertFalse(NONFROZEN_LIST_INT.isAtomic());
        // map<double, text>
        assertFalse(NONFROZEN_MAP_DOUBLE_TEXT.isAtomic());
        // tuple<inet, int>
        assertFalse(NONFROZEN_TUPLE_INET_INT.isAtomic());
    }

    @Test
    public void testSchemaGetDeletedColumnDefinition() {
        ChangeSchema.ColumnDefinition deletedV = TEST_SCHEMA_SIMPLE.getDeletedColumnDefinition("v");
        assertEquals("cdc$deleted_v", deletedV.getColumnName());
        assertEquals(ChangeSchema.CqlType.BOOLEAN, deletedV.getCdcLogDataType().getCqlType());
        assertTrue(deletedV.isCdcColumn());

        ChangeSchema.ColumnDefinition v3 = TEST_SCHEMA_FROZEN_COLLECTIONS.getColumnDefinition("v3");
        ChangeSchema.ColumnDefinition deletedV3 = TEST_SCHEMA_FROZEN_COLLECTIONS.getDeletedColumnDefinition(v3);
        assertEquals("cdc$deleted_v3", deletedV3.getColumnName());

        // Clustering keys don't have a cdc$deleted_ column.
        assertThrows(IllegalArgumentException.class, () -> TEST_SCHEMA_FROZEN_COLLECTIONS.getDeletedColumnDefinition("ck"));
    }

    @Test
    public void testSchemaGetDeletedElementsColumnDefinition() {
        // v set<int>
        // cdc$deleted_elements_v frozen<set<int>>
        ChangeSchema.ColumnDefinition deletedElementsV =
                TEST_SCHEMA_NONFROZEN_COLLECTIONS.getDeletedElementsColumnDefinition("v");
        assertEquals("cdc$deleted_elements_v", deletedElementsV.getColumnName());
        assertEquals(ChangeSchema.CqlType.SET, deletedElementsV.getCdcLogDataType().getCqlType());
        assertTrue(deletedElementsV.getCdcLogDataType().isFrozen());
        assertEquals(ChangeSchema.CqlType.INT, deletedElementsV.getCdcLogDataType().getTypeArguments().get(0).getCqlType());

        // v3 frozen<map<double, text>>
        // no cdc$deleted_elements_v3
        ChangeSchema.ColumnDefinition v3 = TEST_SCHEMA_FROZEN_COLLECTIONS.getColumnDefinition("v3");
        assertThrows(IllegalArgumentException.class, () -> TEST_SCHEMA_FROZEN_COLLECTIONS.getDeletedElementsColumnDefinition(v3));
    }
}
