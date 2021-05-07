package com.scylladb.cdc.model.worker;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

import static com.scylladb.cdc.model.worker.ChangeSchemaTest.*;
import static com.scylladb.cdc.model.worker.RawChange.OperationType.ROW_INSERT;
import static org.junit.jupiter.api.Assertions.*;

public class RawChangeTest {
    @Test
    public void testIsDeleted() {
        // INSERT INTO ks.simple(pk, ck, v) VALUES (1, 2, 3);
        MockRawChange insert1 = MockRawChange.builder()
                .withChangeSchema(TEST_SCHEMA_SIMPLE)
                .withStreamId("0xc73400000000000058fe971e700004f1")
                .withTime("fc7ee7b6-af35-11eb-0443-36839c176f26")
                .withOperation(ROW_INSERT)
                .addPrimaryKey("pk", 1)
                .addPrimaryKey("ck", 2)
                .addAtomicRegularColumn("v", 3)
                .build();

        assertFalse(insert1.isDeleted("v"));
        assertThrows(IllegalArgumentException.class, () -> insert1.isDeleted("pk"));
        assertThrows(IllegalStateException.class, () -> insert1.isDeleted("cdc$operation"));

        // INSERT INTO ks.simple(pk, ck, v) VALUES (1, 2, NULL);
        MockRawChange insert2 = MockRawChange.builder()
                .withChangeSchema(TEST_SCHEMA_SIMPLE)
                .withStreamId("0xc73400000000000058fe971e700004f1")
                .withTime("d2b8ea72-af39-11eb-aaf9-4e300aa0a9a0")
                .withOperation(ROW_INSERT)
                .addPrimaryKey("pk", 1)
                .addPrimaryKey("ck", 2)
                .addAtomicRegularColumn("v", null)
                .build();

        assertTrue(insert2.isDeleted("v"));

        // INSERT INTO ks.simple(pk, ck) VALUES (1, 2);
        MockRawChange insert3 = MockRawChange.builder()
                .withChangeSchema(TEST_SCHEMA_SIMPLE)
                .withStreamId("0xc73400000000000058fe971e700004f1")
                .withTime("098d1744-af3a-11eb-c6af-998af699dbeb")
                .withOperation(ROW_INSERT)
                .addPrimaryKey("pk", 1)
                .addPrimaryKey("ck", 2)
                .build();

        ChangeSchema.ColumnDefinition vColumnDefinition = insert3.getSchema().getColumnDefinition("v");
        assertFalse(insert3.isDeleted(vColumnDefinition));
        assertTrue(insert3.isNull("v"));

        // INSERT INTO ks.frozen_collections(pk, ck, v, v2) VALUES (1, 2, {3, 5}, null);
        MockRawChange insert4 = MockRawChange.builder()
                .withChangeSchema(TEST_SCHEMA_FROZEN_COLLECTIONS)
                .withStreamId("0xc73400000000000058fe971e700004f1")
                .withTime("9989568c-af3a-11eb-e1eb-533f0734bcd9")
                .withOperation(ROW_INSERT)
                .addPrimaryKey("pk", 1)
                .addPrimaryKey("ck", 2)
                .addFrozenSetRegularColumn("v", Sets.newHashSet(3, 5), new ChangeSchema.DataType(ChangeSchema.CqlType.INT))
                .addFrozenSetRegularColumn("v2", null, new ChangeSchema.DataType(ChangeSchema.CqlType.INT))
                .build();

        assertFalse(insert4.isDeleted("v"));
        assertTrue(insert4.isDeleted("v2"));
        // Not affected columns:
        assertFalse(insert4.isDeleted("v3"));
        assertFalse(insert4.isDeleted("v4"));

        // INSERT INTO ks.nonfrozen_collections(pk, ck, v, v2) VALUES (1, 2, {3, 5}, null);
        MockRawChange insert5 = MockRawChange.builder()
                .withChangeSchema(TEST_SCHEMA_NONFROZEN_COLLECTIONS)
                .withStreamId("0xc73400000000000058fe971e700004f1")
                .withTime("0df7f766-af3c-11eb-0ec0-cad5080ba1d6")
                .withOperation(ROW_INSERT)
                .addPrimaryKey("pk", 1)
                .addPrimaryKey("ck", 2)
                .addNonfrozenSetRegularColumnOverwrite("v", Sets.newHashSet(3, 5), new ChangeSchema.DataType(ChangeSchema.CqlType.INT))
                .addNonfrozenListRegularColumnOverwrite("v2", null, new ChangeSchema.DataType(ChangeSchema.CqlType.INT))
                .build();

        // v, v2 are deleted because those are non-frozen collection overwrites
        assertTrue(insert5.isDeleted("v"));
        assertTrue(insert5.isDeleted("v2"));
        // Not affected columns:
        assertFalse(insert5.isDeleted("v3"));
        assertFalse(insert5.isDeleted("v4"));
    }
}
