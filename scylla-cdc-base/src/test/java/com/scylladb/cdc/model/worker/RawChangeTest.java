package com.scylladb.cdc.model.worker;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.scylladb.cdc.model.worker.cql.Field;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Iterator;

import static com.scylladb.cdc.model.worker.ChangeSchemaTest.*;
import static com.scylladb.cdc.model.worker.RawChange.OperationType.ROW_INSERT;
import static com.scylladb.cdc.model.worker.RawChange.OperationType.ROW_UPDATE;
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

    @Test
    public void testGetDeletedElements() {
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

        // v is not a nonfrozen collection, so it doesn't have
        // a cdc$deleted_elements_ column.
        assertThrows(IllegalStateException.class, () -> insert1.getDeletedElements("v"));

        // INSERT INTO ks.nonfrozen_collections(pk, ck, v, v2) VALUES (1, 2, {3, 5}, null);
        MockRawChange insert2 = MockRawChange.builder()
                .withChangeSchema(TEST_SCHEMA_NONFROZEN_COLLECTIONS)
                .withStreamId("0xc73400000000000058fe971e700004f1")
                .withTime("0df7f766-af3c-11eb-0ec0-cad5080ba1d6")
                .withOperation(ROW_INSERT)
                .addPrimaryKey("pk", 1)
                .addPrimaryKey("ck", 2)
                .addNonfrozenSetRegularColumnOverwrite("v", Sets.newHashSet(3, 5), new ChangeSchema.DataType(ChangeSchema.CqlType.INT))
                .addNonfrozenListRegularColumnOverwrite("v2", null, new ChangeSchema.DataType(ChangeSchema.CqlType.INT))
                .build();

        // This INSERT only overwrites the column values, so
        // we expect the cdc$deleted_elements_ to be empty.
        ChangeSchema.ColumnDefinition colV = insert2.getSchema().getColumnDefinition("v");
        assertEquals(Collections.emptySet(), insert2.getDeletedElements(colV));
        assertEquals(Collections.emptySet(), insert2.getDeletedElements("v2"));

        // UPDATE ks.nonfrozen_collections SET v = v - {3, 4} WHERE pk = 1 AND ck = 1;
        MockRawChange update = MockRawChange.builder()
                .withChangeSchema(TEST_SCHEMA_NONFROZEN_COLLECTIONS)
                .withStreamId("0xc417fed9f65a8577e5ac04d700000411")
                .withTime("ba188a00-b249-11eb-f06a-3815701ff8c4")
                .withOperation(ROW_UPDATE)
                .addPrimaryKey("pk", 1)
                .addPrimaryKey("ck", 1)
                .addNonfrozenSetRegularColumnDelete("v", Sets.newLinkedHashSet(Lists.newArrayList(3, 4)), new ChangeSchema.DataType(ChangeSchema.CqlType.INT))
                .build();

        // There should be a cdc$deleted_elements_ column with two
        // values: 3, 4.
        assertEquals(2, update.getDeletedElements("v").size());

        Iterator<Field> deletedElements = update.getDeletedElements("v").iterator();
        Field firstDeletedElement = deletedElements.next();
        assertEquals(ChangeSchema.CqlType.INT, firstDeletedElement.getDataType().getCqlType());
        assertEquals(3, firstDeletedElement.getInt());

        Field secondDeletedElement = deletedElements.next();
        assertEquals(ChangeSchema.CqlType.INT, secondDeletedElement.getDataType().getCqlType());
        assertEquals(4, secondDeletedElement.getInt());
    }
}
