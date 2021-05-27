package com.scylladb.cdc.cql.driver3;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.google.common.collect.Lists;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.RawChange;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class Driver3SchemaFactoryIT extends BaseScyllaIntegrationTest {
    @Test
    public void testColumnKindSchemaGeneration() throws ExecutionException, InterruptedException, TimeoutException {
        // Create a simple table with a few columns
        // in the primary key. Insert a single row
        // to that table and check the resulting schema
        // built by Driver3SchemaFactory for the
        // column kinds (whether a column is a
        // clustering key, partition key, regular key).

        driverSession.execute("CREATE TABLE ks.column_kinds(pk1 int, pk2 int, pk3 int, " +
                "ck1 int, ck2 int, ck3 int, col1 int, col2 int, " +
                "PRIMARY KEY((pk1, pk2, pk3), ck1, ck2, ck3)) WITH cdc = {'enabled': true}");

        // Insert a single row.
        driverSession.execute("INSERT INTO ks.column_kinds(pk1, pk2, pk3, " +
                "ck1, ck2, ck3) VALUES (1, 1, 1, 1, 1, 1)");

        // Read the inserted row.
        RawChange change = getFirstRawChange(new TableName("ks", "column_kinds"));
        ChangeSchema changeSchema = change.getSchema();

        // Partition keys
        assertEquals(ChangeSchema.ColumnKind.PARTITION_KEY,
                changeSchema.getColumnDefinition("pk1").getBaseTableColumnKind());
        assertEquals(ChangeSchema.ColumnKind.PARTITION_KEY,
                changeSchema.getColumnDefinition("pk2").getBaseTableColumnKind());
        assertEquals(ChangeSchema.ColumnKind.PARTITION_KEY,
                changeSchema.getColumnDefinition("pk3").getBaseTableColumnKind());

        // Clustering keys
        assertEquals(ChangeSchema.ColumnKind.CLUSTERING_KEY,
                changeSchema.getColumnDefinition("ck1").getBaseTableColumnKind());
        assertEquals(ChangeSchema.ColumnKind.CLUSTERING_KEY,
                changeSchema.getColumnDefinition("ck2").getBaseTableColumnKind());
        assertEquals(ChangeSchema.ColumnKind.CLUSTERING_KEY,
                changeSchema.getColumnDefinition("ck3").getBaseTableColumnKind());

        // Regular keys
        assertEquals(ChangeSchema.ColumnKind.REGULAR,
                changeSchema.getColumnDefinition("col1").getBaseTableColumnKind());
        assertEquals(ChangeSchema.ColumnKind.REGULAR,
                changeSchema.getColumnDefinition("col2").getBaseTableColumnKind());
    }

    @Test
    public void testNativeTypesSchemaGeneration() throws ExecutionException, InterruptedException, TimeoutException {
        // Create a table with all "native" types
        // (non-collection types). Insert a single row
        // to that table and check the resulting schema
        // built by Driver3SchemaFactory.

        DataType[] driverTypes = new DataType[] {
            DataType.ascii(),
            DataType.bigint(),
            DataType.blob(),
            DataType.cboolean(),
            DataType.date(),
            DataType.decimal(),
            DataType.cdouble(),
            DataType.duration(),
            DataType.cfloat(),
            DataType.inet(),
            DataType.cint(),
            DataType.smallint(),
            DataType.text(),
            DataType.time(),
            DataType.timestamp(),
            DataType.timeuuid(),
            DataType.tinyint(),
            DataType.uuid(),
            DataType.varchar(),
            DataType.varint()
        };
        ChangeSchema.DataType[] libraryTypes = new ChangeSchema.DataType[] {
                new ChangeSchema.DataType(ChangeSchema.CqlType.ASCII),
                new ChangeSchema.DataType(ChangeSchema.CqlType.BIGINT),
                new ChangeSchema.DataType(ChangeSchema.CqlType.BLOB),
                new ChangeSchema.DataType(ChangeSchema.CqlType.BOOLEAN),
                new ChangeSchema.DataType(ChangeSchema.CqlType.DATE),
                new ChangeSchema.DataType(ChangeSchema.CqlType.DECIMAL),
                new ChangeSchema.DataType(ChangeSchema.CqlType.DOUBLE),
                new ChangeSchema.DataType(ChangeSchema.CqlType.DURATION),
                new ChangeSchema.DataType(ChangeSchema.CqlType.FLOAT),
                new ChangeSchema.DataType(ChangeSchema.CqlType.INET),
                new ChangeSchema.DataType(ChangeSchema.CqlType.INT),
                new ChangeSchema.DataType(ChangeSchema.CqlType.SMALLINT),
                new ChangeSchema.DataType(ChangeSchema.CqlType.VARCHAR), // VARCHAR is an alias for TEXT
                new ChangeSchema.DataType(ChangeSchema.CqlType.TIME),
                new ChangeSchema.DataType(ChangeSchema.CqlType.TIMESTAMP),
                new ChangeSchema.DataType(ChangeSchema.CqlType.TIMEUUID),
                new ChangeSchema.DataType(ChangeSchema.CqlType.TINYINT),
                new ChangeSchema.DataType(ChangeSchema.CqlType.UUID),
                new ChangeSchema.DataType(ChangeSchema.CqlType.VARCHAR),
                new ChangeSchema.DataType(ChangeSchema.CqlType.VARINT)
        };
        assertEquals(driverTypes.length, libraryTypes.length);

        // Create a table with a single partition key (pk)
        // and v{i} columns of all native types.
        Create createTable = SchemaBuilder.createTable("ks", "native_types");
        createTable.addPartitionKey("pk", DataType.cint());
        for (int i = 0; i < driverTypes.length; i++) {
            createTable.addColumn("v" + i, driverTypes[i]);
        }
        driverSession.execute(createTable.getQueryString() + " WITH cdc = {'enabled': true}");

        // Insert a single row.
        driverSession.execute("INSERT INTO ks.native_types(pk) VALUES (1)");

        // Read the inserted row.
        RawChange change = getFirstRawChange(new TableName("ks", "native_types"));
        ChangeSchema changeSchema = change.getSchema();

        ChangeSchema.ColumnDefinition pk = changeSchema.getColumnDefinition("pk");

        // pk should be a partition key with INT type in both base table
        // and CDC log table.
        assertEquals(ChangeSchema.ColumnKind.PARTITION_KEY, pk.getBaseTableColumnKind());
        assertEquals(ChangeSchema.CqlType.INT, pk.getBaseTableDataType().getCqlType());
        assertEquals(ChangeSchema.CqlType.INT, pk.getCdcLogDataType().getCqlType());

        for (int i = 0; i < libraryTypes.length; i++) {
            // v{i} column should have the correct CDC log data type,
            // the same as in the base table.
            ChangeSchema.ColumnDefinition column = changeSchema.getColumnDefinition("v" + i);
            assertEquals(libraryTypes[i], column.getCdcLogDataType());
            assertEquals(libraryTypes[i], column.getBaseTableDataType());

            // cdc$deleted_v{i} should be of BOOLEAN type and without
            // any information about the base table type.
            ChangeSchema.ColumnDefinition deletedColumn = changeSchema.getDeletedColumnDefinition("v" + i);
            assertEquals(ChangeSchema.CqlType.BOOLEAN, deletedColumn.getCdcLogDataType().getCqlType());
            assertThrows(IllegalStateException.class, deletedColumn::getBaseTableDataType);
            assertThrows(IllegalStateException.class, deletedColumn::getBaseTableColumnKind);
        }
    }

    @Test
    public void testFrozenCollectionsSchemaGeneration() throws ExecutionException, InterruptedException, TimeoutException {
        // Create a table with frozen collections: lists,
        // sets, maps, tuples, UDTs. Insert a single row
        // to that table and check the resulting schema
        // built by Driver3SchemaFactory.

        driverSession.execute("CREATE TYPE ks.my_udt(b int, a int, c int)");
        driverSession.execute("CREATE TABLE ks.frozen_collections(pk int, " +
                "v1 frozen<list<varchar>>, v2 frozen<set<double>>," +
                "v3 frozen<map<inet, int>>, v4 frozen<my_udt>," +
                "v5 frozen<tuple<varchar, int, float>>, PRIMARY KEY(pk)) WITH cdc = {'enabled': true}");

        // Insert a single row.
        driverSession.execute("INSERT INTO ks.frozen_collections(pk) VALUES (1)");

        // Read the inserted row.
        RawChange change = getFirstRawChange(new TableName("ks", "frozen_collections"));
        ChangeSchema changeSchema = change.getSchema();

        // Frozen collections are atomic types.
        // Therefore, they are handled the same way as
        // native types - base table type of collection column
        // is the same as CDC table type of collection column.

        // frozen<list<varchar>>
        ChangeSchema.ColumnDefinition v1 = changeSchema.getColumnDefinition("v1");
        ChangeSchema.DataType v1DataType = new ChangeSchema.DataType(ChangeSchema.CqlType.LIST,
                Collections.singletonList(new ChangeSchema.DataType(ChangeSchema.CqlType.VARCHAR)), true);
        assertEquals(v1DataType, v1.getBaseTableDataType());
        assertEquals(v1DataType, v1.getCdcLogDataType());

        // frozen<set<double>>
        ChangeSchema.ColumnDefinition v2 = changeSchema.getColumnDefinition("v2");
        ChangeSchema.DataType v2DataType = new ChangeSchema.DataType(ChangeSchema.CqlType.SET,
                Collections.singletonList(new ChangeSchema.DataType(ChangeSchema.CqlType.DOUBLE)), true);
        assertEquals(v2DataType, v2.getBaseTableDataType());
        assertEquals(v2DataType, v2.getCdcLogDataType());

        // frozen<map<inet, int>>
        ChangeSchema.ColumnDefinition v3 = changeSchema.getColumnDefinition("v3");
        ChangeSchema.DataType v3DataType = new ChangeSchema.DataType(ChangeSchema.CqlType.MAP,
                Lists.newArrayList(new ChangeSchema.DataType(ChangeSchema.CqlType.INET),
                        new ChangeSchema.DataType(ChangeSchema.CqlType.INT)), true);
        assertEquals(v3DataType, v3.getBaseTableDataType());
        assertEquals(v3DataType, v3.getCdcLogDataType());

        // frozen<my_udt>
        ChangeSchema.ColumnDefinition v4 = changeSchema.getColumnDefinition("v4");

        Map<String, ChangeSchema.DataType> udtFields = new LinkedHashMap<>();
        udtFields.put("b", new ChangeSchema.DataType(ChangeSchema.CqlType.INT));
        udtFields.put("a", new ChangeSchema.DataType(ChangeSchema.CqlType.INT));
        udtFields.put("c", new ChangeSchema.DataType(ChangeSchema.CqlType.INT));

        ChangeSchema.DataType.UdtType udtType = new ChangeSchema.DataType.UdtType(udtFields, "ks", "my_udt");
        ChangeSchema.DataType v4DataType = new ChangeSchema.DataType(ChangeSchema.CqlType.UDT, udtType, true);
        assertEquals(v4DataType, v4.getBaseTableDataType());
        assertEquals(v4DataType, v4.getCdcLogDataType());

        // frozen<tuple<varchar, int, float>>
        ChangeSchema.ColumnDefinition v5 = changeSchema.getColumnDefinition("v5");
        ChangeSchema.DataType v5DataType = new ChangeSchema.DataType(ChangeSchema.CqlType.TUPLE,
                Lists.newArrayList(new ChangeSchema.DataType(ChangeSchema.CqlType.VARCHAR),
                        new ChangeSchema.DataType(ChangeSchema.CqlType.INT),
                        new ChangeSchema.DataType(ChangeSchema.CqlType.FLOAT)), true);
        assertEquals(v5DataType, v5.getBaseTableDataType());
        assertEquals(v5DataType, v5.getCdcLogDataType());
    }

    @Test
    public void testNonFrozenCollectionsSchemaGeneration() throws ExecutionException, InterruptedException, TimeoutException {
        // Create a table with non-frozen collections: lists,
        // sets, maps, tuples, UDTs. Insert a single row
        // to that table and check the resulting schema
        // built by Driver3SchemaFactory.

        driverSession.execute("CREATE TYPE ks.my_udt(b int, a int, c int)");
        driverSession.execute("CREATE TABLE ks.nonfrozen_collections(pk int, " +
                "v1 list<varchar>, v2 set<double>, " +
                "v3 map<inet, int>, v4 my_udt, " +
                "v5 tuple<varchar, int, float>, " +
                "v6 map<timeuuid, varchar>, " + // map<timeuuid, varchar> and list<varchar> are the same type in CDC table
                " PRIMARY KEY(pk)) WITH cdc = {'enabled': true}");

        // Insert a single row.
        driverSession.execute("INSERT INTO ks.nonfrozen_collections(pk) VALUES (1)");

        // Read the inserted row.
        RawChange change = getFirstRawChange(new TableName("ks", "nonfrozen_collections"));
        ChangeSchema changeSchema = change.getSchema();

        // Base table: list<varchar>>
        // CDC table:  frozen<map<timeuuid, varchar>>
        ChangeSchema.ColumnDefinition v1 = changeSchema.getColumnDefinition("v1");
        ChangeSchema.DataType v1BaseDataType = new ChangeSchema.DataType(ChangeSchema.CqlType.LIST,
                Collections.singletonList(new ChangeSchema.DataType(ChangeSchema.CqlType.VARCHAR)), false);
        ChangeSchema.DataType v1CdcDataType = new ChangeSchema.DataType(ChangeSchema.CqlType.MAP,
                Lists.newArrayList(new ChangeSchema.DataType(ChangeSchema.CqlType.TIMEUUID),
                        new ChangeSchema.DataType(ChangeSchema.CqlType.VARCHAR)), true);
        assertEquals(v1BaseDataType, v1.getBaseTableDataType());
        assertEquals(v1CdcDataType, v1.getCdcLogDataType());

        // Also check the cdc$deleted_elements_ column,
        // which should be frozen<set<timeuuid>>
        ChangeSchema.ColumnDefinition v1DeletedElements = changeSchema.getDeletedElementsColumnDefinition("v1");
        ChangeSchema.DataType v1DeletedElementsType = new ChangeSchema.DataType(ChangeSchema.CqlType.SET,
                Collections.singletonList(new ChangeSchema.DataType(ChangeSchema.CqlType.TIMEUUID)), true);
        assertEquals(v1DeletedElementsType, v1DeletedElements.getCdcLogDataType());

        // Base table: set<double>
        // CDC table:  frozen<set<double>>
        ChangeSchema.ColumnDefinition v2 = changeSchema.getColumnDefinition("v2");
        ChangeSchema.DataType v2BaseDataType = new ChangeSchema.DataType(ChangeSchema.CqlType.SET,
                Collections.singletonList(new ChangeSchema.DataType(ChangeSchema.CqlType.DOUBLE)), false);
        ChangeSchema.DataType v2CdcDataType = new ChangeSchema.DataType(ChangeSchema.CqlType.SET,
                Collections.singletonList(new ChangeSchema.DataType(ChangeSchema.CqlType.DOUBLE)), true);
        assertEquals(v2BaseDataType, v2.getBaseTableDataType());
        assertEquals(v2CdcDataType, v2.getCdcLogDataType());

        // Base table: map<inet, int>
        // CDC table: frozen<map<inet, int>>
        ChangeSchema.ColumnDefinition v3 = changeSchema.getColumnDefinition("v3");
        ChangeSchema.DataType v3BaseDataType = new ChangeSchema.DataType(ChangeSchema.CqlType.MAP,
                Lists.newArrayList(new ChangeSchema.DataType(ChangeSchema.CqlType.INET),
                        new ChangeSchema.DataType(ChangeSchema.CqlType.INT)), false);
        ChangeSchema.DataType v3CdcDataType = new ChangeSchema.DataType(ChangeSchema.CqlType.MAP,
                Lists.newArrayList(new ChangeSchema.DataType(ChangeSchema.CqlType.INET),
                        new ChangeSchema.DataType(ChangeSchema.CqlType.INT)), true);
        assertEquals(v3BaseDataType, v3.getBaseTableDataType());
        assertEquals(v3CdcDataType, v3.getCdcLogDataType());

        // Base table: my_udt
        // CDC table:  frozen<my_udt>
        ChangeSchema.ColumnDefinition v4 = changeSchema.getColumnDefinition("v4");

        Map<String, ChangeSchema.DataType> udtFields = new LinkedHashMap<>();
        udtFields.put("b", new ChangeSchema.DataType(ChangeSchema.CqlType.INT));
        udtFields.put("a", new ChangeSchema.DataType(ChangeSchema.CqlType.INT));
        udtFields.put("c", new ChangeSchema.DataType(ChangeSchema.CqlType.INT));

        ChangeSchema.DataType.UdtType udtType = new ChangeSchema.DataType.UdtType(udtFields, "ks", "my_udt");
        ChangeSchema.DataType v4BaseDataType = new ChangeSchema.DataType(ChangeSchema.CqlType.UDT, udtType, false);
        ChangeSchema.DataType v4CdcDataType = new ChangeSchema.DataType(ChangeSchema.CqlType.UDT, udtType, true);
        assertEquals(v4BaseDataType, v4.getBaseTableDataType());
        assertEquals(v4CdcDataType, v4.getCdcLogDataType());

        // Currently in Scylla, creating a table
        // with a non-frozen tuple (tuple<varchar, int, float>)
        // automatically transforms it into creating a frozen tuple
        // (both base table and CDC log table).
        //
        // Check that assumption below.
        ChangeSchema.ColumnDefinition v5 = changeSchema.getColumnDefinition("v5");
        ChangeSchema.DataType v5DataType = new ChangeSchema.DataType(ChangeSchema.CqlType.TUPLE,
                Lists.newArrayList(new ChangeSchema.DataType(ChangeSchema.CqlType.VARCHAR),
                        new ChangeSchema.DataType(ChangeSchema.CqlType.INT),
                        new ChangeSchema.DataType(ChangeSchema.CqlType.FLOAT)), true);
        assertEquals(v5DataType, v5.getBaseTableDataType());
        assertEquals(v5DataType, v5.getCdcLogDataType());

        // Both map<timeuuid, varchar> and list<varchar> in the
        // base table are transformed to frozen<map<timeuuid, varchar>> in
        // the CDC log table. Check that Driver3SchemaFactory can
        // correctly distinguish those cases (list case already
        // tested above).

        // Base table: map<timeuuid, varchar>
        // CDC table:  frozen<map<timeuuid, varchar>>
        ChangeSchema.ColumnDefinition v6 = changeSchema.getColumnDefinition("v6");
        ChangeSchema.DataType v6BaseDataType = new ChangeSchema.DataType(ChangeSchema.CqlType.MAP,
                Lists.newArrayList(new ChangeSchema.DataType(ChangeSchema.CqlType.TIMEUUID),
                        new ChangeSchema.DataType(ChangeSchema.CqlType.VARCHAR)), false);
        ChangeSchema.DataType v6CdcDataType = new ChangeSchema.DataType(ChangeSchema.CqlType.MAP,
                Lists.newArrayList(new ChangeSchema.DataType(ChangeSchema.CqlType.TIMEUUID),
                        new ChangeSchema.DataType(ChangeSchema.CqlType.VARCHAR)), true);
        assertEquals(v6BaseDataType, v6.getBaseTableDataType());
        assertEquals(v6CdcDataType, v6.getCdcLogDataType());
    }

    @Test
    public void testNestedTypesSchemaGeneration() throws ExecutionException, InterruptedException, TimeoutException {
        // Create a table with a column with nested
        // type. Insert a single row to that table
        // and check the resulting schema built
        // by Driver3SchemaFactory.

        driverSession.execute("CREATE TYPE ks.my_udt(b int, a int, c int)");
        driverSession.execute("CREATE TYPE ks.my_udt2(x double, y frozen<my_udt>)");
        driverSession.execute("CREATE TABLE ks.nested_type(pk int, " +
                "v1 map<frozen<set<double>>, frozen<list<list<my_udt2>>>>," +
                " PRIMARY KEY(pk)) WITH cdc = {'enabled': true}");

        // Insert a single row.
        driverSession.execute("INSERT INTO ks.nested_type(pk) VALUES (1)");

        // Read the inserted row.
        RawChange change = getFirstRawChange(new TableName("ks", "nested_type"));
        ChangeSchema changeSchema = change.getSchema();

        // Construct the expected types:
        Map<String, ChangeSchema.DataType> myUdtFields = new LinkedHashMap<>();
        myUdtFields.put("b", new ChangeSchema.DataType(ChangeSchema.CqlType.INT));
        myUdtFields.put("a", new ChangeSchema.DataType(ChangeSchema.CqlType.INT));
        myUdtFields.put("c", new ChangeSchema.DataType(ChangeSchema.CqlType.INT));
        ChangeSchema.DataType frozenMyUdtType = new ChangeSchema.DataType(ChangeSchema.CqlType.UDT,
                new ChangeSchema.DataType.UdtType(myUdtFields, "ks", "my_udt"), true);

        Map<String, ChangeSchema.DataType> myUdt2Fields = new LinkedHashMap<>();
        myUdt2Fields.put("x", new ChangeSchema.DataType(ChangeSchema.CqlType.DOUBLE));
        myUdt2Fields.put("y", frozenMyUdtType);
        ChangeSchema.DataType frozenMyUdt2Type = new ChangeSchema.DataType(ChangeSchema.CqlType.UDT,
                new ChangeSchema.DataType.UdtType(myUdt2Fields, "ks", "my_udt2"), true);

        ChangeSchema.DataType frozenSetDouble = new ChangeSchema.DataType(ChangeSchema.CqlType.SET,
                Collections.singletonList(new ChangeSchema.DataType(ChangeSchema.CqlType.DOUBLE)), true);

        ChangeSchema.DataType frozenListFrozenMyUdt2 = new ChangeSchema.DataType(ChangeSchema.CqlType.LIST,
                Collections.singletonList(frozenMyUdt2Type), true);
        ChangeSchema.DataType frozenListFrozenListFrozenMyUdt2 = new ChangeSchema.DataType(ChangeSchema.CqlType.LIST,
                Collections.singletonList(frozenListFrozenMyUdt2), true);

        ChangeSchema.DataType topLevelMap = new ChangeSchema.DataType(ChangeSchema.CqlType.MAP,
                Lists.newArrayList(frozenSetDouble, frozenListFrozenListFrozenMyUdt2), false);
        ChangeSchema.DataType frozenTopLevelMap = new ChangeSchema.DataType(topLevelMap, true);

        // Base table type of v1 after running CREATE TABLE,
        // as Scylla automatically freezes inner types:
        // map<frozen<set<double>>, frozen<list<frozen<list<frozen<my_udt2>>>>>>
        assertEquals(topLevelMap, changeSchema.getColumnDefinition("v1").getBaseTableDataType());

        // CDC table type of v1:
        // frozen<map<frozen<set<double>>, frozen<list<frozen<list<frozen<my_udt2>>>>>>>
        assertEquals(frozenTopLevelMap, changeSchema.getColumnDefinition("v1").getCdcLogDataType());
    }
}
