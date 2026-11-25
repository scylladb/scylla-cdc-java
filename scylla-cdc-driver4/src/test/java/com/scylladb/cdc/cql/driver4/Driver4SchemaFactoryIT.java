package com.scylladb.cdc.cql.driver4;

import com.google.common.collect.Lists;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.worker.ChangeSchema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The port of {@code Driver3SchemaFactoryIT}.
 */
@Tag("integration")
public class Driver4SchemaFactoryIT extends BaseScyllaIntegrationTest {
    @Test
    public void testColumnKindSchemaGeneration() throws Exception {
        driverSession.execute("CREATE TABLE ks.column_kinds(pk1 int, pk2 int, pk3 int, "
                + "ck1 int, ck2 int, ck3 int, col1 int, col2 int, "
                + "PRIMARY KEY((pk1, pk2, pk3), ck1, ck2, ck3)) WITH cdc = {'enabled': true}");
        driverSession.execute("INSERT INTO ks.column_kinds(pk1, pk2, pk3, ck1, ck2, ck3) VALUES (1, 1, 1, 1, 1, 1)");

        ChangeSchema changeSchema = getFirstChangeSchema("column_kinds");

        for (String column : new String[] {"pk1", "pk2", "pk3"}) {
            assertEquals(ChangeSchema.ColumnKind.PARTITION_KEY, changeSchema.getColumnDefinition(column).getBaseTableColumnKind());
        }
        for (String column : new String[] {"ck1", "ck2", "ck3"}) {
            assertEquals(ChangeSchema.ColumnKind.CLUSTERING_KEY, changeSchema.getColumnDefinition(column).getBaseTableColumnKind());
        }
        for (String column : new String[] {"col1", "col2"}) {
            assertEquals(ChangeSchema.ColumnKind.REGULAR, changeSchema.getColumnDefinition(column).getBaseTableColumnKind());
        }
    }

    @Test
    public void testNativeTypesSchemaGeneration() throws Exception {
        String[] cqlTypes = new String[] {"ascii", "bigint", "blob", "boolean", "date", "decimal", "double", "duration",
                "float", "inet", "int", "smallint", "text", "time", "timestamp", "timeuuid", "tinyint", "uuid", "varchar",
                "varint"};
        ChangeSchema.CqlType[] libraryTypes = new ChangeSchema.CqlType[] {
                ChangeSchema.CqlType.ASCII,
                ChangeSchema.CqlType.BIGINT,
                ChangeSchema.CqlType.BLOB,
                ChangeSchema.CqlType.BOOLEAN,
                ChangeSchema.CqlType.DATE,
                ChangeSchema.CqlType.DECIMAL,
                ChangeSchema.CqlType.DOUBLE,
                ChangeSchema.CqlType.DURATION,
                ChangeSchema.CqlType.FLOAT,
                ChangeSchema.CqlType.INET,
                ChangeSchema.CqlType.INT,
                ChangeSchema.CqlType.SMALLINT,
                ChangeSchema.CqlType.VARCHAR, // VARCHAR is an alias for TEXT
                ChangeSchema.CqlType.TIME,
                ChangeSchema.CqlType.TIMESTAMP,
                ChangeSchema.CqlType.TIMEUUID,
                ChangeSchema.CqlType.TINYINT,
                ChangeSchema.CqlType.UUID,
                ChangeSchema.CqlType.VARCHAR,
                ChangeSchema.CqlType.VARINT
        };
        assertEquals(cqlTypes.length, libraryTypes.length);

        StringBuilder createTable = new StringBuilder("CREATE TABLE ks.native_types(pk int PRIMARY KEY");
        for (int i = 0; i < cqlTypes.length; i++) {
            createTable.append(", v").append(i).append(' ').append(cqlTypes[i]);
        }
        driverSession.execute(createTable.append(") WITH cdc = {'enabled': true}").toString());
        driverSession.execute("INSERT INTO ks.native_types(pk) VALUES (1)");

        ChangeSchema changeSchema = getFirstChangeSchema("native_types");

        ChangeSchema.ColumnDefinition pk = changeSchema.getColumnDefinition("pk");
        assertEquals(ChangeSchema.ColumnKind.PARTITION_KEY, pk.getBaseTableColumnKind());
        assertEquals(ChangeSchema.CqlType.INT, pk.getBaseTableDataType().getCqlType());
        assertEquals(ChangeSchema.CqlType.INT, pk.getCdcLogDataType().getCqlType());

        for (int i = 0; i < libraryTypes.length; i++) {
            ChangeSchema.DataType libraryType = new ChangeSchema.DataType(libraryTypes[i]);
            ChangeSchema.ColumnDefinition column = changeSchema.getColumnDefinition("v" + i);
            assertEquals(libraryType, column.getCdcLogDataType(), cqlTypes[i]);
            assertEquals(libraryType, column.getBaseTableDataType(), cqlTypes[i]);

            ChangeSchema.ColumnDefinition deletedColumn = changeSchema.getDeletedColumnDefinition("v" + i);
            assertEquals(ChangeSchema.CqlType.BOOLEAN, deletedColumn.getCdcLogDataType().getCqlType());
            assertThrows(IllegalStateException.class, deletedColumn::getBaseTableDataType);
            assertThrows(IllegalStateException.class, deletedColumn::getBaseTableColumnKind);
        }
    }

    @Test
    public void testFrozenCollectionsSchemaGeneration() throws Exception {
        driverSession.execute("CREATE TYPE ks.my_udt(b int, a int, c int)");
        driverSession.execute("CREATE TABLE ks.frozen_collections(pk int, "
                + "v1 frozen<list<varchar>>, v2 frozen<set<double>>,"
                + "v3 frozen<map<inet, int>>, v4 frozen<my_udt>,"
                + "v5 frozen<tuple<varchar, int, float>>, PRIMARY KEY(pk)) WITH cdc = {'enabled': true}");
        driverSession.execute("INSERT INTO ks.frozen_collections(pk) VALUES (1)");

        ChangeSchema changeSchema = getFirstChangeSchema("frozen_collections");

        // Frozen collections are atomic, so the base table type is the CDC log type.
        assertBothTypes(changeSchema, "v1", new ChangeSchema.DataType(ChangeSchema.CqlType.LIST,
                Collections.singletonList(new ChangeSchema.DataType(ChangeSchema.CqlType.VARCHAR)), true));
        assertBothTypes(changeSchema, "v2", new ChangeSchema.DataType(ChangeSchema.CqlType.SET,
                Collections.singletonList(new ChangeSchema.DataType(ChangeSchema.CqlType.DOUBLE)), true));
        assertBothTypes(changeSchema, "v3", new ChangeSchema.DataType(ChangeSchema.CqlType.MAP,
                Lists.newArrayList(new ChangeSchema.DataType(ChangeSchema.CqlType.INET),
                        new ChangeSchema.DataType(ChangeSchema.CqlType.INT)), true));
        assertBothTypes(changeSchema, "v4", new ChangeSchema.DataType(ChangeSchema.CqlType.UDT, myUdtType(), true));
        // The translator reads UDT values by position, so the field order is part of the type.
        assertEquals(Arrays.asList("b", "a", "c"), new ArrayList<>(
                changeSchema.getColumnDefinition("v4").getCdcLogDataType().getUdtType().getFields().keySet()));
        assertBothTypes(changeSchema, "v5", new ChangeSchema.DataType(ChangeSchema.CqlType.TUPLE,
                Lists.newArrayList(new ChangeSchema.DataType(ChangeSchema.CqlType.VARCHAR),
                        new ChangeSchema.DataType(ChangeSchema.CqlType.INT),
                        new ChangeSchema.DataType(ChangeSchema.CqlType.FLOAT)), true));
    }

    @Test
    public void testNonFrozenCollectionsSchemaGeneration() throws Exception {
        driverSession.execute("CREATE TYPE ks.my_udt(b int, a int, c int)");
        driverSession.execute("CREATE TABLE ks.nonfrozen_collections(pk int, "
                + "v1 list<varchar>, v2 set<double>, "
                + "v3 map<inet, int>, v4 my_udt, "
                + "v5 tuple<varchar, int, float>, "
                + "v6 map<timeuuid, varchar>, " // map<timeuuid, varchar> and list<varchar> are the same type in CDC table
                + " PRIMARY KEY(pk)) WITH cdc = {'enabled': true}");
        driverSession.execute("INSERT INTO ks.nonfrozen_collections(pk) VALUES (1)");

        ChangeSchema changeSchema = getFirstChangeSchema("nonfrozen_collections");
        ChangeSchema.DataType mapTimeuuidVarchar = new ChangeSchema.DataType(ChangeSchema.CqlType.MAP,
                Lists.newArrayList(new ChangeSchema.DataType(ChangeSchema.CqlType.TIMEUUID),
                        new ChangeSchema.DataType(ChangeSchema.CqlType.VARCHAR)), true);

        // Base table: list<varchar>, CDC table: frozen<map<timeuuid, varchar>>
        assertTypes(changeSchema, "v1", new ChangeSchema.DataType(ChangeSchema.CqlType.LIST,
                Collections.singletonList(new ChangeSchema.DataType(ChangeSchema.CqlType.VARCHAR)), false), mapTimeuuidVarchar);
        assertEquals(new ChangeSchema.DataType(ChangeSchema.CqlType.SET,
                        Collections.singletonList(new ChangeSchema.DataType(ChangeSchema.CqlType.TIMEUUID)), true),
                changeSchema.getDeletedElementsColumnDefinition("v1").getCdcLogDataType());

        ChangeSchema.DataType setDouble = new ChangeSchema.DataType(ChangeSchema.CqlType.SET,
                Collections.singletonList(new ChangeSchema.DataType(ChangeSchema.CqlType.DOUBLE)), true);
        assertTypes(changeSchema, "v2", new ChangeSchema.DataType(setDouble, false), setDouble);

        ChangeSchema.DataType mapInetInt = new ChangeSchema.DataType(ChangeSchema.CqlType.MAP,
                Lists.newArrayList(new ChangeSchema.DataType(ChangeSchema.CqlType.INET),
                        new ChangeSchema.DataType(ChangeSchema.CqlType.INT)), true);
        assertTypes(changeSchema, "v3", new ChangeSchema.DataType(mapInetInt, false), mapInetInt);

        assertTypes(changeSchema, "v4", new ChangeSchema.DataType(ChangeSchema.CqlType.UDT, myUdtType(), false),
                new ChangeSchema.DataType(ChangeSchema.CqlType.UDT, myUdtType(), true));

        // ScyllaDB freezes a non-frozen tuple, in both the base table and the CDC log.
        assertBothTypes(changeSchema, "v5", new ChangeSchema.DataType(ChangeSchema.CqlType.TUPLE,
                Lists.newArrayList(new ChangeSchema.DataType(ChangeSchema.CqlType.VARCHAR),
                        new ChangeSchema.DataType(ChangeSchema.CqlType.INT),
                        new ChangeSchema.DataType(ChangeSchema.CqlType.FLOAT)), true));

        // Base table: map<timeuuid, varchar>, CDC table: frozen<map<timeuuid, varchar>>
        assertTypes(changeSchema, "v6", new ChangeSchema.DataType(mapTimeuuidVarchar, false), mapTimeuuidVarchar);
    }

    @Test
    public void testNestedTypesSchemaGeneration() throws Exception {
        driverSession.execute("CREATE TYPE ks.my_udt(b int, a int, c int)");
        driverSession.execute("CREATE TYPE ks.my_udt2(x double, y frozen<my_udt>)");
        driverSession.execute("CREATE TABLE ks.nested_type(pk int, "
                + "v1 map<frozen<set<double>>, frozen<list<list<my_udt2>>>>,"
                + " PRIMARY KEY(pk)) WITH cdc = {'enabled': true}");
        driverSession.execute("INSERT INTO ks.nested_type(pk) VALUES (1)");

        ChangeSchema changeSchema = getFirstChangeSchema("nested_type");

        ChangeSchema.DataType frozenMyUdtType = new ChangeSchema.DataType(ChangeSchema.CqlType.UDT, myUdtType(), true);
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

        // ScyllaDB freezes the inner types:
        // map<frozen<set<double>>, frozen<list<frozen<list<frozen<my_udt2>>>>>>
        assertTypes(changeSchema, "v1", topLevelMap, new ChangeSchema.DataType(topLevelMap, true));
    }

    @Test
    public void testCaseSensitiveNamesSchemaGeneration() throws Exception {
        driverSession.execute("CREATE TABLE ks.\"Tbl\"(\"Pk\" int PRIMARY KEY, \"Vv\" list<int>) WITH cdc = {'enabled': true}");
        driverSession.execute("INSERT INTO ks.\"Tbl\"(\"Pk\", \"Vv\") VALUES (1, [1])");

        ChangeSchema changeSchema = getFirstChangeSchema("Tbl");

        assertEquals(ChangeSchema.ColumnKind.PARTITION_KEY, changeSchema.getColumnDefinition("Pk").getBaseTableColumnKind());
        assertTypes(changeSchema, "Vv", new ChangeSchema.DataType(ChangeSchema.CqlType.LIST,
                        Collections.singletonList(new ChangeSchema.DataType(ChangeSchema.CqlType.INT)), false),
                new ChangeSchema.DataType(ChangeSchema.CqlType.MAP, Lists.newArrayList(
                        new ChangeSchema.DataType(ChangeSchema.CqlType.TIMEUUID),
                        new ChangeSchema.DataType(ChangeSchema.CqlType.INT)), true));
    }

    private static ChangeSchema.DataType.UdtType myUdtType() {
        Map<String, ChangeSchema.DataType> fields = new LinkedHashMap<>();
        fields.put("b", new ChangeSchema.DataType(ChangeSchema.CqlType.INT));
        fields.put("a", new ChangeSchema.DataType(ChangeSchema.CqlType.INT));
        fields.put("c", new ChangeSchema.DataType(ChangeSchema.CqlType.INT));
        return new ChangeSchema.DataType.UdtType(fields, "ks", "my_udt");
    }

    private static void assertBothTypes(ChangeSchema changeSchema, String column, ChangeSchema.DataType type) {
        assertTypes(changeSchema, column, type, type);
    }

    private static void assertTypes(ChangeSchema changeSchema, String column, ChangeSchema.DataType baseTableType,
            ChangeSchema.DataType cdcLogType) {
        assertEquals(baseTableType, changeSchema.getColumnDefinition(column).getBaseTableDataType(), column);
        assertEquals(cdcLogType, changeSchema.getColumnDefinition(column).getCdcLogDataType(), column);
    }

    private ChangeSchema getFirstChangeSchema(String table) throws Exception {
        return getFirstRawChange(new TableName("ks", table)).getSchema();
    }
}
