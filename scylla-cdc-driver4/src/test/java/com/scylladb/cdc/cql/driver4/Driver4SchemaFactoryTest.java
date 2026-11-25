package com.scylladb.cdc.cql.driver4;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.scylladb.cdc.cql.driver4.Driver4SchemaFactory.UnresolvableSchemaInconsistencyException;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.ChangeSchema.ColumnKind;
import com.scylladb.cdc.model.worker.ChangeSchema.CqlType;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the schema inconsistencies the integration test cannot set up. The driver metadata are
 * {@link Proxy} fakes of the public interfaces: the project has no mocking library.
 */
public class Driver4SchemaFactoryTest {
    private static final DataType FROZEN_MAP_TIMEUUID_TEXT = DataTypes.frozenMapOf(DataTypes.TIMEUUID, DataTypes.TEXT);
    private static final ChangeSchema.DataType TEXT = new ChangeSchema.DataType(CqlType.VARCHAR);

    @Test
    public void testRenamedClusteringColumnKeepsItsKind() throws Exception {
        // The CDC log schema still has ck_old, the base schema already has ck_new.
        TableMetadata newBase = table(Arrays.asList(column("p", DataTypes.INT)),
                Arrays.asList(column("ck_new", DataTypes.INT)), Arrays.asList(column("v", DataTypes.INT)));
        ChangeSchema schema = Driver4SchemaFactory.getChangeSchema(Arrays.asList(
                cdcColumn("p", DataTypes.INT), cdcColumn("ck_old", DataTypes.INT), cdcColumn("v", DataTypes.INT)), newBase);
        assertKinds(schema, ColumnKind.PARTITION_KEY, ColumnKind.CLUSTERING_KEY, ColumnKind.REGULAR);

        // The CDC log schema already has ck_new, the base schema still has ck_old.
        TableMetadata oldBase = table(Arrays.asList(column("p", DataTypes.INT)),
                Arrays.asList(column("ck_old", DataTypes.INT)), Arrays.asList(column("v", DataTypes.INT)));
        schema = Driver4SchemaFactory.getChangeSchema(Arrays.asList(
                cdcColumn("p", DataTypes.INT), cdcColumn("ck_new", DataTypes.INT), cdcColumn("v", DataTypes.INT)), oldBase);
        assertKinds(schema, ColumnKind.PARTITION_KEY, ColumnKind.CLUSTERING_KEY, ColumnKind.REGULAR);
    }

    @Test
    public void testColumnMissingFromTheBaseIsRegular() throws Exception {
        TableMetadata base = table(Arrays.asList(column("p", DataTypes.INT)),
                Arrays.asList(column("ck", DataTypes.INT)), Collections.emptyList());
        ChangeSchema schema = Driver4SchemaFactory.getChangeSchema(Arrays.asList(
                cdcColumn("p", DataTypes.INT), cdcColumn("ck", DataTypes.INT), cdcColumn("added", DataTypes.INT)), base);
        assertKinds(schema, ColumnKind.PARTITION_KEY, ColumnKind.CLUSTERING_KEY, ColumnKind.REGULAR);
    }

    @Test
    public void testNonFrozenListAndMapOfTimeuuidAreTold() throws Exception {
        // The column name needs quotes, so a lookup through its CQL name lowercases it.
        List<ColumnDefinition> cdcColumns = Arrays.asList(cdcColumn("p", DataTypes.INT),
                cdcColumn("V", FROZEN_MAP_TIMEUUID_TEXT), cdcColumn("cdc$deleted_elements_V", DataTypes.frozenSetOf(DataTypes.TIMEUUID)));

        ChangeSchema list = Driver4SchemaFactory.getChangeSchema(cdcColumns, tableWithV(DataTypes.listOf(DataTypes.TEXT)));
        assertEquals(new ChangeSchema.DataType(CqlType.LIST, Collections.singletonList(TEXT), false),
                list.getColumnDefinition("V").getBaseTableDataType());

        ChangeSchema map = Driver4SchemaFactory.getChangeSchema(cdcColumns,
                tableWithV(DataTypes.mapOf(DataTypes.TIMEUUID, DataTypes.TEXT)));
        assertEquals(new ChangeSchema.DataType(Driver4SchemaFactory.translateCdcColumnDataType(FROZEN_MAP_TIMEUUID_TEXT), false),
                map.getColumnDefinition("V").getBaseTableDataType());
    }

    @Test
    public void testFrozenMapOfTimeuuidDoesNotReadTheBase() throws Exception {
        // Without cdc$deleted_elements_V the column is frozen, so a changed base column is fine.
        ChangeSchema schema = Driver4SchemaFactory.getChangeSchema(Arrays.asList(
                cdcColumn("p", DataTypes.INT), cdcColumn("V", FROZEN_MAP_TIMEUUID_TEXT)), tableWithV(DataTypes.BLOB));
        assertEquals(Driver4SchemaFactory.translateCdcColumnDataType(FROZEN_MAP_TIMEUUID_TEXT),
                schema.getColumnDefinition("V").getBaseTableDataType());
    }

    @Test
    public void testUnknownListnessThrows() {
        List<ColumnDefinition> cdcColumns = Arrays.asList(cdcColumn("p", DataTypes.INT),
                cdcColumn("V", FROZEN_MAP_TIMEUUID_TEXT), cdcColumn("cdc$deleted_elements_V", DataTypes.frozenSetOf(DataTypes.TIMEUUID)));

        // V was dropped from the base after the CDC log row was written.
        UnresolvableSchemaInconsistencyException missing = assertThrows(UnresolvableSchemaInconsistencyException.class,
                () -> Driver4SchemaFactory.getChangeSchema(cdcColumns, table(Arrays.asList(column("p", DataTypes.INT)),
                        Collections.emptyList(), Collections.emptyList())));
        assertTrue(missing.getMessage().startsWith("Column 'V' in base table metadata is missing"), missing.getMessage());

        // V was dropped and re-added with another type.
        UnresolvableSchemaInconsistencyException changed = assertThrows(UnresolvableSchemaInconsistencyException.class,
                () -> Driver4SchemaFactory.getChangeSchema(cdcColumns, tableWithV(DataTypes.setOf(DataTypes.TEXT))));
        assertTrue(changed.getMessage().startsWith("The type of column 'V' in base table metadata is set<text>"),
                changed.getMessage());
        assertThrows(UnresolvableSchemaInconsistencyException.class, () -> Driver4SchemaFactory.getChangeSchema(cdcColumns,
                tableWithV(DataTypes.mapOf(DataTypes.UUID, DataTypes.TEXT))));
    }

    @Test
    public void testMissingKeyspaceOrTableThrows() {
        Row row = row(Arrays.asList(cdcColumn("p", DataTypes.INT)));

        UnresolvableSchemaInconsistencyException keyspace = assertThrows(UnresolvableSchemaInconsistencyException.class,
                () -> Driver4SchemaFactory.getChangeSchema(row, metadata(Collections.emptyMap())));
        assertEquals("Keyspace 'Ks' is missing from the cluster metadata.", keyspace.getMessage());

        UnresolvableSchemaInconsistencyException table = assertThrows(UnresolvableSchemaInconsistencyException.class,
                () -> Driver4SchemaFactory.getChangeSchema(row, metadata(Collections.singletonMap(
                        CqlIdentifier.fromInternal("Ks"), keyspace(Collections.emptyMap())))));
        assertEquals("Table 'Tbl' is missing from the keyspace 'Ks' metadata.", table.getMessage());
    }

    @Test
    public void testCaseSensitiveNamesFindTheBaseTable() throws Exception {
        // Keyspace "Ks" and table "Tbl" need quotes, so a lookup through CQL names lowercases them.
        TableMetadata base = table(Arrays.asList(column("p", DataTypes.INT)), Collections.emptyList(), Collections.emptyList());
        ChangeSchema schema = Driver4SchemaFactory.getChangeSchema(row(Arrays.asList(cdcColumn("p", DataTypes.INT))),
                metadata(Collections.singletonMap(CqlIdentifier.fromInternal("Ks"),
                        keyspace(Collections.singletonMap(CqlIdentifier.fromInternal("Tbl"), base)))));
        assertEquals(ColumnKind.PARTITION_KEY, schema.getColumnDefinition("p").getBaseTableColumnKind());
    }

    @Test
    public void testTypesOnlyThisTestReaches() {
        assertEquals(new ChangeSchema.DataType(CqlType.COUNTER), Driver4SchemaFactory.translateCdcColumnDataType(DataTypes.COUNTER));
        RuntimeException custom = assertThrows(RuntimeException.class,
                () -> Driver4SchemaFactory.translateCdcColumnDataType(DataTypes.custom("com.example.Custom")));
        assertEquals("Data type 'com.example.Custom' is currently not supported.", custom.getMessage());
    }

    private static void assertKinds(ChangeSchema schema, ColumnKind... kinds) {
        assertEquals(kinds.length, schema.getAllColumnDefinitions().size());
        for (int i = 0; i < kinds.length; i++) {
            ChangeSchema.ColumnDefinition column = schema.getAllColumnDefinitions().get(i);
            assertEquals(i, column.getIndex());
            assertEquals(kinds[i], column.getBaseTableColumnKind(), column.getColumnName());
        }
    }

    private static TableMetadata tableWithV(DataType type) {
        return table(Arrays.asList(column("p", DataTypes.INT)), Collections.emptyList(), Arrays.asList(column("V", type)));
    }

    private static TableMetadata table(List<ColumnMetadata> partitionKey, List<ColumnMetadata> clusteringColumns,
            List<ColumnMetadata> regularColumns) {
        Map<ColumnMetadata, ClusteringOrder> clustering = new LinkedHashMap<>();
        clusteringColumns.forEach(c -> clustering.put(c, ClusteringOrder.ASC));
        Map<CqlIdentifier, ColumnMetadata> columns = new LinkedHashMap<>();
        for (List<ColumnMetadata> group : Arrays.asList(partitionKey, clusteringColumns, regularColumns)) {
            group.forEach(c -> columns.put(c.getName(), c));
        }
        return fake(TableMetadata.class, method -> {
            switch (method) {
                case "getPartitionKey": return args -> partitionKey;
                case "getClusteringColumns": return args -> clustering;
                case "getColumns": return args -> columns;
                default: return null;
            }
        });
    }

    private static ColumnMetadata column(String name, DataType type) {
        return fake(ColumnMetadata.class, method -> {
            switch (method) {
                case "getName": return args -> CqlIdentifier.fromInternal(name);
                case "getType": return args -> type;
                default: return null;
            }
        });
    }

    private static ColumnDefinition cdcColumn(String name, DataType type) {
        return fake(ColumnDefinition.class, method -> {
            switch (method) {
                case "getKeyspace": return args -> CqlIdentifier.fromInternal("Ks");
                case "getTable": return args -> CqlIdentifier.fromInternal("Tbl_scylla_cdc_log");
                case "getName": return args -> CqlIdentifier.fromInternal(name);
                case "getType": return args -> type;
                default: return null;
            }
        });
    }

    private static Row row(List<ColumnDefinition> columns) {
        ColumnDefinitions definitions = fake(ColumnDefinitions.class,
                method -> method.equals("iterator") ? args -> columns.iterator() : null);
        return fake(Row.class, method -> method.equals("getColumnDefinitions") ? args -> definitions : null);
    }

    private static Metadata metadata(Map<CqlIdentifier, KeyspaceMetadata> keyspaces) {
        return fake(Metadata.class, method -> method.equals("getKeyspace")
                ? args -> Optional.ofNullable(keyspaces.get((CqlIdentifier) args[0])) : null);
    }

    private static KeyspaceMetadata keyspace(Map<CqlIdentifier, TableMetadata> tables) {
        return fake(KeyspaceMetadata.class, method -> method.equals("getTable")
                ? args -> Optional.ofNullable(tables.get((CqlIdentifier) args[0])) : null);
    }

    /**
     * Returns a fake that answers the methods {@code answers} maps and fails on any other, so a
     * call the factory should not make shows up. Overloads taking a {@code String} name fail too.
     */
    private static <T> T fake(Class<T> type, Function<String, Function<Object[], Object>> answers) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, (proxy, method, args) -> {
            if (method.getName().equals("toString") && method.getParameterCount() == 0) {
                return type.getSimpleName() + " fake";
            } else if (method.getName().equals("hashCode") && method.getParameterCount() == 0) {
                return System.identityHashCode(proxy);
            } else if (method.getName().equals("equals") && method.getParameterCount() == 1) {
                return proxy == args[0];
            }
            Function<Object[], Object> answer = answers.apply(method.getName());
            boolean stringOverload = args != null && Arrays.stream(method.getParameterTypes()).anyMatch(String.class::equals);
            if (answer == null || stringOverload) {
                throw new UnsupportedOperationException(type.getSimpleName() + "." + method.getName());
            }
            return answer.apply(args);
        }));
    }
}
