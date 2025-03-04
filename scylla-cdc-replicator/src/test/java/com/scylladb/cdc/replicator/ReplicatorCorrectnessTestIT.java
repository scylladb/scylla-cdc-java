package com.scylladb.cdc.replicator;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.Select;
import com.scylladb.cdc.lib.CDCConsumer;
import com.scylladb.cdc.replicator.Main.Mode;
import com.scylladb.cdc.model.TableName;

import org.awaitility.core.ConditionFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.*;

import static org.awaitility.Awaitility.with;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.fail;

@Tag("integration")
public class ReplicatorCorrectnessTestIT extends BaseScyllaIntegrationTest {
      private static final String[] UDTsCreate = {
            "CREATE TYPE ks.udt_simple (a int, b int, c text)"
      };

      private static final long DEFAULT_AWAIT_TIMEOUT_MS = 30000;

      private static final ConditionFactory DEFAULT_AWAIT =
            with().pollInterval(200, TimeUnit.MILLISECONDS).await()
                    .atMost(DEFAULT_AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

      private Session srcSession;
      private Session dstSession;


      @BeforeAll
      public void prepare() {
            srcSession = buildLibrarySessionSrc();
            dstSession = buildLibrarySessionDst();

            for (String query : UDTsCreate) {
                  srcSession.execute(query);
                  dstSession.execute(query);
            }
      }

      static class Schema {
            public String keyspace;
            public String tableName;
            public String createTable;

            public Schema(String keyspace, String tableName, String createTable) {
                  this.keyspace = keyspace;
                  this.tableName = tableName;
                  this.createTable = createTable;
            }
      }

      static class TestData {
            public String testName;
            public Schema schema;
            public String[] queries;

            public TestData(String testName, Schema schema, String[] queries) {
                  this.testName = testName;
                  this.schema = schema;
                  this.queries = queries;
            }
      }

      private void prepareTables(TestData data) {
            srcSession.execute(SchemaBuilder.dropTable(data.schema.keyspace, data.schema.tableName).ifExists());
            dstSession.execute(SchemaBuilder.dropTable(data.schema.keyspace, data.schema.tableName).ifExists());

            srcSession.execute(data.schema.createTable + " WITH cdc = {'enabled': true, 'preimage': true, 'postimage': true}");
            dstSession.execute(data.schema.createTable);
      }

      private static final byte[] HEX_ARRAY = "0123456789ABCDEF".getBytes(StandardCharsets.US_ASCII);
      private static String bytesToHex(ByteBuffer bytes) {
            if (bytes == null) {
                  return "[]";
            }
            byte[] hexChars = new byte[bytes.remaining() * 2 + 2];
            int j = 0;
            hexChars[j] = '[';
            j += 1;
            while(bytes.hasRemaining()) {
                  byte v = bytes.get();
                  hexChars[j] = HEX_ARRAY[(v & 0xff) >>> 4];
                  hexChars[j + 1] = HEX_ARRAY[v & 0x0F];
                  j += 2;
            }
            hexChars[j] = ']';
            return new String(hexChars, StandardCharsets.UTF_8);
      }


      private Optional<String> verifyResult(Select select_all) {
            ResultSet srcRes = srcSession.execute(select_all);
            ResultSet dstRes = dstSession.execute(select_all);
            List<Row> srcRows = srcRes.all();
            List<Row> dstRows = dstRes.all();

            if (srcRows.size() != dstRows.size()) {
                  return Optional.of(String.format("Row amount not equal. Source: %d, destination: %d", srcRows.size(), dstRows.size()));
            }

            Iterator<Row> srcIt = srcRows.iterator();
            Iterator<Row> dstIt = dstRows.iterator();

            while(srcIt.hasNext() && dstIt.hasNext()) {
                  Row srcRow = srcIt.next();
                  Row dstRow = dstIt.next();
                  Optional<Definition> wrongColumnOptional = srcRow.getColumnDefinitions().asList().stream().filter(def -> {
                        ByteBuffer srcBytes = srcRow.getBytesUnsafe(def.getName());
                        ByteBuffer dstBytes = dstRow.getBytesUnsafe(def.getName());
                        return !Objects.equals(srcBytes, dstBytes);
                  }).findAny();
                  
                  if (wrongColumnOptional.isPresent()) {
                        Definition wrongColumn = wrongColumnOptional.get();
                        ByteBuffer srcBytes = srcRow.getBytesUnsafe(wrongColumn.getName());
                        ByteBuffer dstBytes = dstRow.getBytesUnsafe(wrongColumn.getName());
                        return Optional.of(String.format(
                                    "Invalid contents of column %s. Expected: %s, actual: %s", 
                                    wrongColumn.getName(), 
                                    bytesToHex(srcBytes), 
                                    bytesToHex(dstBytes)));
                  }
            }

            return Optional.empty();
      }

      private void performTest(TestData data, Mode mode) {
            System.out.println(data.testName);

            prepareTables(data);

            CDCConsumer consumer = CDCConsumer.builder()
            .addContactPoint(new InetSocketAddress(hostnameSrc, portSrc))
            .withConsumerProvider((threadId) ->
                    new ReplicatorConsumer(mode, dstSession.getCluster(), dstSession,
                            data.schema.keyspace, data.schema.tableName, ConsistencyLevel.QUORUM))
            .addTable(new TableName(data.schema.keyspace, data.schema.tableName))
            .withWorkersCount(1)
            .withQueryTimeWindowSizeMs(3000)
            .withConfidenceWindowSizeMs(3000)
            .build();

            Optional<Throwable> validation = consumer.validate();
            if(validation.isPresent()) {
                  fail("Validation error of the source table: " + validation.get().getMessage());
            }
            consumer.start();

            for (String query : data.queries) {
                  srcSession.execute(String.format(query, data.schema.keyspace + "." + data.schema.tableName));
            }

            Select select_all = QueryBuilder.select().from(data.schema.keyspace, data.schema.tableName);
            Optional<String> errorMessage;
            try {
                  errorMessage = DEFAULT_AWAIT.until(() -> verifyResult(select_all), (msg) -> !msg.isPresent());
            } catch (Exception e) {
                  StringWriter sw = new StringWriter();
                  PrintWriter pw = new PrintWriter(sw);
                  e.printStackTrace(pw);
                  errorMessage = Optional.of(sw.toString());
            }

            assertDoesNotThrow(() -> consumer.stop());

            if(errorMessage.isPresent()) {
                  fail(errorMessage.get());
            }
      }

      private static final Schema schemaSimple = new Schema(
            "ks",
            "tbl_simple",
            "CREATE TABLE ks.tbl_simple (pk text, ck int, v1 int, v2 text, PRIMARY KEY (pk, ck))"
      );

      private static final Schema schemaMultipleClusteringKeys = new Schema(
            "ks",
            "tbl_multiple_clustering_keys",
            "CREATE TABLE ks.tbl_multiple_clustering_keys (pk text, ck1 int, ck2 int, v int, PRIMARY KEY (pk, ck1, ck2))"
      );

      private static final Schema schemaBlobs = new Schema(
            "ks",
            "tbl_blobs",
            "CREATE TABLE ks.tbl_blobs (pk text, ck int, v blob, PRIMARY KEY (pk, ck))"
      );

      private static final Schema schemaLists = new Schema(
            "ks",
            "tbl_lists",
            "CREATE TABLE ks.tbl_lists (pk text, ck int, v list<int>, PRIMARY KEY(pk, ck))"
      );

      private static final Schema schemaSets = new Schema(
            "ks",
            "tbl_sets",
            "CREATE TABLE ks.tbl_sets (pk text, ck int, v set<int>, PRIMARY KEY (pk, ck))"
      );
      
      private static final Schema schemaMaps = new Schema(
            "ks",
            "tbl_maps",
            "CREATE TABLE ks.tbl_maps (pk text, ck int, v map<int, int>, PRIMARY KEY (pk, ck))"
      );

      private static final Schema schemaTuples = new Schema(
            "ks",
            "tbl_tuples",
            "CREATE TABLE ks.tbl_tuples (pk text, ck int, v tuple<int, text>, PRIMARY KEY (pk, ck))"
      );

      private static final Schema schemaTuplesInTuples = new Schema(
            "ks",
            "tbl_tuples_in_tuples",
            "CREATE TABLE ks.tbl_tuples_in_tuples (pk text, ck int, v tuple<tuple<int, text>, int>, PRIMARY KEY (pk, ck))"
      );

      private static final Schema schemaTuplesInTuplesInTuples = new Schema(
            "ks",
            "tbl_tuples_in_tuples_in_tuples",
            "CREATE TABLE ks.tbl_tuples_in_tuples_in_tuples (pk text, ck int, v tuple<tuple<tuple<int, int>, text>, int>, PRIMARY KEY (pk, ck))"
      );

      private static final Schema schemaUDTs = new Schema(
            "ks",
            "tbl_udts",
            "CREATE TABLE ks.tbl_udts (pk text, ck int, v ks.udt_simple, PRIMARY KEY (pk, ck))"
      );

      @ParameterizedTest
      @EnumSource(value = Mode.class)
      public void testSimpleInserts(Mode mode) {
            TestData data = new TestData(
                  "simpleInserts", 
                  schemaSimple,
                  new String[] {
                        "INSERT INTO %s (pk, ck, v1, v2) VALUES ('simpleInserts', 1, 2, 'abc')",
                        "INSERT INTO %s (pk, ck, v1) VALUES ('simpleInserts', 2, 3)",
                        "INSERT INTO %s (pk, ck, v2) VALUES ('simpleInserts', 2, 'def')",
                  }
            );

            performTest(data, mode);
      }

      @ParameterizedTest
      @EnumSource(value = Mode.class)
      public void testSimpleUpdates(Mode mode) {
            TestData data = new TestData(
                  "simpleUpdates", 
                  schemaSimple,
                  new String[] {
                        "UPDATE %s SET v1 = 1 WHERE pk = 'simpleUpdates' AND ck = 1",
                        "UPDATE %s SET v2 = 'abc' WHERE pk = 'simpleUpdates' AND ck = 2",
                        "UPDATE %s SET v1 = 5, v2 = 'def' WHERE pk = 'simpleUpdates' AND ck = 3",
                  }
            );

            performTest(data, mode);
      }

      @ParameterizedTest
      @EnumSource(value = Mode.class)
      public void testRowDeletes(Mode mode) {
            TestData data = new TestData(
                  "rowDeletes", 
                  schemaSimple,
                  new String[] {
                        "INSERT INTO %s (pk, ck, v1, v2) VALUES ('rowDeletes', 1, 2, 'abc')",
                        "INSERT INTO %s (pk, ck, v1, v2) VALUES ('rowDeletes', 2, 3, 'def')",
                        "DELETE FROM %s WHERE pk = 'rowDeletes' AND ck = 1",
                  }
            );

            performTest(data, mode);
      }

      @ParameterizedTest
      @EnumSource(value = Mode.class)
      public void testPartitionDeletes(Mode mode) {
            TestData data = new TestData(
                  "partitionDeletes", 
                  schemaSimple,
                  new String[] {
                        "INSERT INTO %s (pk, ck, v1, v2) VALUES ('partitionDeletes', 1, 2, 'abc')",
                        "INSERT INTO %s (pk, ck, v1, v2) VALUES ('partitionDeletes', 2, 3, 'def')",
                        "DELETE FROM %s WHERE pk = 'partitionDeletes'",
                        // Insert one more row, just to check if replication works at all
                        "INSERT INTO %s (pk, ck, v1, v2) VALUES ('partitionDeletes', 4, 5, 'def')",
                  }
            );

            performTest(data, mode);
      }

      @ParameterizedTest
      @EnumSource(value = Mode.class)
      public void testRangeDeletes(Mode mode) {
            TestData data = new TestData(
                  "rangeDeletes", 
                  schemaMultipleClusteringKeys,
                  new String[] {
                        "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('rangeDeletes', 1, 1, 0)",
                        "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('rangeDeletes', 1, 2, 0)",
                        "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('rangeDeletes', 1, 3, 0)",
                        "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('rangeDeletes', 1, 4, 0)",
                        "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('rangeDeletes', 2, 1, 0)",
                        "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('rangeDeletes', 2, 2, 0)",
                        "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('rangeDeletes', 2, 3, 0)",
                        "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('rangeDeletes', 2, 4, 0)",
                        "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('rangeDeletes', 3, 1, 0)",
                        "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('rangeDeletes', 3, 2, 0)",
                        "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('rangeDeletes', 3, 3, 0)",
                        "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('rangeDeletes', 3, 4, 0)",
                        "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('rangeDeletes', 4, 1, 0)",
                        "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('rangeDeletes', 4, 2, 0)",
                        "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('rangeDeletes', 4, 3, 0)",
                        "INSERT INTO %s (pk, ck1, ck2, v) VALUES ('rangeDeletes', 4, 4, 0)",
                        "DELETE FROM %s WHERE pk = 'rangeDeletes' AND ck1 > 3",
                        "DELETE FROM %s WHERE pk = 'rangeDeletes' AND ck1 <= 1",
                        "DELETE FROM %s WHERE pk = 'rangeDeletes' AND ck1 = 2 AND ck2 > 1 AND ck2 < 4",
                  }
            );

            performTest(data, mode);
      }

      @ParameterizedTest
      @EnumSource(value = Mode.class)
      public void testBlobs(Mode mode) {
            TestData data = new TestData(
                  "blobs", 
                  schemaBlobs,
                  new String[] {
                        "INSERT INTO %s (pk, ck, v) VALUES ('blobs', 1, 0x1234)",
                        "INSERT INTO %s (pk, ck, v) VALUES ('blobs', 2, 0x)",
                        "INSERT INTO %s (pk, ck, v) VALUES ('blobs', 3, null)",
                        "INSERT INTO %s (pk, ck, v) VALUES ('blobs', 4, 0x4321)",
                        "INSERT INTO %s (pk, ck, v) VALUES ('blobs', 5, 0x00)",
                        "UPDATE %s SET v = null WHERE pk = 'blobs' AND ck = 4",
                        "UPDATE %s SET v = 0x WHERE pk = 'blobs' AND ck = 5",
                  }
            );

            performTest(data, mode);
      }

      @ParameterizedTest
      @EnumSource(value = Mode.class)
      public void testListOverwrites(Mode mode) {
            TestData data = new TestData(
                  "listOverwrites", 
                  schemaLists,
                  new String[] {
                        "INSERT INTO %s (pk, ck, v) VALUES ('listOverwrites', 1, [1, 2, 3])",
                        "INSERT INTO %s (pk, ck, v) VALUES ('listOverwrites', 1, [4, 5, 6, 7])",
                        "INSERT INTO %s (pk, ck, v) VALUES ('listOverwrites', 2, [6, 5, 4, 3, 2, 1])",
                        "INSERT INTO %s (pk, ck, v) VALUES ('listOverwrites', 2, null)",
                        "INSERT INTO %s (pk, ck, v) VALUES ('listOverwrites', 3, [1, 11, 111])",
                        "UPDATE %s SET v = [2, 22, 222] WHERE pk = 'listOverwrites' AND ck = 3",
                  }
            );

            performTest(data, mode);
      }

      // @ParameterizedTest
      // @EnumSource(value = Mode.class, names = {"PRE_IMAGE"}, mode = EnumSource.Mode.EXCLUDE)
      // public void testListAppends(Mode mode) {
      //       TestData data = new TestData(
      //             "listOverwrites", 
      //             schemaLists,
      //             new String[] {
      //                   "INSERT INTO %s (pk, ck, v) VALUES ('listAppends', 1, [1, 2, 3])",
      //                   "UPDATE %s SET v = v + [4, 5, 6] WHERE pk = 'listAppends' AND ck = 1",
      //                   "UPDATE %s SET v = [-2, -1, 0] + v WHERE pk = 'listAppends' AND ck = 1",
      //             }
      //       );

      //       performTest(data, mode);
      // }

      // @ParameterizedTest
      // @EnumSource(value = Mode.class, names = {"PRE_IMAGE"}, mode = EnumSource.Mode.EXCLUDE)
      // public void testListRemoves(Mode mode) {
      //       TestData data = new TestData(
      //             "listRemoves", 
      //             schemaLists,
      //             new String[] {
      //                   "INSERT INTO %s (pk, ck, v) VALUES ('listRemoves', 1, [1, 2, 3])",
      //                   "UPDATE %s SET v = v + [4, 5, 6] WHERE pk = 'listRemoves' AND ck = 1",
      //                   "UPDATE %s SET v = v - [1, 2, 3] WHERE pk = 'listRemoves' AND ck = 1",
      //             }
      //       );

      //       performTest(data, mode);
      // }

      @ParameterizedTest
      @EnumSource(value = Mode.class)
      public void testSetOverwrites(Mode mode) {
            TestData data = new TestData(
                  "setOverwrites", 
                  schemaSets,
                  new String[] {
                        "INSERT INTO %s (pk, ck, v) VALUES ('setOverwrites', 1, {1, 2, 3, 4})",
                        "INSERT INTO %s (pk, ck, v) VALUES ('setOverwrites', 1, {4, 5, 6, 7})",
                        "INSERT INTO %s (pk, ck, v) VALUES ('setOverwrites', 2, {8, 9, 10, 11})",
                        "INSERT INTO %s (pk, ck, v) VALUES ('setOverwrites', 2, null)",
                        "INSERT INTO %s (pk, ck, v) VALUES ('setOverwrites', 3, {12, 13, 14, 15})",
                        "UPDATE %s SET v = null WHERE pk = 'setOverwrites' AND ck = 3",
                  }
            );

            performTest(data, mode);
      }

      @ParameterizedTest
      @EnumSource(value = Mode.class)
      public void testSetAppends(Mode mode) {
            TestData data = new TestData(
                  "setAppends", 
                  schemaSets,
                  new String[] {
                        "INSERT INTO %s (pk, ck, v) VALUES ('setAppends', 1, {1, 2, 3, 4})",
                        "UPDATE %s SET v = v + {5, 6} WHERE pk = 'setAppends' AND ck = 1",
                        "UPDATE %s SET v = v + {5, 6} WHERE pk = 'setAppends' AND ck = 2",
                  }
            );

            performTest(data, mode);
      }

      @ParameterizedTest
      @EnumSource(value = Mode.class)
      public void testSetRemovals(Mode mode) {
            TestData data = new TestData(
                  "setRemovals", 
                  schemaSets,
                  new String[] {
                        "INSERT INTO %s (pk, ck, v) VALUES ('setRemovals', 1, {1, 2, 3, 4})",
                        "UPDATE %s SET v = v - {1, 3} WHERE pk = 'setRemovals' AND ck = 1",
                        "UPDATE %s SET v = v - {1138} WHERE pk = 'setRemovals' AND ck = 2",
                  }
            );

            performTest(data, mode);
      }

      @ParameterizedTest
      @EnumSource(value = Mode.class)
      public void testMapOverwrites(Mode mode) {
            TestData data = new TestData(
                  "mapOverwrites", 
                  schemaMaps,
                  new String[] {
                        "INSERT INTO %s (pk, ck, v) VALUES ('mapOverwrites', 1, {1: 2, 3: 4})",
                        "INSERT INTO %s (pk, ck, v) VALUES ('mapOverwrites', 1, {5: 6, 7: 8})",
                        "INSERT INTO %s (pk, ck, v) VALUES ('mapOverwrites', 2, {9: 10, 11: 12})",
                        "INSERT INTO %s (pk, ck, v) VALUES ('mapOverwrites', 2, null)",
                        "INSERT INTO %s (pk, ck, v) VALUES ('mapOverwrites', 3, {13: 14, 15: 16})",
                        "UPDATE %s SET v = null WHERE pk = 'mapOverwrites' AND ck = 3",
                  }
            );

            performTest(data, mode);
      }

      @ParameterizedTest
      @EnumSource(value = Mode.class)
      public void testMapSets(Mode mode) {
            TestData data = new TestData(
                  "mapSets", 
                  schemaMaps,
                  new String[] {
                        "INSERT INTO %s (pk, ck, v) VALUES ('mapSets', 1, {1: 2, 3: 4, 5: 6})",
                        "UPDATE %s SET v[1] = 42 WHERE pk = 'mapSets' AND ck = 1",
                        "UPDATE %s SET v[3] = null WHERE pk = 'mapSets' AND ck = 1",
                        "UPDATE %s SET v[3] = 123 WHERE pk = 'mapSets' AND ck = 1",
                        "UPDATE %s SET v[5] = 321 WHERE pk = 'mapSets' AND ck = 2",
                  }
            );

            performTest(data, mode);
      }

      @ParameterizedTest
      @EnumSource(value = Mode.class)
      public void testMapAppends(Mode mode) {
            TestData data = new TestData(
                  "mapAppends", 
                  schemaMaps,
                  new String[] {
                        "INSERT INTO %s (pk, ck, v) VALUES ('mapAppends', 1, {1: 2, 3: 4})",
                        "UPDATE %s SET v = v + {5: 6} WHERE pk = 'mapAppends' AND ck = 1",
                        "UPDATE %s SET v = v + {5: 6} WHERE pk = 'mapAppends' AND ck = 2",
                  }
            );

            performTest(data, mode);
      }

      @ParameterizedTest
      @EnumSource(value = Mode.class)
      public void testMapRemovals(Mode mode) {
            TestData data = new TestData(
                  "mapRemovals", 
                  schemaMaps,
                  new String[] {
                        "INSERT INTO %s (pk, ck, v) VALUES ('mapRemovals', 1, {1: 2, 3: 4})",
                        "UPDATE %s SET v = v - {1} WHERE pk = 'mapRemovals' AND ck = 1",
                        "UPDATE %s SET v = v - {1138} WHERE pk = 'mapRemovals' AND ck = 2",
                  }
            );

            performTest(data, mode);
      }

      @ParameterizedTest
      @EnumSource(value = Mode.class)
      public void testTupleInserts(Mode mode) {
            TestData data = new TestData(
                  "tupleInserts", 
                  schemaTuples,
                  new String[] {
                        "INSERT INTO %s (pk, ck, v) VALUES ('tupleInserts', 1, (7, 'abc'))",
                        "INSERT INTO %s (pk, ck, v) VALUES ('tupleInserts', 2, (9, 'def'))",
                        "INSERT INTO %s (pk, ck, v) VALUES ('tupleInserts', 2, null)",
                  }
            );

            performTest(data, mode);
      }

      @ParameterizedTest
      @EnumSource(value = Mode.class)
      public void testTupleUpdates(Mode mode) {
            TestData data = new TestData(
                  "tupleUpdates", 
                  schemaTuples,
                  new String[] {
                        "INSERT INTO %s (pk, ck, v) VALUES ('tupleUpdates', 1, (7, 'abc'))",
                        "INSERT INTO %s (pk, ck, v) VALUES ('tupleUpdates', 2, (9, 'def'))",
                        "INSERT INTO %s (pk, ck, v) VALUES ('tupleUpdates', 3, (11, 'ghi'))",
                        "INSERT INTO %s (pk, ck, v) VALUES ('tupleUpdates', 4, (13, 'jkl'))",
                        "INSERT INTO %s (pk, ck, v) VALUES ('tupleUpdates', 5, (15, 'mno'))",
                        "INSERT INTO %s (pk, ck, v) VALUES ('tupleUpdates', 6, (17, 'pqr'))",
                        "INSERT INTO %s (pk, ck, v) VALUES ('tupleUpdates', 7, (19, 'stu'))",
                        "UPDATE %s SET v = (111, 'zyx') WHERE pk = 'tupleUpdates' AND ck = 1",
                        "UPDATE %s SET v = null WHERE pk = 'tupleUpdates' AND ck = 2",
                        "INSERT INTO %s (pk, ck) VALUES ('tupleUpdates', 3)",
                        "UPDATE %s SET v = (null, null) WHERE pk = 'tupleUpdates' AND ck = 4",
                        "UPDATE %s SET v = (null, 'asdf') WHERE pk = 'tupleUpdates' AND ck = 5",
                        "UPDATE %s SET v = (123, null) WHERE pk = 'tupleUpdates' AND ck = 6",
                        "UPDATE %s SET v = (null, '') WHERE pk = 'tupleUpdates' AND ck = 7",
                  }
            );

            performTest(data, mode);
      }

      @ParameterizedTest
      @EnumSource(value = Mode.class)
      public void testTuplesInTuples(Mode mode) {
            TestData data = new TestData(
                  "tuplesInTuples", 
                  schemaTuplesInTuples,
                  new String[] {
                        "INSERT INTO %s (pk, ck, v) VALUES ('tuplesInTuples', 1, ((1, 'abc'), 7))",
                        "INSERT INTO %s (pk, ck, v) VALUES ('tuplesInTuples', 2, ((3, 'def'), 9))",
                        "INSERT INTO %s (pk, ck, v) VALUES ('tuplesInTuples', 3, ((3, 'ghi'), 9))",
                        "INSERT INTO %s (pk, ck, v) VALUES ('tuplesInTuples', 4, ((3, 'jkl'), 9))",
                        "UPDATE %s SET v = ((100, 'zyx'), 111) WHERE pk = 'tuplesInTuples' AND ck = 1",
                        "UPDATE %s SET v = null WHERE pk = 'tuplesInTuples' AND ck = 2",
                        "UPDATE %s SET v = ((200, null), 999) WHERE pk = 'tuplesInTuples' AND ck = 3",
                        "UPDATE %s SET v = ((300, ''), 333) WHERE pk = 'tuplesInTuples' AND ck = 4",
                  }
            );

            performTest(data, mode);
      }

      @ParameterizedTest
      @EnumSource(value = Mode.class)
      public void testTuplesInTuplesInTuples(Mode mode) {
            TestData data = new TestData(
                  "tuplesInTuplesInTuples", 
                  schemaTuplesInTuplesInTuples,
                  new String[] {
                        "INSERT INTO %s (pk, ck, v) VALUES ('tuplesInTuplesInTuples', 1, (((1, 9), 'abc'), 7))",
                        "INSERT INTO %s (pk, ck, v) VALUES ('tuplesInTuplesInTuples', 2, (((3, 8), 'def'), 9))",
                        "UPDATE %s SET v = (((100, 200), 'zyx'), 111) WHERE pk = 'tuplesInTuplesInTuples' AND ck = 1",
                        "UPDATE %s SET v = null WHERE pk = 'tuplesInTuplesInTuples' AND ck = 2",
                        "UPDATE %s SET v = (null, 123) WHERE pk = 'tuplesInTuplesInTuples' AND ck = 3",
                        "UPDATE %s SET v = ((null, 'xyz'), 321) WHERE pk = 'tuplesInTuplesInTuples' AND ck = 4",
                  }
            );

            performTest(data, mode);
      }

      @ParameterizedTest
      @EnumSource(value = Mode.class)
      public void testUdt(Mode mode) {
            TestData data = new TestData(
                  "udt", 
                  schemaUDTs,
                  new String[] {
                        "INSERT INTO %s (pk, ck, v) VALUES ('udt', 1, (2, 3, 'abc'))",
                        "INSERT INTO %s (pk, ck, v) VALUES ('udt', 2, {a: 6, c: 'zxcv'})",
                        "INSERT INTO %s (pk, ck, v) VALUES ('udt', 3, (9, 4, 'def'))",
                        "INSERT INTO %s (pk, ck, v) VALUES ('udt', 4, (123, 321, 'ghi'))",
                        "INSERT INTO %s (pk, ck, v) VALUES ('udt', 5, (333, 222, 'jkl'))",
                        "INSERT INTO %s (pk, ck, v) VALUES ('udt', 6, (432, 678, 'mno'))",
                        "INSERT INTO %s (pk, ck, v) VALUES ('udt', 7, (765, 345, 'pqr'))",
                        "UPDATE %s SET v.b = 41414 WHERE pk = 'udt' AND ck = 2",
                        "UPDATE %s SET v = null WHERE pk = 'udt' AND ck = 3",
                        "UPDATE %s SET v = {b: 123456, c: 'tyu'} WHERE pk = 'udt' AND ck = 4",
                        "INSERT INTO %s (pk, ck, v) VALUES ('udt', 5, (999, 888, 'zxc'))",
                        "UPDATE %s SET v.c = null WHERE pk = 'udt' AND ck = 6",
                        "UPDATE %s SET v = {a: 923, b: 123456, c: ''} WHERE pk = 'udt' AND ck = 7",
                  }
            );

            performTest(data, mode);
      }
}
