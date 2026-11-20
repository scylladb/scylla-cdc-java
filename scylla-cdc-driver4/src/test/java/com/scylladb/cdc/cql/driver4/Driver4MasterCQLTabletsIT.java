package com.scylladb.cdc.cql.driver4;

import com.datastax.oss.driver.api.core.cql.Row;
import com.scylladb.cdc.cql.MasterCQL;
import com.scylladb.cdc.cql.driver4.Driver4MasterCQLIT.RecordingDriver4Session;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.master.GenerationMetadata;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.scylladb.cdc.cql.driver4.Driver4MasterCQLIT.SCYLLA_TIMEOUT_MS;
import static com.scylladb.cdc.cql.driver4.Driver4MasterCQLIT.assertRecent;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
@Tag("tablets")
public class Driver4MasterCQLTabletsIT extends BaseScyllaTabletsIntegrationTest {

    @Test
    public void testTabletsMasterFetchesGenerationIdForTable() throws InterruptedException, ExecutionException, TimeoutException {
        driverSession.execute("CREATE TABLE ks.test(p int, c int, v int, PRIMARY KEY(p, c)) " +
                "WITH cdc = {'enabled': true}");

        try (RecordingDriver4Session session = RecordingDriver4Session.connect()) {
            MasterCQL masterCQL = new Driver4MasterCQL(session);
            TableName tableName = new TableName("ks", "test");
            assertTrue(masterCQL.usesTablets(tableName));

            GenerationId tableGeneration = masterCQL.fetchFirstTableGenerationId(tableName)
                    .get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            assertRecent(tableGeneration.getGenerationStart());

            GenerationMetadata metadata = masterCQL.fetchTableGenerationMetadata(tableName, tableGeneration)
                    .get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            assertEquals(tableGeneration.getGenerationStart(), metadata.getStart());
            assertEquals(currentStreams(tableGeneration.getGenerationStart().toDate().toInstant()), metadata.getStreams());
            // The table was just created, so its first generation is still open.
            assertFalse(metadata.getEnd().isPresent(), "the only generation is open");
            session.assertAllIdempotent();
        }
    }

    /**
     * Reads the streams of {@code ks.test} that are current at a timestamp, filtering in the test.
     */
    private Set<StreamId> currentStreams(Instant timestamp) {
        Set<StreamId> streams = new HashSet<>();
        for (Row row : driverSession.execute("SELECT timestamp, stream_state, stream_id FROM system.cdc_streams "
                + "WHERE keyspace_name = 'ks' AND table_name = 'test'")) {
            if (timestamp.equals(row.getInstant("timestamp")) && row.getByte("stream_state") == 0) {
                streams.add(new StreamId(row.getByteBuffer("stream_id")));
            }
        }
        // The keyspace starts with two tablets, so a read of only the first row is caught.
        assertTrue(streams.size() > 1, "the generation has several streams: " + streams.size());
        return streams;
    }
}
