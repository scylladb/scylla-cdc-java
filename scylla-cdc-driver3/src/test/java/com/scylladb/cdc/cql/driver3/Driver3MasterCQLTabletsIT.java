package com.scylladb.cdc.cql.driver3;

import com.scylladb.cdc.cql.MasterCQL;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.master.GenerationMetadata;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
public class Driver3MasterCQLTabletsIT extends BaseScyllaTabletsIntegrationTest {

    @Test
    public void testTabletsMasterFetchesGenerationIdForTable() throws InterruptedException, ExecutionException, TimeoutException {
        tryCreateTable("CREATE TABLE ks.test(p int, c int, v int, PRIMARY KEY(p, c)) " +
                "WITH cdc = {'enabled': true}");

        // Check that Driver3MasterCQL can fetch the table's generation id in tablet mode
        MasterCQL masterCQL = new Driver3MasterCQL(buildLibrarySession());
        TableName tableName = new TableName("ks", "test");

        // In tablet mode, we fetch per-table generation IDs
        GenerationId tableGeneration = masterCQL.fetchFirstTableGenerationId(tableName)
                .get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        // Verify we got a valid generation ID
        assertNotNull(tableGeneration, "Table generation ID should not be null");
        Timestamp generationStart = tableGeneration.getGenerationStart();

        // Verify this generation was created recently
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.HOUR, -1);
        assertTrue(generationStart.toDate().after(calendar.getTime()),
                "Generation timestamp should be recent (less than 1 hour old)");

        // Fetch and verify generation metadata
        GenerationMetadata metadata = masterCQL.fetchTableGenerationMetadata(tableName, tableGeneration)
                .get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        assertNotNull(metadata, "Generation metadata should not be null");
        assertTrue(metadata.getStart().equals(generationStart),
                "Generation metadata start timestamp should match generation ID timestamp");
        assertFalse(metadata.getStreams().isEmpty(),
                "Generation should have non-empty stream set");
    }
}
