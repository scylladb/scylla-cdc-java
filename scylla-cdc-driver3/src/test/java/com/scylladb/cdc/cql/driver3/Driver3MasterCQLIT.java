package com.scylladb.cdc.cql.driver3;

import com.scylladb.cdc.cql.MasterCQL;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.Timestamp;
import org.junit.jupiter.api.Test;

import java.util.Calendar;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class Driver3MasterCQLIT extends BaseScyllaIntegrationTest {
    private static final long SCYLLA_TIMEOUT_MS = 3000;

    @Test
    public void testMasterFetchesFirstGenerationId() throws InterruptedException, ExecutionException, TimeoutException {
        // Check that Driver3MasterCQL can fetch the first generation id.

        MasterCQL masterCQL = new Driver3MasterCQL(librarySession);
        Optional<GenerationId> firstGeneration =
                masterCQL.fetchFirstGenerationId().get(SCYLLA_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        assertTrue(firstGeneration.isPresent());
        Timestamp generationStart = firstGeneration.get().getGenerationStart();

        // Sanity check: the read generationStart should
        // be relatively recent - within the last hour.
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.HOUR, -1);
        assertTrue(generationStart.toDate().after(calendar.getTime()));
    }
}
