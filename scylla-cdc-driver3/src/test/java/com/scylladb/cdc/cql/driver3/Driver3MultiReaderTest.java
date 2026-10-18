package com.scylladb.cdc.cql.driver3;

import com.scylladb.cdc.model.worker.RawChange;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

class Driver3MultiReaderTest {
    @Test
    void noReadersCompletesEmpty() throws Exception {
        Driver3WorkerCQL cql = new Driver3WorkerCQL(mock(Driver3Session.class));

        CompletableFuture<Optional<RawChange>> next =
                cql.new Driver3MultiReader(Collections.emptyList(), Optional.empty()).nextChange();

        assertEquals(Optional.empty(), next.get(1, TimeUnit.SECONDS));
    }
}
