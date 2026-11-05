package com.scylladb.cdc.replicator.operations;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.google.common.util.concurrent.SettableFuture;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class FutureUtilsTest {

    @Test
    public void convertCompletesWhenTheWriteSucceeds() throws Exception {
        SettableFuture<Object> write = SettableFuture.create();
        CompletableFuture<Void> converted = FutureUtils.convert(write, "write failed");

        write.set(new Object());

        assertNull(converted.get(5, TimeUnit.SECONDS));
    }

    @Test
    public void convertFailsWhenTheWriteFails() {
        SettableFuture<Object> write = SettableFuture.create();
        CompletableFuture<Void> converted = FutureUtils.convert(write, "write failed");
        RuntimeException failure = new RuntimeException("write timeout");

        write.setException(failure);

        ExecutionException thrown = assertThrows(ExecutionException.class, () -> converted.get(5, TimeUnit.SECONDS));
        assertSame(failure, thrown.getCause());
    }
}
