package com.scylladb.cdc.lib;

import com.scylladb.cdc.model.worker.RawChangeConsumer;

@Deprecated
public interface RawChangeConsumerProvider {
    RawChangeConsumer getForThread(int threadId);
}
