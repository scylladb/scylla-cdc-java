package com.scylladb.cdc.transport;

/** Stable namespaces used by library coordination protocols. */
public final class CoordinationNamespaces {
    public static final String TABLET_TASK_STATE_MIGRATION =
            "scylla-cdc/tablet-task-state-migration/v1";

    private CoordinationNamespaces() {
    }
}
