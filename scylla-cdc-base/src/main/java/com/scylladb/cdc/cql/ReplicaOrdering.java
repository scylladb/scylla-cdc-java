package com.scylladb.cdc.cql;

/**
 A copy-cat TokenAwarePolicy.ReplicaOrdering of the driver
 Needed here not to leak driver types
 */
public enum ReplicaOrdering {
	TOPOLOGICAL,
	RANDOM,
	NEUTRAL;
}

