package com.scylladb.cdc.connector.utils;

public class ScyllaConstants {
    private ScyllaConstants() {
        throw new IllegalStateException("This is Utility Class Can't be initiated");
    }

    public static final String ORG = "dpt_org";
    public static final String TENANT = "dpt_tenant";
    public static final String NAMESPACE = "dpt_namespace";
    public static final String CREATED_AT = "dpt_createdAt";
    public static final String UPDATED_AT = "dpt_updatedAt";
    public static final String INGESTED_AT = "dpt_ingestedAt";
    public static final String OBJECT_NAME = "dpt_objectName";
    public static final String OPCODE = "dpt_opCode";
    public static final String INSERT_OPCODE = "I";
    public static final String UPDATE_OPCODE = "U";
    public static final String DELETE_OPCODE = "D";
    public static final String ENTITY_ID = "dpt_entityId";
    public static final String SCHEMA_VERSION = "dpt_schemaVersion";
    public static final String COMPRESSION_TYPE_SNAPPY = "snappy";
    public static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static final String SLACK_CHANNEL = "dataplatform";
    public static final String SLACK_USER = "FIBER-NRT";
    public static final String SLACK_API_TYPE = "application/json";
    public static final String SLACK_EMOJI = ":twice:";

    public static final String POST_IMAGE = "POST_IMAGE";
    public static final String ROW_UPDATE = "ROW_UPDATE";
    public static final String ROW_DELETE = "ROW_DELETE";
    public static final String ROW_INSERT = "ROW_INSERT";
    public static final String UPDATED_CACHE = "update";

    public static final String CHECK_INSTANCE_QUERY = "Select id,last_read_cdc_timestamp from scylla_kafka_checkpointing.scylla_checkpoint where instance_name= ?";

}
