create database scylla_kafka_checkpointing;
CREATE TABLE `scylla_checkpoint` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `instance_name` varchar(128) DEFAULT NULL,
  `keyspace_table_combination` varchar(128) DEFAULT NULL,
  `last_read_cdc_timestamp` bigint DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
)