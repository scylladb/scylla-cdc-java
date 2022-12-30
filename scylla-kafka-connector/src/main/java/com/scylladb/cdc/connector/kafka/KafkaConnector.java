package com.scylladb.cdc.connector.kafka;

import com.scylladb.cdc.connector.utils.ScyllaConstants;
import com.scylladb.cdc.model.worker.ScyllaConnectorConfiguration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class KafkaConnector implements IConnector{

    private final String brokers;
    private final int poolSize;
    private final Random random;
    private final AtomicReference<List<KafkaProducer<String, String>>> kafkaProducerList;

    public KafkaConnector(ScyllaConnectorConfiguration scyllaConnectorConfiguration) {
        this.random = new Random();
        this.brokers = scyllaConnectorConfiguration.getKafkaBrokers();
        this.poolSize = scyllaConnectorConfiguration.getPoolSize();
        this.kafkaProducerList = new AtomicReference<>(Collections.synchronizedList(new LinkedList<>()));
        init();
    }

    @Override
    public void init() {
        for (int i = 0; i < this.poolSize; ++i) {
            Properties kafkaProperties = new Properties();
            kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokers);
            kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            kafkaProperties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, ScyllaConstants.COMPRESSION_TYPE_SNAPPY);
            this.kafkaProducerList.get().add(new KafkaProducer<>(kafkaProperties));
        }
    }

    @Override
    public KafkaProducer<String, String> getConnector() {
        if(this.kafkaProducerList.get().isEmpty())
            init();
        return this.kafkaProducerList.get().get(this.random.nextInt(this.kafkaProducerList.get().size() == 1 ? this.poolSize : this.poolSize - 1));
    }
}
