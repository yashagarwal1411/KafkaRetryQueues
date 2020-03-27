package com.yashagarwal.messaging;

import com.yashagarwal.messaging.constants.Constant;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.util.Properties;

public class MessageProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageProducer.class);

    Producer<String, String> producer;

    MessageProducer() {
    }

    @PostConstruct
    public void initialize() {

        String brokers = Constant.BROKERS;
        String groupId = Constant.GROUP_ID;

        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, groupId);

        producer = new KafkaProducer<>(properties);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            producer.flush();
            producer.close();
        }));

    }

    public void send(String message, String topic) {
        ProducerRecord<String, String> record = new ProducerRecord(topic, message);
        producer.send(record, (metadata, ex) -> {
            if (metadata != null) {

                LOGGER.info("sent record(key=%s value=%s) " +
                                "meta(partition=%d, offset=%d) \n",
                        record.key(), record.value(), metadata.partition(),
                        metadata.offset());
            } else {
                LOGGER.error("Error in sending message to Kafka", ex);
            }
        });
    }
}
