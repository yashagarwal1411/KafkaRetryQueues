/*
 * MessageConsumer.java
 * This class is a generic Kafka Message Consumer
 */

package com.yashagarwal.messaging;

import com.yashagarwal.messaging.interfaces.MessageProcessor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class MessageConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageConsumer.class);

    public MessageConsumer(MessageProcessor processRecords, String groupId, String brokers, String... topics) {
        Properties properties = new Properties();

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        /*
         * The following property is required so that our 5 min or 1 hr consumer is not removed from
         * consumer group and Kafka doesn't assume consumer is dead and re balances them
         */
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, (int) Duration.ofHours(2).toMillis());

        Consumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topics));

        CountDownLatch latch = new CountDownLatch(1);

        Thread thread = new Thread(() -> {
            try {
                while (true) {
                    ConsumerRecords<String, String> pollRecords = consumer.poll(Duration.ofSeconds(60L));

                    processRecords.process(pollRecords);
                    consumer.commitSync();
                }
            } catch (WakeupException ex) {
                LOGGER.error("Wakeup exception received for Consumer for topic: " + topics);
                //Do nothing, application is closing down
            } finally {
                LOGGER.info("Consumer for topic: " + topics + " is closing on current server");
                consumer.close();
                latch.countDown();
            }
        });

        thread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Received shutdown hook, will close Kafka consumer now");
            consumer.wakeup();
            try {
                latch.await();
            } catch (InterruptedException ex) {
                LOGGER.error("Kafka Consumer for topics: " + topics + " received interrupt signal", ex);
            } finally {
                LOGGER.info("Kafka Consumer for topics: " + topics + " is shutdown");
            }
        }));
    }
}