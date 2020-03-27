package com.yashagarwal.messaging.interfaces;

import org.apache.kafka.clients.consumer.ConsumerRecords;

@FunctionalInterface
public interface MessageProcessor {

    void process(ConsumerRecords<String, String> pollRecords);
}
