package com.yashagarwal.messaging.interfaces;

import org.apache.kafka.clients.consumer.ConsumerRecord;

@FunctionalInterface
public interface RecordProcessor {

    boolean process(ConsumerRecord<String, String> record);
}
