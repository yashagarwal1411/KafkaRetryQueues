package com.yashagarwal.messaging;

import com.yashagarwal.messaging.constants.Constant;
import com.yashagarwal.messaging.dtos.MessageDTO;
import com.yashagarwal.messaging.interfaces.RecordProcessor;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class MessageProcessorWithRetry {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageProcessorWithRetry.class);

    /* Singleton which is injected here */
    private MessageProducer messageProducer;

    /* Singleton which is injected here */
    ObjectMapper objectMapper;

    private static Map<MessageDTO.MessageType, RecordProcessor> map = new HashMap<>();

    public static void registerRecordProcessor(MessageDTO.MessageType type, RecordProcessor recordProcessor) {
        map.put(type, recordProcessor);
    }

    @PostConstruct
    public void initialize() {

        String topicInstant = Constant.TOPIC_INSTANT;
        String topic5min = Constant.TOPIC_5MIN;
        String topic1hr = Constant.TOPIC_1HR;

        String groupId = Constant.GROUP_ID;
        String brokers = Constant.BROKERS;

        objectMapper = new ObjectMapper();

        new MessageConsumer(pollRecords -> {

            pollRecords.forEach(record -> {

                LOGGER.info("Consumer Record:(%s, %s, %s ,%d, %d)\n",
                        record.topic(), record.key(), record.value(),
                        record.partition(), record.offset());

                boolean isProcessed = false;

                String value = record.value();

                try {
                    MessageDTO message = objectMapper.readValue(value, MessageDTO.class);

                    isProcessed = map.get(message.getType()).process(record);

                } catch (IOException ex) {
                    LOGGER.error("Error when parsing Kafka message: " + value, ex);
                } catch (Exception ex) {
                    LOGGER.error("Unknown exception when processing Kafka message: " + value, ex);
                } finally {
                    if (!isProcessed) {
                        messageProducer.send(record.value(), topic5min);
                    }
                }

            });

        }, groupId, brokers, new String[]{topicInstant});

        new MessageConsumer(pollRecords -> {

            pollRecords.forEach(record -> {

                LOGGER.info("Consumer Record:(%s, %s, %s ,%d, %d)\n",
                        record.topic(), record.key(), record.value(),
                        record.partition(), record.offset());

                boolean isProcessed = false;

                String value = record.value();

                try {
                    MessageDTO message = objectMapper.readValue(value, MessageDTO.class);

                    long timeDiff = new Date().getTime() - message.getDateCreated().getTime();
                    long timeDiffRequired = 5 * 60 * 1000;
                    if (timeDiff < timeDiffRequired) {
                        Thread.sleep(timeDiffRequired - timeDiff);
                    }

                    isProcessed = map.get(message.getType()).process(record);

                } catch (IOException ex) {
                    LOGGER.error("Error when parsing Kafka message: " + value, ex);
                } catch (Exception ex) {
                    LOGGER.error("Unknown exception when processing Kafka message: " + value, ex);
                } finally {
                    if (!isProcessed) {
                        messageProducer.send(record.value(), topic1hr);
                    }
                }

            });

        }, groupId, brokers, new String[]{topic5min});


        new MessageConsumer(pollRecords -> {

            pollRecords.forEach(record -> {

                LOGGER.info("Consumer Record:(%s, %s, %s ,%d, %d)\n",
                        record.topic(), record.key(), record.value(),
                        record.partition(), record.offset());


                boolean isProcessed = false;

                String value = record.value();

                MessageDTO message;

                try {
                    message = objectMapper.readValue(value, MessageDTO.class);
                } catch (IOException ex) {
                    LOGGER.error("Error when parsing Kafka message: " + value, ex);
                    return;
                }

                try {
                    long timeDiff = new Date().getTime() - message.getDateCreated().getTime();
                    long timeDiffRequired = 60 * 60 * 1000;
                    if (timeDiff < timeDiffRequired) {
                        Thread.sleep(timeDiffRequired - timeDiff);
                    }

                    isProcessed = map.get(message.getType()).process(record);


                } catch (Exception ex) {
                    LOGGER.error("Unknown exception when processing Kafka message: " + value, ex);
                } finally {
                    if (!isProcessed) {
                        //Save to DB for further analysis
                    }
                }

            });

        }, groupId, brokers, new String[]{topic1hr});


    }
}
