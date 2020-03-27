package com.yashagarwal.helpers;

import com.yashagarwal.messaging.MessageProcessorWithRetry;
import com.yashagarwal.messaging.MessageProducer;
import com.yashagarwal.messaging.constants.Constant;
import com.yashagarwal.messaging.dtos.MessageDTO;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.Date;

public class EmailHelperWithRetry {

    private static final Logger LOGGER =  LoggerFactory.getLogger(EmailHelperWithRetry.class);

    /* Singleton which is injected here */
    private ObjectMapper objectMapper;

    /* Singleton which is injected here */
    private MessageProducer messageProducer;

    /*
    * The code to send out email has been updated to send email to Kafka instead,
    * Basically the following function sendEmailToKafka corresponds to sendEmailSync in EmailHelperOriginal
    */
    private void sendEmailToKafka(Email email) throws IOException {

        String topicInstant = Constant.TOPIC_INSTANT;

        MessageDTO<Email> message = new MessageDTO<>();
        message.setType(MessageDTO.MessageType.EMAIL);
        message.setPayload(email);
        message.setDateCreated(new Date());

        String emailStr = objectMapper.writeValueAsString(message);
        messageProducer.send(emailStr, topicInstant);
    }

    /*
     * For every MessageType that we add use Kafka retry queues, we have to only once define
     * how that record should be processed when retrieved from kafka, good part is logic parts
     * i.e. sending to kafka, and processing when retrieved from Kafka can be defined by developer
     * in the class file which corresponds to business logic for that MessageType
     * So in this helper file for Emails, we define both sets of logic
     * The following code has to be initialised once with application
     */
    @PostConstruct
    void initialize() {
        MessageProcessorWithRetry.registerRecordProcessor(MessageDTO.MessageType.EMAIL, (record) -> {
            try {
                MessageDTO<Email> yumaMessageDTO = objectMapper.readValue(record.value(), new TypeReference<MessageDTO<Email>>() {
                });

                Email email = yumaMessageDTO.getPayload();
                email.send();

                return true;

            } catch (IOException e) {
                LOGGER.error("Error in parsing message with value: " + record.value(), e);
            } catch (Exception e) {
                LOGGER.error("Error in sending email with message value: " + record.value(), e);
            }

            return false;
        });
    }
}
