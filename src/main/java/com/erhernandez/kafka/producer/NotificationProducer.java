package com.erhernandez.kafka.producer;

import com.erhernandez.kafka.dto.Notification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

@Service
public class NotificationProducer {

    private static final Logger log =
            LoggerFactory.getLogger(NotificationProducer.class);

    private final KafkaTemplate<String, Notification> kafkaTemplate;

    public NotificationProducer(KafkaTemplate<String, Notification> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(Notification notification, String correlationId) {

    	Message<Notification> message =
    	        MessageBuilder
    	            .withPayload(notification)
    	            .setHeader(KafkaHeaders.TOPIC, "notifications")
    	            .setHeader(KafkaHeaders.KEY, notification.getOrderId().toString())
    	            .setHeader("eventType", "ORDER_CREATED")
    	            .setHeader("eventVersion", "v1")
    	            .setHeader("source", "springboot-kafka-lab")
    	            .setHeader("correlationId", correlationId)
    	            .build();

        kafkaTemplate.send(message);

        log.info("Notification published");
        log.info("Correlation ID : {}", correlationId);
    }
}