package com.erhernandez.kafka.consumer.dlt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.erhernandez.kafka.avro.Notification;

@Component
public class NotificationDeadLetterConsumer {

    private static final Logger log =
            LoggerFactory.getLogger(NotificationDeadLetterConsumer.class);

    @KafkaListener(
            topics = "notifications-dlt",
            groupId = "notification-dlt-group",
            containerFactory = "notificationDeadLetterKafkaListenerFactory"
    )
    public void consume(Notification notification) {

        log.info("====================================");
        log.info("NOTIFICATION DLT");
        log.info("====================================");
        log.info("Order Id : {}", notification.getOrderId());
        log.info("Message  : {}", notification.getMessage());
        log.info("====================================");

    }

}