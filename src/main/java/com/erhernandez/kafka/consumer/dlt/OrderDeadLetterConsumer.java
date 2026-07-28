package com.erhernandez.kafka.consumer.dlt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import com.erhernandez.kafka.avro.OrderCreated;

@Component
public class OrderDeadLetterConsumer {

    private static final Logger log =
            LoggerFactory.getLogger(OrderDeadLetterConsumer.class);

    @KafkaListener(
            topics = "orders-dlt",
            groupId = "order-dlt-group",
            containerFactory = "orderDeadLetterKafkaListenerFactory"
    )
    public void consume(
            OrderCreated order,
            @Header("eventVersion") String version,
            @Header("eventType") String eventType,
            @Header("correlationId") String correlationId) {

        log.info("====================================");
        log.info("ORDER DLT MESSAGE");
        log.info("====================================");
        log.info("Version      : {}", version);
        log.info("Event Type   : {}", eventType);
        log.info("Correlation  : {}", correlationId);
        log.info("Order Id     : {}", order.getOrderId());
        log.info("Customer     : {}", order.getCustomerName());
        log.info("Priority     : {}", order.getPriority());
        log.info("====================================");

    }

}