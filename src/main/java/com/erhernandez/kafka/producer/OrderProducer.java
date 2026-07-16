package com.erhernandez.kafka.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.erhernandez.kafka.dto.Order;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.util.UUID;

@Service
public class OrderProducer {

    private final KafkaTemplate<String, Order> kafkaTemplate;
    
    private static final Logger log =
	        LoggerFactory.getLogger(OrderProducer.class);

    public OrderProducer(KafkaTemplate<String, Order> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(Order order) {

        String key = order.getOrderId().toString();
        String correlationId = UUID.randomUUID().toString();

        ProducerRecord<String, Order> record =
                new ProducerRecord<>(
                        "orders",
                        String.valueOf(order.getOrderId()),
                        order
                );
        
        record.headers().add(
                new RecordHeader(
                        "eventType",
                        "ORDER_CREATED".getBytes(StandardCharsets.UTF_8)
                )
        );

        record.headers().add(
                new RecordHeader(
                        "source",
                        "springboot-kafka-lab".getBytes(StandardCharsets.UTF_8)
                )
        );

        record.headers().add(
                new RecordHeader(
                        "eventVersion",
                        "v1".getBytes(StandardCharsets.UTF_8)
                )
        );
        
        record.headers().add(
                "correlationId",
                correlationId.getBytes(StandardCharsets.UTF_8));
        
        kafkaTemplate.send(record);
   
        log.info("***********************");
	    log.info("Order sent");
	    log.info("Key       : {}", key);
	    log.info("Customer  : {}", order.getCustomerName());
	    log.info("Correlation ID : {}", correlationId);
	    log.info("***********************");
    }

}
