package com.erhernandez.kafka.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.erhernandez.kafka.dto.OrderV2;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.util.UUID;

@Service
public class OrderProducerV2 {
	
private final KafkaTemplate<String, OrderV2> kafkaTemplate;
    
    private static final Logger log =
	        LoggerFactory.getLogger(OrderProducerV2.class);

    public OrderProducerV2(KafkaTemplate<String, OrderV2> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(OrderV2 order) {

        String key = order.getOrderId().toString();
        String correlationId = UUID.randomUUID().toString();

        ProducerRecord<String, OrderV2> record =
                new ProducerRecord<>(
                        "orders",
                        String.valueOf(order.getOrderId()),
                        order
                );
        
        record.headers().add(
        	    new RecordHeader(
        	        "eventVersion",
        	        "v2".getBytes(StandardCharsets.UTF_8)
        	    )
        	);

        record.headers().add(
                new RecordHeader(
                        "source",
                        "springboot-kafka-lab".getBytes(StandardCharsets.UTF_8)
                )
        );
 
        record.headers().add(
                "correlationId",
                correlationId.getBytes(StandardCharsets.UTF_8));
        
        record.headers().add(
                new RecordHeader(
                        "eventType",
                        "ORDER_CREATED".getBytes(StandardCharsets.UTF_8)
                )
        );
        
        kafkaTemplate.send(record);
   
        log.info("***********************");
        log.info("Order V2 sent");
        log.info("Version   : v2");
        log.info("Key       : {}", key);
        log.info("Customer  : {}", order.getCustomerName());
        log.info("priority  : {}", order.getPriority());
        log.info("Correlation ID : {}", correlationId);
        log.info("***********************");
    }
}
