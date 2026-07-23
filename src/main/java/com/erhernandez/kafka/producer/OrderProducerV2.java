package com.erhernandez.kafka.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.erhernandez.kafka.avro.OrderCreated;
import com.erhernandez.kafka.commons.LogConstants;
import com.erhernandez.kafka.dto.OrderV2;
import com.erhernandez.kafka.event.EventType;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.util.UUID;

@Service
public class OrderProducerV2 {
	
private final KafkaTemplate<String, OrderCreated> kafkaTemplate;
    
	private static final LogConstants LogConstants = new LogConstants();
	
	private static final Logger log =
	        LoggerFactory.getLogger(OrderProducerV2.class);

    public OrderProducerV2(KafkaTemplate<String, OrderCreated> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(
    		OrderV2 orderV2,
    		EventType eventType) {
    	
    	OrderCreated order =
                OrderCreated.newBuilder()
                        .setOrderId(orderV2.getOrderId())
                        .setCustomerName(orderV2.getCustomerName())
                        .setProduct(orderV2.getProduct())
                        .setQuantity(orderV2.getQuantity())
                        .build();

	
        String key = String.valueOf(order.getOrderId());
        String correlationId = UUID.randomUUID().toString();
        
        

        ProducerRecord<String, OrderCreated> record =
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
                        eventType.name().getBytes(StandardCharsets.UTF_8)
                )
        );
        
        log.info(LogConstants.LINE);
        log.info("ORDER EVENT PUBLISHING STARTED");
        log.info(LogConstants.LINE);
        log.info("");
        log.info("Receiving order request...");
        log.info("");
        log.info("Validating request payload...");    
        log.info("");
        log.info("Generating Correlation ID...");    
        log.info("");
        log.info("Building Kafka Headers...");        
        log.info("");
        log.info("Building Kafka Message...");
        log.info("");
        log.info(LogConstants.SECTION);
        log.info("Message Metadata");
        log.info("Correlation ID : {}", correlationId);
        log.info("Event Type     : {}", eventType);
        log.info("Event Version  : v2");
        log.info("Source         : springboot-kafka-lab");
        log.info("Topic			 : orders");
        log.info("Order ID       : {}", order.getOrderId());
        Instant timestamp = Instant.now();
        log.info("Timestamp		 : {}", timestamp);
        log.info(LogConstants.SECTION);
        log.info("");
        log.info("Publishing to Topic orders...");        

        kafkaTemplate.send(record);
        
        log.info("");
        log.info("Message published successfully.");        
        log.info("");
        log.info(LogConstants.LINE);
        log.info("ORDER PUBLISHING FINISHED");
        log.info(LogConstants.LINE);       
    }
}
