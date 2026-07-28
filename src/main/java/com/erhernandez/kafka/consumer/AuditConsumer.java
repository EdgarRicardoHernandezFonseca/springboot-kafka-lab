package com.erhernandez.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;
import com.erhernandez.kafka.avro.OrderCreated;
import com.erhernandez.kafka.validator.OrderValidator;

@Service
public class AuditConsumer {
	
	private static final Logger log =
	        LoggerFactory.getLogger(AuditConsumer.class);
	
	private final OrderValidator orderValidator;
	
	public AuditConsumer(OrderValidator orderValidator) {
		this.orderValidator = orderValidator;
	}

	@KafkaListener(
	        topics = "orders",
	        groupId = "audit-service",
	        containerFactory = "orderKafkaListenerFactory"
	)
	public void consume(
			OrderCreated order,
	        @Header("eventType") String eventType,
            @Header("eventVersion") String eventVersion,
            @Header("source") String source,
            @Header("correlationId") String correlationId,
	        Acknowledgment ack,
	        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
	        @Header(KafkaHeaders.OFFSET) long offset
	        ) {
		
		orderValidator.validate(order);

		log.info("--------------------------------");
		log.info("Message Headers");
		log.info("--------------------------------");

		log.info("Event Type    : {}", eventType);
		log.info("Version       : {}", eventVersion);
		log.info("Source        : {}", source);
		log.info("CorrelationId : {}", correlationId);

		log.info("--------------------------------");

		log.info("AUDIT CONSUMER");
		log.info("Partition : {}", partition);
		log.info("Offset    : {}", offset);
		log.info("Order ID  : {}", order.getOrderId());

		log.info("--------------------------------");

		log.info("Processing audit...");
		log.info("Order ID : {}", order.getOrderId());
		// Simulation of business processing
    	try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

    	log.info("Business completed.");
    	
    	ack.acknowledge();

        log.info("Offset committed manually.");
        
	}
}
