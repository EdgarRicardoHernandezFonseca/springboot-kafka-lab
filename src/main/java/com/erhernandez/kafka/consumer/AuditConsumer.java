package com.erhernandez.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;

import com.erhernandez.kafka.dto.Order;

@Service
public class AuditConsumer {
	
	private static final Logger log =
	        LoggerFactory.getLogger(AuditConsumer.class);

	@KafkaListener(
	        topics = "orders",
	        groupId = "order-group")
	public void consume(
	        Order order,
	        Acknowledgment ack,
	        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
	        @Header(KafkaHeaders.OFFSET) long offset) {

	    log.info("***********************");
	    log.info("AUDIT CONSUMER");
	    log.info("Partition : {}", partition);
	    log.info("Offset    : {}", offset);
	    log.info("Order ID  : {}", order.getOrderId());
	    log.info("***********************");
	    
	    log.info("--------------------------------");
    	log.info("Processing audit...");
    	log.info("Order ID : {}", order.getOrderId());

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
