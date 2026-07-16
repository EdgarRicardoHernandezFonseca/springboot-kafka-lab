package com.erhernandez.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import com.erhernandez.kafka.dto.Notification;

@Service
public class EmailConsumer {
	
	private static final Logger log =
	        LoggerFactory.getLogger(EmailConsumer.class);
	
    @KafkaListener(
            topics = "notifications",
            groupId = "email-service"
            )
    public void consume(
    		Notification notification,
    		@Header("eventType") String eventType,
            @Header("eventVersion") String eventVersion,
            @Header("source") String source,
            @Header("correlationId") String correlationId,
    		Acknowledgment ack,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {
    	
    	
    	if(notification.getOrderId() % 2 == 0){
		    throw new RuntimeException("Retry Test");
		}
    	
    	if(notification.getMessage().equalsIgnoreCase("ERROR")){

		    throw new RuntimeException(
		            "Temporary processing error"
		    );

		}
		
		if(notification.getMessage().isBlank()){

		    throw new IllegalArgumentException(
		            "Message is mandatory"
		    );

		}
    	
		log.info("--------------------------------");
		log.info("Message Headers");
		log.info("--------------------------------");

		log.info("Event Type    : {}", eventType);
		log.info("Version       : {}", eventVersion);
		log.info("Source        : {}", source);
		log.info("CorrelationId : {}", correlationId);

		log.info("--------------------------------");

		log.info("EMAIL CONSUMER");
		log.info("Partition : {}", partition);
		log.info("Offset    : {}", offset);
		log.info("Order ID  : {}", notification.getOrderId());

		log.info("--------------------------------");

		log.info("Processing email...");
		log.info("Order ID : {}", notification.getOrderId());

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
