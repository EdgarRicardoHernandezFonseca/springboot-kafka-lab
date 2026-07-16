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
public class OrderConsumer {
	
	private static final Logger log =
	        LoggerFactory.getLogger(OrderConsumer.class);

    @KafkaListener(
    		topics = "orders", 
    		groupId = "order-processing"
            )
    public void consume(
    		Order order,
    		@Header("eventType") String eventType,
            @Header("eventVersion") String eventVersion,
            @Header("source") String source,
            @Header("correlationId") String correlationId,
    		Acknowledgment ack,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {
    	    	
    	if(order.getOrderId() % 2 == 0){
		    throw new RuntimeException("Retry Test");
		}
    	
    	if(order.getCustomerName().equalsIgnoreCase("ERROR")){

		    throw new RuntimeException(
		            "Temporary processing error"
		    );

		}
		
		if(order.getCustomerName().isBlank()){

		    throw new IllegalArgumentException(
		            "Customer name is mandatory"
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

		log.info("ORDER CONSUMER");
		log.info("Partition : {}", partition);
		log.info("Offset    : {}", offset);
		log.info("Order ID  : {}", order.getOrderId());

		log.info("--------------------------------");

		log.info("Processing order...");
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