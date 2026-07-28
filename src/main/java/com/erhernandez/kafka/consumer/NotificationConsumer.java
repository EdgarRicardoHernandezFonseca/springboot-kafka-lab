package com.erhernandez.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;
import com.erhernandez.kafka.avro.Notification;
import com.erhernandez.kafka.avro.OrderCreated;
import com.erhernandez.kafka.producer.NotificationProducer;

@Service
public class NotificationConsumer {
	
	private static final Logger log =
	        LoggerFactory.getLogger(NotificationConsumer.class);
	
	private final NotificationProducer notificationProducer;

	public NotificationConsumer(NotificationProducer notificationProducer) {
	    this.notificationProducer = notificationProducer;
	}

	@KafkaListener(
	        topics = "orders",
	        groupId = "notification-service",
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
            @Header(KafkaHeaders.OFFSET) long offset) {
    	
    	
		if(order.getOrderId() % 2 == 0){
		    throw new RuntimeException("Retry Test");
		}
    	
    	if ("ERROR".equalsIgnoreCase(order.getCustomerName().toString())) {

		    throw new RuntimeException(
		            "Temporary processing error"
		    );

		}
    	
    	String customer =
    	        order.getCustomerName() == null
    	        ? null
    	        : order.getCustomerName().toString();
		
    	if (customer == null || customer.isBlank()) {

		    throw new IllegalArgumentException(
		            "Customer name is mandatory"
		    );

		}
    	
		Notification notification =
    	        new Notification(
    	        		order.getOrderId(),
    	                "Order processed successfully");

    	notificationProducer.send(notification, correlationId);
		
		log.info("--------------------------------");
		log.info("Message Headers");
		log.info("--------------------------------");

		log.info("Event Type    : {}", eventType);
		log.info("Version       : {}", eventVersion);
		log.info("Source        : {}", source);
		log.info("CorrelationId : {}", correlationId);

		log.info("--------------------------------");

		log.info("NOTIFICATION CONSUMER");
		log.info("Partition : {}", partition);
		log.info("Offset    : {}", offset);
		log.info("Order ID  : {}", order.getOrderId());

		log.info("--------------------------------");

		log.info("Processing notification...");
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
