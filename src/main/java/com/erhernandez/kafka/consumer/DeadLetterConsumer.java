package com.erhernandez.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import com.erhernandez.kafka.dto.Notification;
import com.erhernandez.kafka.dto.Order;
import com.erhernandez.kafka.dto.OrderV2;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class DeadLetterConsumer {
	
	private static final Logger log =
	        LoggerFactory.getLogger(DeadLetterConsumer.class);
	
	private final ObjectMapper objectMapper;

	public DeadLetterConsumer(ObjectMapper objectMapper) {
	    this.objectMapper = objectMapper;
	}

	@KafkaListener(
	        topics = "orders-dlt",
	        groupId = "dlt-group")
	public void consume(
			String payload,
	        @Header("eventVersion") String version
	        ) {
			 	 
	    try {
	    	
	    	log.info(payload);	
	    	
	        if ("v1".equals(version)) {

	            Order order =
	                    objectMapper.readValue(payload, Order.class);

	            log.info("==============================");
	            log.info("MESSAGE ARRIVED TO DLT");
	            log.info("==============================");
	            log.info("OrderId : {}", order.getOrderId());
	            log.info("Customer: {}", order.getCustomerName());
	            log.info("Amount  : {}", order.getTotalAmount());

	        } else if ("v2".equals(version)) {

	            OrderV2 order =
	                    objectMapper.readValue(payload, OrderV2.class);

	            log.info("==============================");
	            log.info("MESSAGE ARRIVED TO DLT");
	            log.info("==============================");
	            log.info("OrderId : {}", order.getOrderId());
	            log.info("Customer: {}", order.getCustomerName());
	            log.info("Priority: {}", order.getPriority());

	        }

	    } catch (Exception ex) {

	        log.error("Error reading DLT message", ex);

	    }
	}
	
	@KafkaListener(
	        topics = "notifications-dlt",
	        groupId = "dlt-group")
	public void consume(Notification notification){

	    log.info("==============================");
	    log.info("MESSAGE ARRIVED TO DLT");
	    log.info("==============================");
	    log.info("OrderId : {}",notification.getOrderId());
	    log.info("Message : {}",notification.getMessage());

	}
}