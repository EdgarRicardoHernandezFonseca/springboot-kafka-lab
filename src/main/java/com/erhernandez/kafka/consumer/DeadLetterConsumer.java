package com.erhernandez.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.erhernandez.kafka.dto.Order;

@Component
public class DeadLetterConsumer {
	
	private static final Logger log =
	        LoggerFactory.getLogger(DeadLetterConsumer.class);

	@KafkaListener(
	        topics = "orders-dlt",
	        groupId = "dlt-group")
	public void consume(Order order){

	    log.info("==============================");
	    log.info("MESSAGE ARRIVED TO DLT");
	    log.info("==============================");
	    log.info("OrderId : {}",order.getOrderId());
	    log.info("Customer: {}",order.getCustomerName());
	    log.info("Amount  : {}",order.getTotalAmount());

	}
}