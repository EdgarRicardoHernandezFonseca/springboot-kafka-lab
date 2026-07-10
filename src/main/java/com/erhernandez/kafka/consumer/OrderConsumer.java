package com.erhernandez.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;

import com.erhernandez.kafka.dto.Order;

@Service
public class OrderConsumer {
	
	private static final Logger log =
	        LoggerFactory.getLogger(OrderConsumer.class);

    @KafkaListener(topics = "orders", groupId = "order-group")
    public void consume(Order order,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

    	log.info("--------------------------------");
    	log.info("Order received");
    	log.info("--------------------------------");
    	log.info("Order Id      : {}", order.getOrderId());
    	log.info("Customer Name : {}", order.getCustomerName());
    	log.info("Total Amount  : {}", order.getTotalAmount());

    	log.info("Partition     : {}", partition);
    	log.info("Offset        : {}", offset);

    }

}