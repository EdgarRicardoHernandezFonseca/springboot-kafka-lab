package com.erhernandez.kafka.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class OrderService {

    private static final Logger log =
            LoggerFactory.getLogger(OrderService.class);

    public void createOrder(Long orderId) {

    	log.info("");
    	log.info("[OrderService]");
        log.info("Creating order {} in database...", orderId);
    }

    public void updateOrder(Long orderId) {

    	log.info("");
    	log.info("[OrderService]");
        log.info("Updating order {}...", orderId);
    }

    public void cancelOrder(Long orderId) {
    	
    	log.info("");
    	log.info("[OrderService]");
        log.info("Cancelling order {} in database...", orderId);
    }
}