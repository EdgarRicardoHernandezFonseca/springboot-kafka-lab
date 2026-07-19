package com.erhernandez.kafka.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class NotificationService {
	
    private static final Logger log =
            LoggerFactory.getLogger(NotificationService.class);

    public void sendConfirmation(Long orderId) {

    	log.info("");
    	log.info("[NotificationService]");
        log.info("Sending confirmation email {}...", orderId);
    }

    public void publishRefund(Long orderId) {
    	
    	log.info("");
    	log.info("[NotificationService]");
        log.info("Publishing refund event {}...", orderId);
    }
}