package com.erhernandez.kafka.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class AuditService {
	
    private static final Logger log =
            LoggerFactory.getLogger(AuditService.class);

    public void audit(String action, Long orderId) {

    	log.info("");
    	log.info("[AuditService]");
    	log.info("Recording audit event...");
    	log.info("Action          : {}", action);
    	log.info("Order ID        : {}", orderId);
    }
}