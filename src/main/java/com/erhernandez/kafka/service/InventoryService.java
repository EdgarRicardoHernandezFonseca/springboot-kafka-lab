package com.erhernandez.kafka.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class InventoryService {
	
    private static final Logger log =
            LoggerFactory.getLogger(InventoryService.class);

    public void reserveInventory(Long orderId) {

    	log.info("");
    	log.info("[InventoryService]");
        log.info("Reserving inventory {}...", orderId);
    }

    public void releaseInventory(Long orderId) {

    	log.info("");
    	log.info("[InventoryService]");
        log.info("Releasing inventory {}...", orderId);
    }
}
