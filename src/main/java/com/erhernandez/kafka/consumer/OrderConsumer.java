package com.erhernandez.kafka.consumer;

import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;
import com.erhernandez.kafka.avro.OrderCreated;
import com.erhernandez.kafka.commons.LogConstants;
import com.erhernandez.kafka.event.EventType;
import com.erhernandez.kafka.service.AuditService;
import com.erhernandez.kafka.service.InventoryService;
import com.erhernandez.kafka.service.NotificationService;
import com.erhernandez.kafka.service.OrderService;
import com.erhernandez.kafka.validator.OrderValidator;

@Service
public class OrderConsumer {
	
	private static final LogConstants LogConstants = new LogConstants();
	
	private static final Logger log =
	        LoggerFactory.getLogger(OrderConsumer.class);
	
	private final OrderService orderService;
	private final InventoryService inventoryService;
	private final NotificationService notificationService;
	private final AuditService auditService;
	private final OrderValidator orderValidator;

	public OrderConsumer(
	        OrderService orderService,
	        InventoryService inventoryService,
	        NotificationService notificationService,
	        AuditService auditService,
	        OrderValidator orderValidator) {

	    this.orderService = orderService;
	    this.inventoryService = inventoryService;
	    this.notificationService = notificationService;
	    this.auditService = auditService;
	    this.orderValidator = orderValidator;
	}
			
    @KafkaListener(
    		topics = "orders", 
    		groupId = "order-processing",
            containerFactory = "orderKafkaListenerFactory"
    )
    public void consume(
    		@Payload OrderCreated order,
    		@Header("eventType") String eventType,
            @Header("eventVersion") String eventVersion,
            @Header("source") String source,
            @Header("correlationId") String correlationId,
    		Acknowledgment ack,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset
            ) {
    	
    	
    	try {
    		
    		log.info("===== ORDER CONSUMER =====");
    		
    		log.info(order.getClass().getName());
        	
        	logHeaders(
                    order,
                    eventType,
                    eventVersion,
                    source,
                    correlationId,
                    partition,
                    offset);
        	
        	log.info("");
            log.info("Executing Business Logic...");
        	
        	EventType type =
                    EventType.valueOf(eventType);

            switch (type) {

                case ORDER_CREATED:
                    processCreate(order, ack, partition, offset);
                    break;

                case ORDER_UPDATED:
                    processUpdate(order, ack, partition, offset);
                    break;

                case ORDER_CANCELLED:
                    processCancel(order, ack, partition, offset);
                    break;

                default:
                    throw new IllegalArgumentException(
                            "Unsupported event");
            }
            
            log.info("");
            log.info("Finished Successfully.");
            log.info("");
            log.info(LogConstants.LINE);
            log.info("ORDER PROCESSING FINISHED");
            log.info(LogConstants.LINE);

    	}
    	catch (Exception ex) {

    	    log.error("ERROR EN OrderConsumer", ex);

    	    throw ex;
    	}
    	
    	
    }
    
    private void processCreate(
    		OrderCreated order,
            Acknowledgment ack,
            int partition,
            long offset) {
    	
    	log.info("");
    	log.info("Publishing Downstream Events...");

        orderService.createOrder(order.getOrderId());

        inventoryService.reserveInventory(order.getOrderId());

        notificationService.sendConfirmation(order.getOrderId());

        auditService.audit("ORDER_CREATED", order.getOrderId());
        
        completeProcessing(order, ack, partition, offset);
    }
    
    private void processUpdate(
    		OrderCreated order,
            Acknowledgment ack,
            int partition,
            long offset) {
    	
    	log.info("");
    	log.info("Publishing Downstream Events...");

    	orderService.updateOrder(order.getOrderId());

    	inventoryService.reserveInventory(order.getOrderId());

    	auditService.audit("ORDER_UPDATED", order.getOrderId());

    	completeProcessing(order, ack, partition, offset);
    }
    
    private void processCancel(
    		OrderCreated order,
            Acknowledgment ack,
            int partition,
            long offset) {

    	log.info("");
    	log.info("Publishing Downstream Events...");
    	
    	orderService.cancelOrder(order.getOrderId());

    	inventoryService.releaseInventory(order.getOrderId());

    	notificationService.publishRefund(order.getOrderId());

    	auditService.audit("ORDER_CANCELLED", order.getOrderId());
        
    	completeProcessing(order, ack, partition, offset);
    }
    
    private void logHeaders(
    		OrderCreated order,
    		String eventType,
            String eventVersion,
            String source,
            String correlationId,
    		int partition,
            long offset
    	    ) {
		
    	log.info("");
        log.info(LogConstants.LINE);
        log.info("ORDER PROCESSING STARTED");
        log.info(LogConstants.LINE);
        log.info("Consumer Group           : order-processing");
        log.info("Consumer Thread Instance : {}", Thread.currentThread().getName().replace("org.springframework.kafka.",""));
    	log.info("Order ID                 : {}", order.getOrderId());
    	log.info("Partition                : {}", partition);
    	log.info("Offset                   : {}", offset);
        log.info("Correlation ID           : {}", correlationId);
        log.info("Event Type               : {}", eventType);
        log.info("Event Version            : {}", eventVersion);
        log.info("Source                   : {}", source);      
        Instant timestamp = Instant.now();
        log.info("Timestamp		           : {}", timestamp);
        
		logHeadersFirstPhase();
		logHeadersSecondPhase();
		logHeadersThirdPhase();
		logHeadersFourthPhase();
		logHeadersFifthPhase();		
    }
    
    private void logHeadersFirstPhase() {
    	
        log.info("");
        log.info("Receiving Event...");
    }
    
    private void logHeadersSecondPhase() {
    	
    	log.info("");
    	log.info("Reading Kafka Headers...");
    }
    
    private void logHeadersThirdPhase() {
    	
    	log.info("");
    	log.info("Validating Event Metadata...");
    }
    
    private void logHeadersFourthPhase() {
    	
    	log.info("");
    	log.info("Routing Event...");
    }
    
    private void logHeadersFifthPhase() {

        log.info("");
        log.info("Avro payload successfully received...");
    }
        
    private void completeProcessing(
    		OrderCreated order, 
    		Acknowledgment ack,
            int partition,
            long offset
    		) {
    	
    	orderValidator.validate(order);
    	
    	log.info(LogConstants.SECTION);
		log.info("ORDER CONSUMER");
		log.info("Partition : {}", partition);
		log.info("Offset    : {}", offset);
		log.info("Order ID  : {}", order.getOrderId());
		log.info(LogConstants.SECTION);
		log.info("Executing Business Logic...");
		log.info("Processing order...");
		log.info("Order ID : {}", order.getOrderId());
		// Simulation of business processing
    	try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

    	log.info("Business completed.");
    	log.info("");
    	log.info("Acknowledging Offset...");
    	
    	ack.acknowledge();

    	log.info("");
        log.info("Offset committed manually.");
    }
}