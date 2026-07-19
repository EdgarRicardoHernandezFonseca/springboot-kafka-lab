package com.erhernandez.kafka.consumer;

import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;

import com.erhernandez.kafka.commons.LogConstants;
import com.erhernandez.kafka.dto.Order;
import com.erhernandez.kafka.dto.OrderV2;
import com.erhernandez.kafka.event.EventType;
import com.erhernandez.kafka.service.AuditService;
import com.erhernandez.kafka.service.InventoryService;
import com.erhernandez.kafka.service.NotificationService;
import com.erhernandez.kafka.service.OrderService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class OrderConsumer {
	
	private static final LogConstants LogConstants = new LogConstants();
	
	private static final Logger log =
	        LoggerFactory.getLogger(OrderConsumer.class);
	
	private final ObjectMapper objectMapper;
	private final OrderService orderService;
	private final InventoryService inventoryService;
	private final NotificationService notificationService;
	private final AuditService auditService;

	public OrderConsumer(
	        ObjectMapper objectMapper,
	        OrderService orderService,
	        InventoryService inventoryService,
	        NotificationService notificationService,
	        AuditService auditService) {

	    this.objectMapper = objectMapper;
	    this.orderService = orderService;
	    this.inventoryService = inventoryService;
	    this.notificationService = notificationService;
	    this.auditService = auditService;
	}

    @KafkaListener(
    		topics = "orders", 
    		groupId = "order-processing"
            )
    public void consume(
    		String payload,
    		@Header("eventType") String eventType,
            @Header("eventVersion") String eventVersion,
            @Header("source") String source,
            @Header("correlationId") String correlationId,
    		Acknowledgment ack,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {
    	
    	try {

    	    if ("v1".equals(eventVersion)) {

    	        Order order =
    	                objectMapper.readValue(payload, Order.class);

    	        processV1(order,
    	                eventType,
    	                eventVersion,
    	                source,
    	                correlationId,
    	                ack,
    	                partition,
    	                offset);

    	    } else if ("v2".equals(eventVersion)) {

    	        OrderV2 order =
    	                objectMapper.readValue(payload, OrderV2.class);

    	        processV2(order,
    	                eventType,
    	                eventVersion,
    	                source,
    	                correlationId,
    	                ack,
    	                partition,
    	                offset);

    	    } else {

    	        throw new IllegalArgumentException(
    	                "Unsupported event version : " + eventVersion
    	        );

    	    }

    	}
    	catch (JsonProcessingException ex) {
    	    throw new IllegalArgumentException("Invalid event payload", ex);
    	}
    }
    
    private void processV1(
    		Order order,
            String eventType,
            String eventVersion,
            String source,
            String correlationId,
            Acknowledgment ack,
            int partition,
            long offset){
    	
    	logHeaders(eventType, eventVersion, source, correlationId, partition, offset);

        log.info("Processing V1");

        processBusiness(order, ack, partition, offset);
        
    }
    
    private void processV2(
    		OrderV2 order,
            String eventType,
            String eventVersion,
            String source,
            String correlationId,
            Acknowledgment ack,
            int partition,
            long offset){
    	
    	logHeaders(eventType, eventVersion, source, correlationId, partition, offset);

    	log.info("");
        log.info("Processing V2");
        log.info("");
        log.info("Executing Business Logic...");
        
        EventType type = EventType.valueOf(eventType);

     // Route the event according to its business type.
        switch (type) {

        case ORDER_CREATED:

            processCreate(
                    order,
                    ack,
                    partition,
                    offset);

            break;

        case ORDER_UPDATED:

            processUpdate(
                    order,
                    ack,
                    partition,
                    offset);

            break;

        case ORDER_CANCELLED:

            processCancel(
                    order,
                    ack,
                    partition,
                    offset);

            break;

        default:

            throw new IllegalArgumentException(
                    "Unsupported event type : "
                            + eventType);
        }
        
        log.info("");
        log.info("Finished Successfully.");
        log.info("");
        log.info(LogConstants.LINE);
        log.info("ORDER PROCESSING FINISHED");
        log.info(LogConstants.LINE);
        
    }
    
    private void processCreate(
            OrderV2 order,
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
            OrderV2 order,
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
            OrderV2 order,
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
        log.info("Correlation ID : {}", correlationId);
        log.info("Event Type     : {}", eventType);
        log.info("Event Version  : {}", eventVersion);
        log.info("Source         : {}", source);
        log.info("Partition      : {}", partition);
        log.info("Offset         : {}", offset);        
        Instant timestamp = Instant.now();
        log.info("Timestamp		 : {}", timestamp);
        
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
        log.info("Deserializing Payload...");
    }
    
    private void processBusiness(
    		Order order, 
    		Acknowledgment ack,
            int partition,
            long offset
    		) {
    	
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
    	
    	log.info(LogConstants.SECTION);

		log.info("ORDER CONSUMER");
		log.info("Partition : {}", partition);
		log.info("Offset    : {}", offset);
		log.info("Order ID  : {}", order.getOrderId());

		log.info(LogConstants.SECTION);

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
    
    private void completeProcessing(
    		OrderV2 order, 
    		Acknowledgment ack,
            int partition,
            long offset
    		) {
    	
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
    	
    	log.info(LogConstants.SECTION);
		log.info("ORDER CONSUMER");
		log.info("Partition : {}", partition);
		log.info("Offset    : {}", offset);
		log.info("Order ID  : {}", order.getOrderId());
		log.info(LogConstants.SECTION);
		log.info("Executing Business Logic...");
		log.info("Processing order...");
		log.info("Order ID : {}", order.getOrderId());
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