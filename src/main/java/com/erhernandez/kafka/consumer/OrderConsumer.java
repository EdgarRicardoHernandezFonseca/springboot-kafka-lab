package com.erhernandez.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;

import com.erhernandez.kafka.dto.Order;
import com.erhernandez.kafka.dto.OrderV2;
import com.erhernandez.kafka.event.EventType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class OrderConsumer {
	
	private static final Logger log =
	        LoggerFactory.getLogger(OrderConsumer.class);
	
	private final ObjectMapper objectMapper;

	public OrderConsumer(ObjectMapper objectMapper) {
	    this.objectMapper = objectMapper;
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
    	
    	logHeaders(eventType, eventVersion, source, correlationId);

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
    	
    	logHeaders(eventType, eventVersion, source, correlationId);

        log.info("Processing V2");
        
        EventType type = EventType.valueOf(eventType);

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
        
    }
    
    private void processCreate(
            OrderV2 order,
            Acknowledgment ack,
            int partition,
            long offset) {

        log.info("========== CREATE ==========");
        log.info("Creating new order...");
        log.info("Customer : {}", order.getCustomerName());
        log.info("--------------------------------");
        log.info("Creating order {} in database...", order.getOrderId());
        log.info("Reserving inventory {} ...", order.getOrderId());
        log.info("Sending confirmation email {} ...", order.getOrderId());
        log.info("--------------------------------");
        
        processBusiness(order, ack, partition, offset);
    }
    
    private void processUpdate(
            OrderV2 order,
            Acknowledgment ack,
            int partition,
            long offset) {

        log.info("========== UPDATE ==========");
        log.info("Updating existing order...");
        log.info("Order ID : {}", order.getOrderId());
        log.info("--------------------------------");
        log.info("Updating order {} ...", order.getOrderId());
        log.info("Reserving inventory {} ...", order.getOrderId());
        log.info("--------------------------------");

        processBusiness(order, ack, partition, offset);
    }
    
    private void processCancel(
            OrderV2 order,
            Acknowledgment ack,
            int partition,
            long offset) {

        log.info("========== CANCEL ==========");
        log.info("Cancelling order...");
        log.info("Order ID : {}", order.getOrderId());
        log.info("--------------------------------");
        log.info("Cancelling order {} in database...", order.getOrderId());
        log.info("Releasing inventory {} ...", order.getOrderId());
        log.info("Publishing refund event {} ...", order.getOrderId());
        log.info("--------------------------------");
        
        processBusiness(order, ack, partition, offset);
    }
    
    private void logHeaders(
    		String eventType,
            String eventVersion,
            String source,
            String correlationId
    	    ) {
		log.info("--------------------------------");
		log.info("Message Headers");
		log.info("--------------------------------");

		log.info("Event Type    : {}", eventType);
		log.info("Version       : {}", eventVersion);
		log.info("Source        : {}", source);
		log.info("CorrelationId : {}", correlationId);	
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
    	
    	log.info("--------------------------------");

		log.info("ORDER CONSUMER");
		log.info("Partition : {}", partition);
		log.info("Offset    : {}", offset);
		log.info("Order ID  : {}", order.getOrderId());

		log.info("--------------------------------");

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
    
    private void processBusiness(
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
    	
    	log.info("--------------------------------");

		log.info("ORDER CONSUMER");
		log.info("Partition : {}", partition);
		log.info("Offset    : {}", offset);
		log.info("Order ID  : {}", order.getOrderId());

		log.info("--------------------------------");

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
}