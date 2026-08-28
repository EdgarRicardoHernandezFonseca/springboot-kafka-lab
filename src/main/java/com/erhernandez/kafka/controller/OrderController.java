package com.erhernandez.kafka.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.erhernandez.kafka.dto.Order;
import com.erhernandez.kafka.dto.OrderV2;
import com.erhernandez.kafka.event.EventType;
import com.erhernandez.kafka.producer.OrderProducer;
import com.erhernandez.kafka.producer.OrderProducerV2;
import com.erhernandez.kafka.service.OrderPersistenceService;
import com.erhernandez.kafka.entity.OrderEntity;

import java.time.Instant;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderProducer producer;
    private final OrderProducerV2 producerV2;
    private final OrderPersistenceService orderPersistenceService;

    public OrderController(
            OrderProducer producer,
            OrderProducerV2 producerV2,
            OrderPersistenceService orderPersistenceService) {

        this.producer = producer;
        this.producerV2 = producerV2;
        this.orderPersistenceService = orderPersistenceService;
    }

    @PostMapping
    public String sendOrder(@RequestBody Order order){

        producer.send(order);

        return "V1 sent";

    }

    @PostMapping("/v2")
    public String sendOrderV2(@RequestBody OrderV2 order){

        producerV2.send(order, EventType.ORDER_CREATED);

        return "V2 sent";

    }
    
    @PostMapping("/v2/create")
    public ResponseEntity<String> createOrder(
            @RequestBody OrderV2 order){

    	OrderEntity orderEntity = new OrderEntity(); 
    	
    	orderEntity.setOrderId(order.getOrderId()); 
    	orderEntity.setCustomerName(order.getCustomerName()); 
    	orderEntity.setPriority(order.getPriority()); 
    	orderEntity.setProduct(order.getProduct()); 
    	orderEntity.setQuantity(order.getQuantity()); 
    	orderEntity.setPrice(order.getPrice()); 
    	orderEntity.setCreatedAt(Instant.now());
    	
    	orderPersistenceService.save(orderEntity);
    	
        return ResponseEntity.ok( 
        		"ORDER_CREATED stored in Transactional Outbox successfully." 
        );
    }
    
    @PostMapping("/v2/update")
    public ResponseEntity<String> updateOrder(
            @RequestBody OrderV2 order){

        producerV2.send(order, EventType.ORDER_UPDATED);

        return ResponseEntity.ok(
                "ORDER_UPDATED sent successfully."
        );
    }
    
    @PostMapping("/v2/cancel")
    public ResponseEntity<String> cancelOrder(
            @RequestBody OrderV2 order){

        producerV2.send(order, EventType.ORDER_CANCELLED);

        return ResponseEntity.ok(
                "ORDER_CANCELLED sent successfully."
        );
    }
}
