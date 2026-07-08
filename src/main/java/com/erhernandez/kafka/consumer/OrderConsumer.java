package com.erhernandez.kafka.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.erhernandez.kafka.dto.Order;

@Service
public class OrderConsumer {

    @KafkaListener(topics = "orders", groupId = "order-group")
    public void consume(Order order) {

    	System.out.println("--------------------------------");
    	System.out.println("Order received");
    	System.out.println("--------------------------------");
    	System.out.println("Order Id      : " + order.getOrderId());
        System.out.println("Customer Name : " + order.getCustomerName());
        System.out.println("Total Amount  : " + order.getTotalAmount());
        System.out.println("--------------------------------");

    }

}