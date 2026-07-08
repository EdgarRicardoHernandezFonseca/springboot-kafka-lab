package com.erhernandez.kafka.producer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.erhernandez.kafka.dto.Order;

@Service
public class OrderProducer {

    private final KafkaTemplate<String, Order> kafkaTemplate;

    public OrderProducer(KafkaTemplate<String, Order> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(Order order) {

        String key = order.getOrderId().toString();

        kafkaTemplate.send("orders", key, order);

        System.out.println("--------------------------------");
        System.out.println("Order sent");
        System.out.println("--------------------------------");
        System.out.println("Key         : " + key);
        System.out.println("Customer    : " + order.getCustomerName());
        System.out.println("--------------------------------");
    }

}
