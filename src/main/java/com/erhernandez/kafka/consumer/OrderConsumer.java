package com.erhernandez.kafka.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumer {

    @KafkaListener(topics = "orders", groupId = "order-group")
    public void consume(String message) {

        System.out.println("--------------------------------");
        System.out.println("Message received:");
        System.out.println(message);
        System.out.println("--------------------------------");

    }

}