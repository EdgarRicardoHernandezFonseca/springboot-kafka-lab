package com.erhernandez.kafka.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.erhernandez.kafka.dto.Order;

@Service
public class NotificationConsumer {

//    @KafkaListener(
//            topics = "orders",
//            groupId = "order-group")
//    public void consume(Order order) {
//
//        System.out.println("########################");
//        System.out.println("NOTIFICATION CONSUMER");
//        System.out.println("Order : " + order.getOrderId());
//        System.out.println("########################");
//
//    }

}