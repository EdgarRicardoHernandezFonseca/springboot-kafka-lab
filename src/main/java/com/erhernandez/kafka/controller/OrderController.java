package com.erhernandez.kafka.controller;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.erhernandez.kafka.dto.Order;
import com.erhernandez.kafka.producer.OrderProducer;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderProducer producer;

    public OrderController(OrderProducer producer) {
        this.producer = producer;
    }

    @PostMapping
    public String sendOrder(@RequestBody Order order) {

        producer.send(order);

        return "Order event sent successfully.";

    }

}
