package com.erhernandez.kafka.controller;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.erhernandez.kafka.dto.Order;
import com.erhernandez.kafka.dto.OrderV2;
import com.erhernandez.kafka.producer.OrderProducer;
import com.erhernandez.kafka.producer.OrderProducerV2;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderProducer producer;
    private final OrderProducerV2 producerV2;

    public OrderController(
            OrderProducer producer,
            OrderProducerV2 producerV2) {

        this.producer = producer;
        this.producerV2 = producerV2;
    }

    @PostMapping
    public String sendOrder(@RequestBody Order order){

        producer.send(order);

        return "V1 sent";

    }

    @PostMapping("/v2")
    public String sendOrderV2(@RequestBody OrderV2 order){

        producerV2.send(order);

        return "V2 sent";

    }
}
