package com.erhernandez.kafka.mapper;

import com.erhernandez.kafka.avro.OrderCreated;
import com.erhernandez.kafka.entity.OrderEntity;
import org.springframework.stereotype.Component;

@Component
public class OrderEventMapper {

    public OrderCreated toAvro(OrderEntity order) {

        return OrderCreated.newBuilder()
                .setOrderId(order.getOrderId())
                .setCustomerName(order.getCustomerName())
                .setPriority(order.getPriority())
                .setProduct(order.getProduct())
                .setQuantity(order.getQuantity())
                .setPrice(order.getPrice())
                .setCreatedAt(order.getCreatedAt().toString())
                .build();
    }
}