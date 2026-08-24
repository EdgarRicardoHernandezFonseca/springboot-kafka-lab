package com.erhernandez.kafka.service;

import com.erhernandez.kafka.entity.OrderEntity;
import com.erhernandez.kafka.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class OrderPersistenceService {

    private final OrderRepository orderRepository;

    public OrderEntity save(OrderEntity order) {

        return orderRepository.save(order);
    }
}
