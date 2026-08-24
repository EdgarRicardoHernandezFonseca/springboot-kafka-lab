package com.erhernandez.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import com.erhernandez.kafka.entity.OrderEntity;
import com.erhernandez.kafka.repository.OrderRepository;

@SpringBootTest
class OrderPersistenceServiceTest {

    @Autowired
    private OrderRepository orderRepository;

    @Test
    void shouldPersistOrder() {

        OrderEntity order = new OrderEntity();

        order.setOrderId(1008L);
        order.setCustomerName("Edgar Hernandez");
        order.setPriority("HIGH");
        order.setProduct("Laptop");
        order.setQuantity(1);
        order.setPrice(4500.00);
        order.setCreatedAt(Instant.now());

        OrderEntity saved = orderRepository.save(order);

        assertNotNull(saved);
        assertEquals(1008L, saved.getOrderId());
        
        orderRepository.deleteById(1008L);
    }
}