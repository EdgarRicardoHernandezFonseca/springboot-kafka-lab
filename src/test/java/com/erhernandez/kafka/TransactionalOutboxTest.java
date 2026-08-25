package com.erhernandez.kafka;

import com.erhernandez.kafka.entity.OrderEntity;
import com.erhernandez.kafka.repository.OrderRepository;
import com.erhernandez.kafka.service.OrderPersistenceService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
class TransactionalOutboxTest {

    @Autowired
    private OrderPersistenceService orderPersistenceService;

    @Autowired
    private OrderRepository orderRepository;

    @Test
    void shouldPersistOrderAndOutboxInSameTransaction() {

        OrderEntity order = new OrderEntity();

        order.setOrderId(2001L);
        order.setCustomerName("Edgar");
        order.setPriority("HIGH");
        order.setProduct("Laptop");
        order.setQuantity(1);
        order.setPrice(2500.00);
        order.setCreatedAt(Instant.now());

        OrderEntity saved = orderPersistenceService.save(order);

        assertThat(saved).isNotNull();

        assertThat(orderRepository.findById(2001L))
                .isPresent();

        orderRepository.deleteById(2001L);
    }
}