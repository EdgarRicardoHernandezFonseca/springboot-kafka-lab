package com.erhernandez.kafka;

import com.erhernandez.kafka.entity.OrderEntity;
import com.erhernandez.kafka.repository.OrderRepository;
import com.erhernandez.kafka.service.OrderPersistenceService;
import com.erhernandez.kafka.service.OutboxService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;

@SpringBootTest
class TransactionalOutboxRollbackTest {

    @Autowired
    private OrderPersistenceService orderPersistenceService;

    @Autowired
    private OrderRepository orderRepository;

    @MockBean
    private OutboxService outboxService;

    @Test
    void shouldRollbackOrderWhenOutboxFails() {

        Long orderId = System.currentTimeMillis();

        OrderEntity order = new OrderEntity();

        order.setOrderId(orderId);
        order.setCustomerName("Edgar");
        order.setPriority("HIGH");
        order.setProduct("Laptop");
        order.setQuantity(1);
        order.setPrice(2500.00);
        order.setCreatedAt(Instant.now());

        doThrow(new RuntimeException("Outbox persistence failed"))
                .when(outboxService)
                .save(
                        any(),
                        any(),
                        any(),
                        any()
                );

        assertThatThrownBy(() ->
                orderPersistenceService.save(order)
        )
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Outbox persistence failed");

        assertThat(orderRepository.findById(orderId))
                .isEmpty();
    }
}