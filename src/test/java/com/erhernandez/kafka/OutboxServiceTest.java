package com.erhernandez.kafka;

import com.erhernandez.kafka.entity.OutboxEventEntity;
import com.erhernandez.kafka.repository.OutboxEventRepository;
import com.erhernandez.kafka.service.OutboxService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class OutboxServiceTest {

    @Autowired
    private OutboxService outboxService;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Test
    void shouldCreateAndPersistOutboxEvent() {

        Long orderId = 1001L;

        String payload = """
                {
                  "orderId": 1001,
                  "customerName": "Edgar",
                  "product": "Laptop",
                  "quantity": 2
                }
                """;

        OutboxEventEntity saved = outboxService.save(
                orderId,
                "Order",
                "ORDER_CREATED",
                payload
        );

        assertThat(saved.getId()).isNotNull();
        assertThat(saved.getAggregateId()).isEqualTo(orderId);
        assertThat(saved.getAggregateType()).isEqualTo("Order");
        assertThat(saved.getEventType()).isEqualTo("ORDER_CREATED");
        assertThat(saved.getPayload()).contains("\"orderId\": 1001");
        assertThat(saved.getCreatedAt()).isNotNull();
        assertThat(saved.getProcessedAt()).isNull();

        assertThat(outboxEventRepository.findById(saved.getId()))
                .isPresent();

        outboxEventRepository.deleteById(saved.getId());
    }
}
