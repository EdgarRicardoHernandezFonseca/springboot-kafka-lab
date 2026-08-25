package com.erhernandez.kafka;

import com.erhernandez.kafka.entity.OutboxEventEntity;
import com.erhernandez.kafka.repository.OutboxEventRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class OutboxEventRepositoryTest {

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Test
    void shouldPersistOutboxEvent() {

        UUID eventId = UUID.randomUUID();

        OutboxEventEntity event = new OutboxEventEntity();

        event.setId(eventId);
        event.setAggregateType("Order");
        event.setAggregateId(1001L);
        event.setEventType("ORDER_CREATED");
        event.setPayload("""
                {
                  "orderId": 1001,
                  "customerName": "Edgar",
                  "product": "Laptop",
                  "quantity": 2
                }
                """);
        event.setCreatedAt(Instant.now());

        OutboxEventEntity saved =
                outboxEventRepository.save(event);

        assertThat(saved.getId()).isEqualTo(eventId);
        assertThat(saved.getAggregateType()).isEqualTo("Order");
        assertThat(saved.getAggregateId()).isEqualTo(1001L);
        assertThat(saved.getEventType()).isEqualTo("ORDER_CREATED");
        assertThat(saved.getPayload()).contains("\"orderId\": 1001");
        assertThat(saved.getCreatedAt()).isNotNull();
        assertThat(saved.getProcessedAt()).isNull();

        outboxEventRepository.deleteById(eventId);
    }
}
