package com.erhernandez.kafka.service;

import com.erhernandez.kafka.entity.OutboxEventEntity;
import com.erhernandez.kafka.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class OutboxService {

    private final OutboxEventRepository outboxEventRepository;

    public OutboxEventEntity save(
            Long aggregateId,
            String aggregateType,
            String eventType,
            String payload) {

        OutboxEventEntity event = new OutboxEventEntity();

        event.setId(UUID.randomUUID());
        event.setAggregateId(aggregateId);
        event.setAggregateType(aggregateType);
        event.setEventType(eventType);
        event.setPayload(payload);
        event.setCreatedAt(Instant.now());

        return outboxEventRepository.save(event);
    }
}
