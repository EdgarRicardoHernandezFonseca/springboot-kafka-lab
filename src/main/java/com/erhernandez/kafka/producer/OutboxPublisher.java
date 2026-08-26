package com.erhernandez.kafka.producer;

import com.erhernandez.kafka.entity.OutboxEventEntity;
import com.erhernandez.kafka.service.OutboxService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class OutboxPublisher {

    private static final String TOPIC = "orders";

    private final OutboxService outboxService;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Scheduled(fixedDelay = 5000)
    public void publishPendingEvents() {

        var events = outboxService.findPendingEvents();

        for (OutboxEventEntity event : events) {

            log.info(
                    "Publishing outbox event id={}, eventType={}",
                    event.getId(),
                    event.getEventType()
            );

            kafkaTemplate.send(
                    TOPIC,
                    String.valueOf(event.getAggregateId()),
                    event.getPayload()
            );

            outboxService.markAsProcessed(event);

            log.info(
                    "Outbox event marked as processed id={}",
                    event.getId()
            );
        }
    }
}
