package com.erhernandez.kafka.producer;

import com.erhernandez.kafka.avro.OrderCreated;
import com.erhernandez.kafka.entity.OutboxEventEntity;
import com.erhernandez.kafka.service.OutboxService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
@ConditionalOnProperty(
        name = "kafka.lab.outbox.publisher.enabled",
        havingValue = "true",
        matchIfMissing = true
)
public class OutboxPublisher {

    private static final String TOPIC = "orders";
    private static final String EVENT_VERSION = "v2";
    private static final String SOURCE = "springboot-kafka-lab";

    private final OutboxService outboxService;
    private final KafkaTemplate<String, OrderCreated> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Scheduled(fixedDelay = 5000)
    public void publishPendingEvents() {

        var events = outboxService.findPendingEvents();

        for (OutboxEventEntity event : events) {

            try {

                log.info(
                        "Publishing outbox event id={}, eventType={}",
                        event.getId(),
                        event.getEventType()
                );

                OrderCreated orderCreated =
                        objectMapper.readValue(
                                event.getPayload(),
                                OrderCreated.class
                        );

                String correlationId =
                        UUID.randomUUID().toString();

                ProducerRecord<String, OrderCreated> record =
                        new ProducerRecord<>(
                                TOPIC,
                                String.valueOf(event.getAggregateId()),
                                orderCreated
                        );

                record.headers().add(
                        "eventType",
                        event.getEventType()
                                .getBytes(StandardCharsets.UTF_8)
                );

                record.headers().add(
                        "eventVersion",
                        EVENT_VERSION
                                .getBytes(StandardCharsets.UTF_8)
                );

                record.headers().add(
                        "source",
                        SOURCE
                                .getBytes(StandardCharsets.UTF_8)
                );

                record.headers().add(
                        "correlationId",
                        correlationId
                                .getBytes(StandardCharsets.UTF_8)
                );

                kafkaTemplate
                        .send(record)
                        .get();

                outboxService.markAsProcessed(event);

                log.info(
                        "Outbox event published and marked as processed. id={}",
                        event.getId()
                );

            } catch (Exception e) {

                log.error(
                        "Failed to publish outbox event id={}",
                        event.getId(),
                        e
                );
            }
        }
    }
}
