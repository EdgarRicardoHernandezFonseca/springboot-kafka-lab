package com.erhernandez.kafka.producer;

import com.erhernandez.kafka.avro.OrderCreated;
import com.erhernandez.kafka.entity.OutboxEventEntity;
import com.erhernandez.kafka.service.OutboxService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    private final KafkaTemplate<String, OrderCreated> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Scheduled(fixedDelay = 5000)
    public void publishPendingEvents() {

        var events = outboxService.findPendingEvents();

        for (OutboxEventEntity event : events) {

            log.info(
                    "Publishing outbox event id={}, eventType={}",
                    event.getId(),
                    event.getEventType()
            );

            try {

                JsonNode payload = objectMapper.readTree(event.getPayload());

                OrderCreated orderCreated = OrderCreated.newBuilder()
                        .setOrderId(payload.get("orderId").asLong())
                        .setCustomerName(payload.get("customerName").asText())
                        .setPriority(payload.get("priority").asText())
                        .setProduct(payload.get("product").asText())
                        .setQuantity(payload.get("quantity").asInt())
                        .setPrice(payload.get("price").asDouble())
                        .setCreatedAt(
                                payload.has("createdAt")
                                        ? payload.get("createdAt").asText()
                                        : event.getCreatedAt().toString()
                        )
                        .build();

                kafkaTemplate.send(
                        TOPIC,
                        String.valueOf(event.getAggregateId()),
                        orderCreated
                );

                outboxService.markAsProcessed(event);

                log.info(
                        "Outbox event marked as processed id={}",
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