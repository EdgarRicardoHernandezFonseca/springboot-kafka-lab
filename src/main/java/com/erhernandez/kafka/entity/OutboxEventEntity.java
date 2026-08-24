package com.erhernandez.kafka.entity;

import java.time.LocalDateTime;
import java.util.UUID;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;

@Entity
@Table(name = "outbox_event")
public class OutboxEventEntity {

    @Id
    private UUID id;

    private String aggregateType;

    private String aggregateId;

    private String eventType;

    @Lob
    private String payload;

    private String status;

    private LocalDateTime createdAt;

    private LocalDateTime publishedAt;
}
