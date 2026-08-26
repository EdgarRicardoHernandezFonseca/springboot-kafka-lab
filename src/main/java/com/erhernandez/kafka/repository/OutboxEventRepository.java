package com.erhernandez.kafka.repository;

import com.erhernandez.kafka.entity.OutboxEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEventEntity, UUID> {

    List<OutboxEventEntity> findByProcessedAtIsNullOrderByCreatedAtAsc();
}
