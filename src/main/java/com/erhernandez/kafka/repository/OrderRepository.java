package com.erhernandez.kafka.repository;

import com.erhernandez.kafka.entity.OrderEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<OrderEntity, Long> {
}