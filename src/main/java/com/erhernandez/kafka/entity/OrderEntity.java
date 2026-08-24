package com.erhernandez.kafka.entity;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "orders")
public class OrderEntity {

    @Id
    private Long id;

    private String customerName;

    private String priority;

    private String product;

    private Integer quantity;

    private BigDecimal price;

    private LocalDateTime createdAt;
}
