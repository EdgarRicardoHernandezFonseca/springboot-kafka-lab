package com.erhernandez.kafka.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

@Entity
@Table(name = "orders")
@Getter
@Setter
@NoArgsConstructor
public class OrderEntity {

    @Id
    private Long orderId;

    private String customerName;

    private String priority;

    private String product;

    private Integer quantity;

    private Double price;

    private Instant createdAt;
}