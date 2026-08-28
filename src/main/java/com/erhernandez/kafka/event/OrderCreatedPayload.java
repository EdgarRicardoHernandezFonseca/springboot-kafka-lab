package com.erhernandez.kafka.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderCreatedPayload {

	private Long orderId;

	private String customerName;

	private String priority;

	private String product;

	private Integer quantity;

	private Double price;

	private Instant createdAt;

}
