package com.erhernandez.kafka.service;

import com.erhernandez.kafka.entity.OrderEntity;
import com.erhernandez.kafka.event.OrderCreatedPayload;
import com.erhernandez.kafka.repository.OrderRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class OrderPersistenceService {

	private final OrderRepository orderRepository;
	private final OutboxService outboxService;
	private final ObjectMapper objectMapper;

	@Transactional
	public OrderEntity save(OrderEntity order) {

		OrderEntity savedOrder = orderRepository.save(order);

		OrderCreatedPayload payloadObject =
				new OrderCreatedPayload(
						savedOrder.getOrderId(),
						savedOrder.getCustomerName(),
						savedOrder.getPriority(),
						savedOrder.getProduct(),
						savedOrder.getQuantity(),
						savedOrder.getPrice(),
						savedOrder.getCreatedAt()
						);

		try {

			String payload =
					objectMapper.writeValueAsString(payloadObject);

			outboxService.save(
					savedOrder.getOrderId(),
					"Order",
					"ORDER_CREATED",
					payload
					);

		} catch (JsonProcessingException e) {

			throw new IllegalStateException(
					"Failed to serialize ORDER_CREATED payload",
					e
					);
		}

		return savedOrder;
	}
}
