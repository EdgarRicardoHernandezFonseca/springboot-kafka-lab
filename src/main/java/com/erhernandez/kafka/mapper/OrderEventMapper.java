package com.erhernandez.kafka.mapper;

import com.erhernandez.kafka.avro.OrderCreated;
import com.erhernandez.kafka.entity.OrderEntity;
import com.erhernandez.kafka.event.OrderCreatedPayload;
import org.springframework.stereotype.Component;

@Component
public class OrderEventMapper {


	public OrderCreated toAvro(OrderEntity order) {

		return OrderCreated.newBuilder()
				.setOrderId(order.getOrderId())
				.setCustomerName(order.getCustomerName())
				.setPriority(order.getPriority())
				.setProduct(order.getProduct())
				.setQuantity(order.getQuantity())
				.setPrice(order.getPrice())
				.setCreatedAt(order.getCreatedAt().toString())
				.build();
	}

	public OrderCreated toAvro(OrderCreatedPayload payload) {

		return OrderCreated.newBuilder()
				.setOrderId(payload.getOrderId())
				.setCustomerName(payload.getCustomerName())
				.setPriority(payload.getPriority())
				.setProduct(payload.getProduct())
				.setQuantity(payload.getQuantity())
				.setPrice(payload.getPrice())
				.setCreatedAt(payload.getCreatedAt().toString())
				.build();
	}
}
