package com.erhernandez.kafka.service;

import com.erhernandez.kafka.entity.OrderEntity;
import com.erhernandez.kafka.entity.OutboxEventEntity;
import com.erhernandez.kafka.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class OrderPersistenceService {

    private final OrderRepository orderRepository;
    private final OutboxService outboxService;

    @Transactional
    public OrderEntity save(OrderEntity order) {

        OrderEntity savedOrder = orderRepository.save(order);

        String payload = """
                {
                  "orderId": %d,
                  "customerName": "%s",
                  "priority": "%s",
                  "product": "%s",
                  "quantity": %d,
                  "price": %.2f,
                  "createdAt": "%s"
                }
                """.formatted(
                savedOrder.getOrderId(),
                savedOrder.getCustomerName(),
                savedOrder.getPriority(),
                savedOrder.getProduct(),
                savedOrder.getQuantity(),
                savedOrder.getPrice(),
                savedOrder.getCreatedAt()
        );
        outboxService.save(
                savedOrder.getOrderId(),
                "Order",
                "ORDER_CREATED",
                payload
        );

        return savedOrder;
    }
}
