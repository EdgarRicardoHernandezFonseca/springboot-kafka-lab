# Spring Boot Kafka Lab
## v1.6.1 – Manual Commit and AckMode.MANUAL

---

# Objective

Learn how **Manual Commit** works in Apache Kafka using Spring Boot and understand the purpose of **AckMode.MANUAL**.

Unlike automatic commits, the consumer explicitly decides **when a message has been processed successfully** before committing its offset.

This mechanism is commonly used in enterprise applications where message processing must be reliable.

---

# Why Manual Commit?

By default Kafka commits offsets automatically.

```
Receive message
↓

Process message
↓

Kafka commits offset automatically
```

If the application crashes while processing, Kafka may think the message was already processed and it could be lost.

Manual Commit solves this problem.

```
Receive message
↓

Process business logic

↓

Everything OK?

↓

YES

↓

Commit Offset
```

The consumer controls exactly when the offset is committed.

---

# AckMode.MANUAL

Spring Kafka provides several acknowledgment modes.

For this lab we use:

```
AckMode.MANUAL
```

This mode requires calling:

```java
acknowledgment.acknowledge();
```

Only then will Kafka store the new offset.

---

# Configuration

The listener container factory is configured as:

```java
factory.getContainerProperties()
       .setAckMode(ContainerProperties.AckMode.MANUAL);
```

Automatic offset commits are disabled:

```yaml
spring:
  kafka:
    consumer:
      enable-auto-commit: false
```

---

# Consumer Example

```java
@KafkaListener(topics = "orders")
public void consume(Order order,
                    Acknowledgment acknowledgment) {

    processBusiness(order);

    acknowledgment.acknowledge();
}
```

The important part is that the offset is committed only after the business logic finishes successfully.

---

# Processing Flow

```
Producer
    │
    ▼
Kafka Topic
    │
    ▼
Consumer receives message
    │
    ▼
Business Logic
    │
    ▼
Business completed
    │
    ▼
acknowledge()
    │
    ▼
Offset committed
```

---

# Lab Execution

Each consumer simulates business processing for several seconds.

Example:

```
Processing order...

Business completed.

Offset committed manually.
```

This proves that the offset is committed only after successful processing.

---

# Advantages

✔ Greater reliability

✔ Prevents premature offset commits

✔ Better control over processing

✔ Required for critical business operations

✔ Common in financial systems

✔ Useful for payment processing

✔ Useful for inventory management

✔ Useful for order processing

---

# Automatic Commit vs Manual Commit

| Automatic Commit | Manual Commit |
|------------------|--------------|
| Kafka commits offsets automatically | Application commits offsets |
| Less code | More control |
| Faster | More reliable |
| Possible message loss if processing fails | Prevents committing before processing completes |
| Good for simple consumers | Recommended for enterprise systems |

---

# Real-World Use Cases

Manual Commit is frequently used in systems such as:

- Banking transactions
- Payment gateways
- Order processing
- Inventory updates
- Invoice generation
- Logistics
- Financial reconciliation
- Event-driven microservices

Whenever losing a message is unacceptable, Manual Commit is usually preferred.

---

# What We Learned

In this version we learned how to:

- Disable automatic offset commits.
- Configure `AckMode.MANUAL`.
- Inject the `Acknowledgment` object.
- Commit offsets manually.
- Ensure business processing finishes before committing offsets.
- Understand why enterprise applications often prefer Manual Commit over automatic commits.

This version provides the foundation for implementing more advanced reliability features such as retries, dead-letter topics (DLT), and transactional message processing.