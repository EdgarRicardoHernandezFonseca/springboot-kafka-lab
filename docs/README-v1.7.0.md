# v1.7.0 – Sending Custom Headers

## Objective

In distributed event-driven systems, the payload is often not enough.

Applications usually need additional metadata to describe the event without modifying the business object itself.

Kafka message headers allow developers to attach metadata to every message while keeping the payload clean.

This version demonstrates how to send custom Kafka headers using Spring Boot and Spring Kafka.

---

# What are Kafka Message Headers?

A Kafka message consists of:

```
+----------------------------+
| Key                        |
+----------------------------+
| Value (Payload)            |
+----------------------------+
| Headers                    |
+----------------------------+
```

The payload contains the business information.

Example:

```json
{
  "orderId":1001,
  "customerName":"Edgar Hernandez",
  "totalAmount":150000
}
```

Headers contain metadata about the message.

Example:

```
eventType = ORDER_CREATED
eventVersion = v1
source = springboot-kafka-lab
```

Unlike the payload, headers do not affect the business model.

---

# Why use Headers?

Headers are commonly used for:

- Event versioning
- Correlation IDs
- Request IDs
- Trace IDs
- Authentication tokens
- Tenant identification
- Source application
- Content type
- Retry count
- Processing information

This allows multiple services to process the same event differently without changing the payload.

---

# Example

Instead of sending only:

```
Order
```

we send:

```
Headers

eventType=ORDER_CREATED
eventVersion=v1
source=springboot-kafka-lab

Payload

Order
```

The consumer receives both.

---

# Spring Kafka Implementation

Instead of:

```java
kafkaTemplate.send("orders", order);
```

Spring allows sending a complete Message.

Example:

```java
Message<Order> message =
        MessageBuilder
            .withPayload(order)
            .setHeader(KafkaHeaders.TOPIC, "orders")
            .setHeader("eventType", "ORDER_CREATED")
            .setHeader("eventVersion", "v1")
            .setHeader("source", "springboot-kafka-lab")
            .build();

kafkaTemplate.send(message);
```

---

# Result in Kafka UI

Kafka UI displays:

```
Headers

eventVersion : v1
eventType    : ORDER_CREATED
source       : springboot-kafka-lab
```

Payload:

```json
{
    "orderId":1001,
    "customerName":"Edgar Hernandez",
    "totalAmount":150000
}
```

---

# Advantages

Using headers provides several benefits:

- Keeps the payload clean.
- Adds contextual information.
- Simplifies routing.
- Enables event versioning.
- Facilitates distributed tracing.
- Improves interoperability between microservices.

---

# Real-world Examples

A payment service could publish:

```
eventType=PAYMENT_COMPLETED
eventVersion=v2
source=payment-service
```

An inventory service could publish:

```
eventType=STOCK_UPDATED
warehouse=BOGOTA
priority=HIGH
```

A shipping service could publish:

```
eventType=ORDER_SHIPPED
carrier=DHL
trackingEnabled=true
```

All these messages may have completely different metadata while keeping their payload independent.

---

# Version Summary

Implemented features:

- Sending Kafka Message instead of raw payload.
- Custom message headers.
- Event metadata.
- Event version header.
- Source application header.
- Verification using Kafka UI.

Current project evolution:

- Producer
- Consumer
- Consumer Groups
- Manual Acknowledgement
- Retry Mechanism
- Dead Letter Topic (DLT)
- Non-Retryable Exceptions
- Kafka Message Headers (current version)

Next step:

Read and process custom headers inside Kafka consumers.