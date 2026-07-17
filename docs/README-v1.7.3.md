# README – v1.7.3 Event Versioning

## Overview

As distributed systems evolve, message schemas also evolve. New business requirements often require adding, removing, or changing fields in existing events. If producers and consumers are updated at different times, incompatible message formats can easily break the communication between services.

**Event Versioning** is a common strategy used in event-driven architectures to maintain backward compatibility while allowing events to evolve over time.

In this laboratory, event versions are identified through **Kafka message headers**, allowing a consumer to process different versions of the same business event without changing the Kafka topic.

---

## Objectives

- Understand the concept of Event Versioning.
- Send different versions of the same event through a single Kafka topic.
- Use Kafka Headers to identify the event version.
- Deserialize messages according to their version.
- Keep backward compatibility between producers and consumers.

---

## Architecture

```
                Producer V1
                     │
                     │ eventVersion = v1
                     ▼
               +-------------+
               |   orders    |
               +-------------+
                     ▲
                     │ eventVersion = v2
                     │
                Producer V2


                     │
                     ▼
              OrderConsumer
                     │
         ┌───────────┴───────────┐
         │                       │
 eventVersion=v1         eventVersion=v2
         │                       │
         ▼                       ▼
      Order                 OrderV2
```

---

## Why Event Versioning?

Imagine that the original event looks like this:

```json
{
  "orderId": 1001,
  "customerName": "John Doe",
  "totalAmount": 150.00
}
```

Months later, the business requires storing the order priority.

Instead of breaking every consumer, a second version is introduced.

```json
{
  "orderId": 1001,
  "customerName": "John Doe",
  "priority": "HIGH"
}
```

Both events can coexist during the migration period.

---

## Version 1

DTO

```java
public class Order {

    private Long orderId;
    private String customerName;
    private Double totalAmount;

}
```

Header

```
eventVersion = v1
```

---

## Version 2

DTO

```java
public class OrderV2 {

    private Long orderId;
    private String customerName;
    private String priority;

}
```

Header

```
eventVersion = v2
```

---

## Sending Version Information

The producer includes the event version inside the Kafka message headers.

```java
record.headers().add(
    new RecordHeader(
        "eventVersion",
        "v2".getBytes(StandardCharsets.UTF_8)
    )
);
```

Example headers

```
eventType = ORDER_CREATED

eventVersion = v2

source = springboot-kafka-lab

correlationId = 8dad3252-e045...
```

---

## Reading Headers

The consumer reads the version directly from the Kafka headers.

```java
@Header("eventVersion")
String eventVersion
```

This allows the application to decide how to deserialize the message.

---

## Consumer Routing

The consumer selects the appropriate DTO according to the event version.

```java
if ("v1".equals(eventVersion)) {

    Order order =
        objectMapper.readValue(payload, Order.class);

    processV1(order);

}
else if ("v2".equals(eventVersion)) {

    OrderV2 order =
        objectMapper.readValue(payload, OrderV2.class);

    processV2(order);

}
```

This approach keeps a single Kafka topic while supporting multiple message versions.

---

## Benefits

- Backward compatibility
- Forward compatibility during migrations
- Independent deployment of producers and consumers
- Zero downtime upgrades
- Easier schema evolution
- Cleaner consumer logic

---

## Best Practices

- Never remove support for an active event version immediately.
- Prefer headers instead of embedding version information inside the payload.
- Introduce new DTOs instead of modifying existing ones.
- Deprecate old versions gradually.
- Document every event version.

---

## Example Flow

```
Producer V1
      │
      │ eventVersion=v1
      ▼
Kafka Topic
      ▼
Consumer
      ▼
Order DTO
```

```
Producer V2
      │
      │ eventVersion=v2
      ▼
Kafka Topic
      ▼
Consumer
      ▼
OrderV2 DTO
```

Both versions are processed by the same consumer without changing the topic.

---

## Advantages over Multiple Topics

Instead of creating:

```
orders-v1

orders-v2

orders-v3
```

a single topic can be maintained:

```
orders
```

while the event version is carried in the message header.

This greatly simplifies infrastructure and reduces operational complexity.

---

## Technologies

- Java 21
- Spring Boot 3.5.x
- Spring for Apache Kafka
- Apache Kafka
- Kafka Headers
- Jackson ObjectMapper
- Docker
- Maven

---

## Version

**Project Version**

```
v1.7.3
```

**Topic**

```
Event Versioning
```

---

## Next Step

The next laboratory introduces **Event Type Routing**, allowing the consumer to execute different business logic based on the `eventType` header.

```
v1.7.4 – Event Type Routing
```