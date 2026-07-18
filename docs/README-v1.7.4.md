# v1.7.4 – Event Type Routing

## Overview

In previous versions of this lab we learned how to enrich Kafka events with metadata such as:

- Custom Headers
- Correlation IDs
- Event Versioning

In this version we introduce another common Event-Driven Architecture (EDA) pattern:

> **Event Type Routing**

Instead of creating multiple Kafka topics for every business action, producers publish different event types into the same topic while consumers decide how to process each message based on an **eventType** header.

---

# Why Event Type Routing?

Imagine an Order Service.

An order can generate multiple business events:

- Order Created
- Order Updated
- Order Cancelled
- Order Paid
- Order Shipped
- Order Delivered

A naive implementation would create one topic for each event:

```

orders-created
orders-updated
orders-cancelled
orders-paid
orders-shipped
orders-delivered

```

Although this works, it quickly becomes difficult to maintain.

Instead, many enterprise systems publish every order-related event into a single topic:

```

orders

```

and attach an Event Type header:

```

eventType = ORDER_CREATED

```

or

```

eventType = ORDER_UPDATED

```

or

```

eventType = ORDER_CANCELLED

```

The consumer then routes the message to the appropriate business logic.

---

# Architecture

```

REST API
│
▼
Order Producer
│
│ eventType = ORDER_CREATED
│
▼
Kafka Topic
orders
│
▼
Order Consumer
│
├── processCreate()
├── processUpdate()
└── processCancel()

```

A single topic transports multiple business events while consumers determine the processing flow.

---

# Event Header Example

Headers

```

eventType : ORDER_CREATED
eventVersion : v2
correlationId : 610a6e0c-d17a-4d2f-be7d-3f652999b945
source : springboot-kafka-lab

```

Payload

```json
{
  "orderId": 1001,
  "customerName": "Edgar R Hernandez",
  "priority": "High"
}
```

Notice that the payload does not indicate the business action.

The business meaning comes from the **eventType** header.

---

# Routing Logic

The consumer reads the header and dispatches the event.

Example:

```java
switch (eventType) {

    case "ORDER_CREATED":
        processCreate(order);
        break;

    case "ORDER_UPDATED":
        processUpdate(order);
        break;

    case "ORDER_CANCELLED":
        processCancel(order);
        break;

    default:
        log.warn("Unknown event type");
}
```

Each business action is isolated inside its own method.

This makes the consumer easier to maintain as new event types are added.

---

# Event Version vs Event Type

These two concepts are frequently confused but represent different concerns.

| Event Version | Event Type |
|---------------|------------|
| Defines the payload schema | Defines the business action |
| Handles backward compatibility | Routes business logic |
| Evolves over time | Changes per business operation |
| Example: v1, v2 | Example: ORDER_CREATED |

Example:

```

eventVersion = v2
eventType = ORDER_UPDATED

```

means

> "This is an Order Updated event using version 2 of the payload."

Both headers complement each other.

---

# Business Methods

The consumer separates each business action into dedicated methods.

```

processCreate()

```

Responsible for:

- Persisting the new order
- Reserving inventory
- Sending confirmation notifications

```

processUpdate()

```

Responsible for:

- Updating the order
- Adjusting inventory
- Publishing update notifications

```

processCancel()

```

Responsible for:

- Cancelling the order
- Releasing inventory
- Publishing refund events

Although this lab only logs each operation, in a real microservice these methods would call repositories, external services, or publish additional Kafka events.

---

# Benefits

Using Event Type Routing provides several advantages:

- Fewer Kafka topics
- Easier maintenance
- Cleaner producer implementation
- Flexible consumer logic
- Easy addition of new event types
- Better organization of domain events
- Widely used in enterprise Event-Driven Architectures

---

# REST Endpoints

This version exposes three endpoints.

Create Order

```

POST /orders/v2/create

```

Update Order

```

POST /orders/v2/update

```

Cancel Order

```

POST /orders/v2/cancel

```

All endpoints publish to the same Kafka topic:

```

orders

```

The only difference is the **eventType** header.

---

# Sample Execution

Request

```http
POST /orders/v2/create
```

Headers produced

```

eventType = ORDER_CREATED
eventVersion = v2
source = springboot-kafka-lab
correlationId = xxxxxxxx

```

Consumer Output

```

========== CREATE ==========

Creating new order...

Creating order 1001 in database...

Reserving inventory...

Sending confirmation email...

```

The same consumer can later receive:

```

ORDER_UPDATED

```

or

```

ORDER_CANCELLED

```

without requiring additional Kafka topics.

---

# Summary

In this version we implemented the **Event Type Routing** pattern.

Instead of creating multiple Kafka topics for each business action, all Order events are published to a single topic and differentiated using the **eventType** header.

Together with **eventVersion**, this approach allows Kafka consumers to support multiple business operations while maintaining payload compatibility and keeping the architecture simple, scalable, and closer to real-world enterprise event-driven systems.