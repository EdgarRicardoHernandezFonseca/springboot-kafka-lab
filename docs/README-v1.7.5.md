# v1.7.5 – Production Example

## Overview

This version transforms the previous Kafka laboratory into a production-style example.

The objective is not only to publish and consume Kafka messages, but also to demonstrate how an enterprise event-driven application is usually organized.

The implementation introduces:

- Production-like event workflow
- Business service orchestration
- Event metadata (Kafka Headers)
- Event Versioning
- Event Routing
- Professional logging
- Clear separation between infrastructure and business logic

The project now resembles the architecture commonly found in real enterprise applications.

---

# Production Flow

The following diagram represents the complete event lifecycle.

```
                REST API
                    │
                    ▼
          OrderProducerV2
                    │
         Build Kafka Message
         + Kafka Headers
                    │
                    ▼
            Kafka Topic (orders)
                    │
                    ▼
            OrderConsumer
                    │
        Read Kafka Headers
                    │
        Validate Metadata
                    │
        Detect Event Version
                    │
        Route Event Type
                    │
                    ▼
          Business Services
       ┌─────────┬────────────┬──────────────┬──────────┐
       │         │            │              │          │
       ▼         ▼            ▼              ▼
 OrderService Inventory Notification Audit
                    │
                    ▼
          Manual Offset Commit
```

---

# Producer Flow

The producer simulates the entry point of an enterprise application.

Its responsibilities are:

- Receive the business request
- Validate the payload
- Generate Correlation ID
- Build Kafka Headers
- Build Kafka Message
- Publish the event

Each message includes metadata that allows downstream systems to identify how the event should be processed.

Example Kafka Headers:

| Header | Description |
|---------|-------------|
| eventType | Business action (CREATE, UPDATE, CANCEL) |
| eventVersion | Event schema version |
| source | Originating application |
| correlationId | Request tracing identifier |

---

# Consumer Flow

The consumer is responsible for orchestrating the entire event processing pipeline.

The workflow is divided into several phases.

## Phase 1

Receive Kafka Event

The consumer receives the raw event from Kafka.

---

## Phase 2

Read Kafka Headers

Metadata is extracted from the event.

Example:

- Event Type
- Event Version
- Source
- Correlation ID

---

## Phase 3

Validate Metadata

The application validates whether the event is supported.

Examples:

- supported version
- supported event type
- valid payload

---

## Phase 4

Route Event

Based on the event type, the consumer delegates processing.

```
ORDER_CREATED
        │
        ▼
processCreate()

ORDER_UPDATED
        │
        ▼
processUpdate()

ORDER_CANCELLED
        │
        ▼
processCancel()
```

---

## Phase 5

Business Processing

Each event executes the corresponding business services.

Example:

```
ORDER_CREATED

↓

OrderService

↓

InventoryService

↓

NotificationService

↓

AuditService
```

Each service is intentionally isolated to simulate a real production architecture.

---

## Phase 6

Commit Offset

After all business operations complete successfully:

```
ack.acknowledge();
```

The Kafka offset is manually committed.

This guarantees **at-least-once delivery**.

---

# Business Services

The project separates business logic into independent services.

## OrderService

Responsible for:

- Create Order
- Update Order
- Cancel Order

---

## InventoryService

Responsible for:

- Reserve Inventory
- Release Inventory

---

## NotificationService

Responsible for:

- Confirmation Email
- Refund Notification

---

## AuditService

Responsible for recording business events for auditing purposes.

Example:

```
ORDER_CREATED
ORDER_UPDATED
ORDER_CANCELLED
```

---

# Production Logging

One of the main improvements introduced in this version is standardized logging.

Instead of printing isolated log messages, the application now produces structured logs similar to those found in enterprise systems.

Example:

```
============================================================

ORDER PROCESSING STARTED

Correlation ID

Event Type

Event Version

Source

Timestamp

============================================================
```

The logs are organized into logical execution phases:

Producer

- Receiving request
- Validating payload
- Generating Correlation ID
- Building Kafka Headers
- Building Kafka Message
- Publishing to Kafka

Consumer

- Receiving Event
- Reading Kafka Headers
- Validating Metadata
- Routing Event
- Deserializing Payload
- Executing Business Logic
- Publishing Downstream Events
- Manual Offset Commit

This approach greatly improves:

- Debugging
- Monitoring
- Observability
- Production support

---

# Event Versioning

The project demonstrates event evolution using versioned payloads.

Supported versions:

- v1
- v2

The consumer automatically detects the version using Kafka Headers.

```
eventVersion = v1

↓

processV1()

----------------------------

eventVersion = v2

↓

processV2()
```

This approach enables backward compatibility without breaking existing consumers.

---

# Event Routing

Business behavior is selected using the eventType header.

Example:

```
ORDER_CREATED

↓

Create Order

Reserve Inventory

Send Confirmation

Audit Event
```

```
ORDER_UPDATED

↓

Update Order

Reserve Inventory

Audit Event
```

```
ORDER_CANCELLED

↓

Cancel Order

Release Inventory

Publish Refund

Audit Event
```

This keeps the consumer open for extension while avoiding large conditional blocks.

---

# Benefits

This version introduces several production-oriented practices:

- Separation of concerns
- Event metadata
- Event versioning
- Event routing
- Business service orchestration
- Professional logging
- Manual acknowledgment
- Enterprise-style code organization

Although simplified for educational purposes, the architecture closely resembles the event-driven workflow used in modern microservice-based systems.