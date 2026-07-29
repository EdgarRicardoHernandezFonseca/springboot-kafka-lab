# README-v1.9.1.md

# v1.9.1 – Complete Avro + Schema Registry Integration

This version completes the migration from plain JSON messages to **Apache Avro** with **Confluent Schema Registry**, creating a much more realistic event-driven architecture similar to those used in enterprise systems.

Unlike previous versions, producers and consumers now exchange strongly typed Avro objects while schemas are managed centrally by the Schema Registry.

---

# Objectives

This version demonstrates:

- Apache Avro serialization
- Schema Registry integration
- Automatic schema registration
- Avro Producers
- Avro Consumers
- Kafka Metadata Headers
- Correlation ID propagation
- Event Versioning
- Event Source tracking
- Notification Events
- Email Events
- Manual Offset Acknowledgement
- Business Services
- End-to-End Event Processing

---

# Architecture

```
REST API

        │
        ▼

OrderProducerV2
        │
        │
        ▼

Kafka Topic (orders)

        │
        ▼

Schema Registry

        │

        ▼

OrderConsumer

        │

        ├──────────────► OrderService

        ├──────────────► InventoryService

        ├──────────────► NotificationService

        │                      │
        │                      ▼
        │               NotificationProducer
        │                      │
        │                      ▼
        │              notifications Topic
        │                      │
        │                      ▼
        │             NotificationConsumer
        │
        ├──────────────► AuditService
        │
        ▼

Manual Acknowledgement
```

---

# Apache Avro

Apache Avro is a binary serialization format designed for high-performance event streaming.

Compared to JSON:

| JSON | Avro |
|-------|------|
| Text | Binary |
| Larger payload | Smaller payload |
| No schema enforcement | Strong schema |
| Slower serialization | Faster serialization |
| Runtime validation | Compile-time generated classes |

Instead of sending JSON objects, Kafka exchanges generated Java classes.

Example:

```
OrderCreated.avsc
```

↓

Generated automatically

```
OrderCreated.java
```

↓

Producer publishes

```
OrderCreated
```

↓

Consumer receives

```
OrderCreated
```

without any manual parsing.

---

# Schema Registry

Schema Registry stores every Avro schema used by Kafka.

Instead of embedding the entire schema inside every message, Kafka only sends:

- Schema ID
- Binary payload

The consumer retrieves the schema automatically.

Benefits:

- Schema evolution
- Backward compatibility
- Forward compatibility
- Version control
- Contract enforcement

---

# Registered Schemas

Current project registers:

## orders-value

```
OrderCreated
```

Contains:

- orderId
- customerName
- priority
- product
- quantity
- price
- createdAt

---

## notifications-value

```
Notification
```

Contains:

- orderId
- message

---

# Avro Serializers

Producers now use:

```
KafkaAvroSerializer
```

instead of

```
JsonSerializer
```

Responsibilities:

- Convert Java object to Avro binary
- Register schema automatically
- Store Schema ID
- Publish optimized payload

Producer configuration:

```
KafkaAvroSerializer

Schema Registry URL

Auto Register Schemas
```

---

# Avro Deserializers

Consumers now use:

```
KafkaAvroDeserializer
```

Responsibilities:

- Read Schema ID
- Retrieve schema
- Deserialize binary payload
- Create generated Java object

No manual conversion is required.

---

# New Producers

This version introduces multiple producers.

## OrderProducerV2

Responsible for publishing:

```
OrderCreated
```

Main responsibilities:

- Build Avro object
- Add metadata headers
- Generate Correlation ID
- Publish event

---

## NotificationProducer

Publishes notification events after order processing.

Produces:

```
Notification
```

This simulates downstream microservices.

---

# New Consumers

Several independent consumers subscribe to Kafka topics.

## OrderConsumer

Consumes:

```
orders
```

Responsibilities:

- Read metadata
- Validate headers
- Execute business logic
- Invoke services
- Commit offset manually

---

## NotificationConsumer

Consumes:

```
notifications
```

Responsible for processing notification events.

---

## EmailConsumer

Simulates an email microservice.

Receives notification events and processes:

```
Send Email
```

---

## AuditConsumer

Consumes order events for auditing purposes.

Stores or logs:

- Event type
- Version
- Correlation ID
- Source

without affecting business processing.

---

# Metadata Headers

Instead of relying only on payload data, producers include Kafka headers.

Headers provide metadata required by enterprise systems.

Current headers include:

```
eventType

eventVersion

source

correlationId
```

Example:

```
ORDER_CREATED

v2

springboot-kafka-lab

cfc1d4f6-410d-4d12-97ac-71fa65df7159
```

---

# Correlation ID

Each request generates a unique identifier.

Example:

```
cfc1d4f6-410d-4d12-97ac-71fa65df7159
```

Every downstream service receives the same value.

Benefits:

- Distributed tracing
- Debugging
- Log correlation
- Observability

---

# Event Version

Current implementation includes:

```
v2
```

inside metadata.

Purpose:

- Schema evolution
- Backward compatibility
- Multiple consumer versions

Older consumers may continue processing older event versions.

---

# Source

Every produced event identifies its origin.

Current value:

```
springboot-kafka-lab
```

Useful when multiple applications publish into the same Kafka cluster.

---

# Notification Events

After an order is processed:

```
OrderConsumer
```

publishes

```
Notification
```

This demonstrates event chaining.

Flow:

```
Order Created

↓

Order Consumer

↓

Notification Producer

↓

notifications Topic

↓

Notification Consumer
```

---

# Email Events

NotificationConsumer simulates another microservice by triggering:

```
EmailConsumer
```

Example workflow:

```
Order Created

↓

Notification

↓

Email Event

↓

Confirmation Email
```

This models asynchronous communication between independent services.

---

# Manual Acknowledgement

Consumers no longer commit offsets automatically.

Instead:

```
Acknowledgment.acknowledge();
```

is executed only after successful processing.

Benefits:

- Prevent message loss
- Better retry control
- Enterprise-grade processing
- At-least-once delivery

Log example:

```
Business completed.

Acknowledging Offset...

Offset committed manually.
```

---

# Business Services

To simulate a production architecture, business logic has been separated into dedicated services.

## OrderService

Responsible for:

- Creating orders

---

## InventoryService

Responsible for:

- Reserving inventory

---

## NotificationService

Responsible for:

- Sending notifications

---

## AuditService

Responsible for:

- Recording audit information

Each service executes independently while the consumer orchestrates the workflow.

---

# Full Integration Flow

```
POST /orders/v2/create

↓

OrderProducerV2

↓

Kafka

↓

Schema Registry

↓

OrderConsumer

↓

OrderService

↓

InventoryService

↓

NotificationService

↓

NotificationProducer

↓

notifications Topic

↓

NotificationConsumer

↓

EmailConsumer

↓

AuditConsumer

↓

Manual Offset Commit
```

---

# Technologies Used

- Java 21
- Spring Boot
- Spring Kafka
- Apache Kafka
- Apache Avro
- Confluent Schema Registry
- Kafka UI
- Maven
- Docker
- Docker Compose

---

# Learning Outcomes

After completing this version you will understand:

- How Apache Avro works
- Why Schema Registry is required
- Producer serialization
- Consumer deserialization
- Generated Avro classes
- Metadata headers
- Event versioning
- Correlation IDs
- Distributed event tracing
- Multiple Kafka consumers
- Multiple Kafka producers
- Event-driven architecture
- Notification pipelines
- Email event processing
- Manual offset acknowledgement
- Business service orchestration
- Enterprise Kafka integration

---

# Version Summary

**v1.9.1** transforms the project from a basic Kafka producer/consumer example into a complete enterprise-style event-driven architecture.

Major improvements include:

- Complete Avro serialization/deserialization
- Full Schema Registry integration
- Multiple producers
- Multiple consumers
- Metadata headers
- Correlation ID propagation
- Event versioning
- Notification event pipeline
- Email event simulation
- Manual offset acknowledgement
- Layered business services
- End-to-end asynchronous processing

This version closely resembles how modern Java microservices communicate using Apache Kafka in production environments.