# v1.9.0 – Introduction to Apache Avro & Schema Registry

## Overview

Until this point, every Kafka message in this project has been exchanged using JSON.

Although JSON is simple and human-readable, large enterprise systems usually require a more efficient and controlled serialization format.

The industry standard solution for Apache Kafka is:

- Apache Avro
- Confluent Schema Registry

This version introduces the concepts behind both technologies before implementing them in later versions.

---

# Why JSON is not enough

JSON is excellent for learning and debugging.

Example:

```json
{
  "orderId":1001,
  "customer":"John",
  "amount":250.75
}
```

However, JSON has several limitations in distributed systems.

## Problems

### No schema validation

Any producer can send:

```json
{
   "customer":"John"
}
```

or

```json
{
   "amount":"ABC"
}
```

The consumer discovers the problem only at runtime.

---

### Large payloads

JSON repeats field names.

Example

```
orderId
customer
amount
```

Those strings travel inside every message.

Millions of messages mean unnecessary network traffic.

---

### Poor versioning

What happens if tomorrow we add:

```
email
```

or

```
currency
```

Old consumers may fail.

Managing compatibility becomes difficult.

---

# What is Apache Avro?

Apache Avro is a binary serialization format.

Instead of sending field names every time, Avro serializes the data according to a predefined schema.

Advantages:

- Smaller messages
- Faster serialization
- Faster deserialization
- Strong typing
- Schema validation
- Version compatibility

---

# JSON vs Avro

| JSON | Avro |
|-------|-------|
| Text | Binary |
| Human readable | Compact |
| Larger payload | Smaller payload |
| No schema enforcement | Strong schema |
| Slower | Faster |
| Flexible but risky | Strict and reliable |

---

# What is a Schema?

A schema defines the structure of an event.

Example:

```json
{
  "type":"record",
  "name":"OrderCreated",
  "fields":[
      {
         "name":"orderId",
         "type":"long"
      },
      {
         "name":"customer",
         "type":"string"
      },
      {
         "name":"amount",
         "type":"double"
      }
  ]
}
```

This schema becomes the contract between producers and consumers.

---

# What is Schema Registry?

Schema Registry is a centralized service that stores every event schema used by Kafka applications.

Instead of embedding schemas inside every application, all producers and consumers retrieve them from a central repository.

```
Producer
    │
    │
    ▼
Schema Registry
    │
    ▼
Kafka Topic
    ▲
    │
Consumer
```

Benefits:

- Central schema management
- Automatic compatibility validation
- Safe evolution of events
- Reduced deployment risks

---

# Schema Evolution

One of the biggest enterprise advantages.

Example V1

```
orderId

customer

amount
```

Later...

Version 2

```
orderId

customer

amount

email
```

Old consumers can continue processing events if compatibility rules are respected.

This is known as Schema Evolution.

---

# Compatibility Modes

Schema Registry supports multiple compatibility strategies.

## Backward

New consumers can read old messages.

Most common option.

---

## Forward

Old consumers can read new messages.

---

## Full

Both directions are compatible.

---

## None

No compatibility validation.

Usually used only during development.

---

# Why Enterprise Systems Prefer Avro

Enterprise architectures typically process:

- millions of events
- dozens of microservices
- hundreds of event types

Requirements include:

- consistency
- version control
- performance
- interoperability

Avro addresses these challenges through compact serialization and schema enforcement.

---

# Production Flow

```
REST API

      │

      ▼

Order Producer

      │

      ▼

Serialize with Avro

      │

      ▼

Validate Schema

      │

      ▼

Schema Registry

      │

      ▼

Kafka Topic

      │

      ▼

Consumer

      │

      ▼

Deserialize using Schema

      │

      ▼

Business Services
```

---

# Benefits

✔ Smaller Kafka messages

✔ Better network utilization

✔ Faster serialization

✔ Strong contracts

✔ Schema validation

✔ Safe versioning

✔ Enterprise-ready messaging

✔ Language-independent communication

✔ Better interoperability between microservices

---

# Current Version Status

This version is conceptual only.

No code changes have been introduced yet.

The objective is to understand:

- Why Avro exists
- Why Schema Registry is needed
- How enterprise Kafka systems manage schemas
- Why binary serialization is preferred over JSON

Future versions will implement:

- Apache Avro serialization
- Avro Producer
- Avro Consumer
- Schema Registry integration
- Schema evolution examples
- Backward compatibility demonstrations

---

# Key Takeaways

- JSON is simple but inefficient for enterprise messaging.
- Apache Avro provides compact binary serialization.
- Schemas define the contract between producers and consumers.
- Schema Registry centralizes schema management.
- Schema evolution enables safe event versioning.
- Avro is the de facto serialization standard for production Kafka systems.
---

# Author

**Edgar Ricardo Hernández Fonseca**

Backend Developer

Java | Spring Boot | REST APIs | AWS

LinkedIn: *https://linkedin.com/in/edgar-ricardo-hernandez-fonseca*

GitHub: *https://github.com/EdgarRicardoHernandezFonseca*

---

# License

This project is available for educational and demonstration purposes.