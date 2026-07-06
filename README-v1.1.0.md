# README-v1.1.0.md

# Kafka Producer

## Overview

A **Kafka Producer** is an application responsible for publishing messages to one or more Kafka topics.

Producers are the entry point of data into an Apache Kafka cluster. They send events, commands, or data records that can later be processed by one or more consumers.

In this laboratory, the Spring Boot application acts as a Kafka Producer.

---

# What is a Producer?

A Producer is a Kafka client that writes messages to a topic.

Unlike traditional messaging systems where applications communicate directly with each other, a producer sends messages only to Kafka.

Kafka becomes responsible for storing and distributing those messages.

```
Spring Boot
     │
     │ send()
     ▼
+-------------+
|   Kafka     |
|   Topic      |
+-------------+
```

---

# How Does a Producer Work?

The message publishing process follows these steps:

1. The application creates a message.
2. The Producer connects to Kafka.
3. Kafka receives the message.
4. Kafka stores the message inside a topic.
5. Consumers can later read the message.

```
Application
      │
      ▼
KafkaProducer
      │
      ▼
Kafka Broker
      │
      ▼
Topic (orders)
```

---

# Producer in This Laboratory

The laboratory contains a REST endpoint:

```
POST /orders
```

When the endpoint is called:

1. Spring Boot receives the HTTP request.
2. The Producer creates a Kafka message.
3. The message is published to the **orders** topic.
4. Kafka stores the message.

Example request:

```http
POST /orders

First Kafka Message
```

---

# Spring Kafka Components

The Producer implementation uses two main Spring components.

## KafkaTemplate

`KafkaTemplate` is the primary class used to publish messages.

Example:

```java
kafkaTemplate.send("orders", message);
```

It abstracts the low-level Kafka Producer API and simplifies message publishing.

---

## Kafka Configuration

The Producer connects to Kafka using the bootstrap server configured in:

```properties
spring.kafka.bootstrap-servers=localhost:9092
```

This tells Spring Boot where the Kafka broker is located.

---

# Message Flow

```
Postman
    │
    ▼
POST /orders
    │
    ▼
OrderController
    │
    ▼
KafkaProducerService
    │
    ▼
KafkaTemplate
    │
    ▼
Kafka Broker
    │
    ▼
orders Topic
```

---

# Why Use a Producer?

Using Kafka Producers provides several advantages.

- Asynchronous communication
- Loose coupling between applications
- High throughput
- Scalability
- Reliability
- Event-driven architecture

The Producer does not need to know who will consume the message.

It only sends data to Kafka.

---

# Advantages

- Simple message publishing
- High performance
- Built-in retry mechanisms
- Supports synchronous and asynchronous sending
- Supports serialization
- Integrates seamlessly with Spring Boot

---

# Laboratory Implementation

Current implementation:

- Spring Boot REST API
- Apache Kafka 4.2.1
- Spring for Apache Kafka
- Topic:

```
orders
```

Producer implementation:

```
OrderController
        │
        ▼
KafkaProducerService
        │
        ▼
KafkaTemplate
        │
        ▼
Kafka Broker
```

---

# Expected Result

After sending a request to:

```http
POST /orders
```

The message is successfully published into Kafka.

The Consumer (implemented in version v1.2.0) can then read and process the message.

---

# Version

Laboratory Version

```
v1.1.0
```

Main Topic

```
Kafka Producer
```

---

# References

Apache Kafka Documentation

https://kafka.apache.org/documentation/

Spring for Apache Kafka

https://docs.spring.io/spring-kafka/reference/
