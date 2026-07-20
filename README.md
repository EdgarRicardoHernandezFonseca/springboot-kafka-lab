# Spring Boot Kafka Lab

A hands-on learning project demonstrating how to build event-driven applications using **Apache Kafka** and **Spring Boot**.

This repository is organized as a step-by-step laboratory where each version introduces a new Kafka concept, allowing developers to understand how messaging systems work in real-world enterprise applications.

---

# Project Objectives

The main goals of this project are:

- Learn Apache Kafka fundamentals.
- Build producers and consumers using Spring Boot.
- Understand Kafka topics and partitions.
- Explore Consumer Groups.
- Implement manual acknowledgements.
- Configure retry mechanisms.
- Handle Dead Letter Topics (DLT).
- Work with retryable and non-retryable exceptions.
- Send and process Kafka Message Headers.
- Apply messaging best practices used in microservices.

---

# Technologies

- Java 21
- Spring Boot 3.x
- Spring for Apache Kafka
- Apache Kafka
- Kafka UI
- Docker
- Maven

---

# Project Structure

```
src
 ├── config
 ├── consumer
 ├── controller
 ├── dto
 ├── producer
 └── service
```

---

# Features Implemented

| Version | Feature | Status |
|----------|---------|--------|
| v1.0.0 | Hello Kafka – Basic Infrastructure | ✅ |
| v1.1.0 | Kafka Producer | ✅ |
| v1.2.0 | Kafka Consumer | ✅ |
| v1.2.1 | Kafka Dual Listener Configuration | ✅ |
| v1.3.0 | JSON Serialization | ✅ |
| v1.4.0 | Message Keys and Partitions | ✅ |
| v1.5.0 | Consumer Groups & Parallel Processing | ✅ |
| v1.5.1 | Consumer Group Rebalancing | ✅ |
| v1.5.2 | Horizontal Scaling with Multiple Spring Boot Instances | ✅ |
| v1.6.0 | Offset Management Fundamentals | ✅ |
| v1.6.1 | Manual Commit and AckMode.MANUAL | ✅ |
| v1.6.2 | Retry con DefaultErrorHandler | ✅ |
| v1.6.3 | Dead Letter Topic (DLT) | ✅ |
| v1.6.4 | Non-Retryable Exceptions | ✅ |
| v1.7.0 | Kafka Message Headers (Sending) | ✅ |
| v1.7.1 | Kafka Message Headers (Reading) | ✅ |
| v1.7.2 | Correlation ID | ✅ |
| v1.7.3 | Event Versioning | ✅ |
| v1.7.4 | Event Type Routing | ✅ |
| v1.7.5 | Production Example | ✅ |
| v1.8.0 | Multiple Consumers and Concurrent Processing | ✅ |
| v1.8.1 | Multiple Consumers Implementation & Load Distribution | ✅ |
| v1.8.2 |  | 🚧 |
| v1.9.0 | Avro y Schema Registry | ⏳ |
| v1.9.0 | Database integration using the Outbox pattern to ensure | ⏳ |

---

# Architecture

```
REST API
    │
    ▼
Order Producer
    │
    ▼
Kafka Topic (orders)
    │
    ▼
Notification Consumer
    │
    ├── Success
    │       │
    │       ▼
    │   Manual Ack
    │
    └── Failure
            │
            ▼
      Retry Mechanism
            │
            ▼
      Dead Letter Topic
```

---

# Running the Project

## Clone the repository

```bash
git clone https://github.com/EdgarRicardoHernandezFonseca/springboot-kafka-lab.git
```

---

## Start Kafka

```bash
docker compose up -d
```

---

## Run Spring Boot

```bash
mvn spring-boot:run
```

or

Run the project directly from your IDE.

---

# Sending a Test Message

Example request:

```
POST /orders
```

```json
{
    "orderId":1001,
    "customerName":"Edgar Hernandez",
    "totalAmount":150000
}
```

---

# Kafka UI

Kafka UI is available at:

```
http://localhost:8081
```

From Kafka UI you can:

- Browse Topics
- Inspect Messages
- View Headers
- Inspect Partitions
- Monitor Consumer Groups

---

# Learning Roadmap

This repository follows a progressive learning path.

Current topics include:

- Kafka Producer
- Kafka Consumer
- Consumer Groups
- Partitions
- Manual Acknowledgement
- Retry Mechanism
- Dead Letter Topics
- Non-Retryable Exceptions
- Kafka Message Headers

Upcoming topics:

- Reading Custom Headers
- Message Filtering
- Record Interceptors
- JSON Schema
- Avro Serialization
- Transactions
- Exactly Once Semantics
- Batch Processing
- Request-Reply Pattern
- Kafka Streams

---

# Documentation

Each implemented version contains its own documentation.

Examples:

```
docs/
    README-v1.0.0.md
    README-v1.1.0.md
    README-v1.2.0.md
    README-v1.2.1.md
    README-v1.3.0.md
    README-v1.4.0.md
    README-v1.5.0.md
    README-v1.5.1.md
    README-v1.5.2.md
    README-v1.6.0.md
    README-v1.6.1.md
    README-v1.6.2.md
    README-v1.6.3.md
    README-v1.6.4.md
    README-v1.7.0.md
    README-v1.7.1.md
    README-v1.7.2.md
    README-v1.7.3.md
    README-v1.7.4.md
    README-v1.7.5.md
    README-v1.8.0.md
    README-v1.8.1.md
```

Each document explains:

- Objective
- Theory
- Spring Kafka implementation
- Practical examples
- Results
- Best practices

---

# What You Will Learn

By completing this laboratory, you will gain practical experience with:

- Apache Kafka
- Event-Driven Architecture
- Spring Kafka
- Enterprise Messaging
- Error Handling
- Distributed Systems
- Microservices Communication
- Event Metadata
- Kafka Best Practices

---

# Future Improvements

Some planned enhancements include:

- Docker Compose improvements
- Integration Tests
- Testcontainers
- Avro
- Schema Registry
- Kafka Streams
- Observability
- Micrometer Metrics
- OpenTelemetry
- Kubernetes Deployment

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

---

# Progress

| Progress                 | Status   |
| ------------------------ | -------- |
| Producer                 | ✅       |
| Consumer                 | ✅       |
| Consumer Groups          | ✅       |
| Manual Ack               | ✅       |
| Retry                    | ✅       |
| Dead Letter Topic        | ✅       |
| Non-Retryable Exceptions | ✅       |
| Custom Headers           | ✅       |
| Reading Headers          | ✅       |
| Filtering                | 🚧       |
| Avro                     | 🚧       |
| Kafka Streams            | ⏳       |
| Transactions             | ⏳       |
| Exactly Once             | ⏳       |

---