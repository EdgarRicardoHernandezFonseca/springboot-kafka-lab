# v1.7.2 – Correlation ID

## Objective

Implement **Correlation ID propagation** across Kafka producers and consumers to enable end-to-end request tracing in an event-driven architecture.

---

# What is a Correlation ID?

A **Correlation ID** is a unique identifier attached to an event that allows all related messages to be linked together.

Instead of identifying an individual Kafka message, it identifies the entire business transaction.

For example:

Customer creates an order.

↓

OrderCreated event

↓

Inventory service

↓

Notification service

↓

Email service

↓

Audit service

Every service receives the same Correlation ID.

---

## Why is it important?

Without a Correlation ID:

```
Order Created
Notification Sent
Email Sent
Audit Saved
```

There is no way to know whether these operations belong to the same request.

With Correlation ID:

```
Correlation ID:
96e9b210-bfc1-42f8-ab18-c405b27316c0

Order Created

Notification Sent

Email Sent

Audit Saved
```

Now the complete execution can be reconstructed.

---

# Event Flow

```
REST API

↓

OrderProducer

↓

orders Topic

├── OrderConsumer

├── AuditConsumer

└── NotificationConsumer

↓

NotificationProducer

↓

notifications Topic

↓

EmailConsumer
```

The Correlation ID remains unchanged during the entire process.

---

# Kafka Headers

Every produced message includes custom headers:

| Header | Description |
|---------|-------------|
| correlationId | Unique business transaction identifier |
| eventType | Event name |
| eventVersion | Event schema version |
| source | Event producer |

Example:

```json
{
  "eventVersion": "v1",
  "correlationId": "96e9b210-bfc1-42f8-ab18-c405b27316c0",
  "eventType": "ORDER_CREATED",
  "source": "springboot-kafka-lab"
}
```

---

# Benefits

- Distributed tracing
- Easier debugging
- End-to-end monitoring
- Log correlation
- Production support
- Observability

---

# Real-world Usage

Correlation IDs are commonly used in:

- Apache Kafka
- RabbitMQ
- ActiveMQ
- AWS SNS/SQS
- Azure Service Bus
- Google Pub/Sub

They are also fundamental in distributed tracing tools such as:

- Zipkin
- Jaeger
- OpenTelemetry
- Grafana Tempo

---

# Example Console Output

```
OrderProducer
Correlation ID:
96e9b210-bfc1-42f8-ab18-c405b27316c0

↓

OrderConsumer
Correlation ID:
96e9b210-bfc1-42f8-ab18-c405b27316c0

↓

NotificationConsumer
Correlation ID:
96e9b210-bfc1-42f8-ab18-c405b27316c0

↓

EmailConsumer
Correlation ID:
96e9b210-bfc1-42f8-ab18-c405b27316c0
```

The same Correlation ID is preserved throughout the complete event pipeline.

---

# Version

**v1.7.2**

Implemented Correlation ID propagation using Kafka message headers across producers and consumers.

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