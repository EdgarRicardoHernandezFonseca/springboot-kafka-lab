# v1.7.1 – Reading Kafka Message Headers

## Objective

After learning how to send custom Kafka headers, the next step is understanding how consumers can read and use them.

In Event-Driven Architectures (EDA), consumers often make decisions based not only on the message payload but also on metadata contained in the message headers.

This version demonstrates how to read custom Kafka headers using Spring Boot and Spring Kafka.

---

# Why Read Message Headers?

A Kafka message contains both:

- Business Data (Payload)
- Metadata (Headers)

The payload answers:

> **What happened?**

The headers answer:

> **How should this event be processed?**

Typical metadata includes:

- Event type
- Event version
- Source application
- Correlation ID
- Trace ID
- Tenant
- Retry count

Without reading headers, a consumer only has access to the business object.

---

# Kafka Message Structure

```
+----------------------------+
| Key                        |
+----------------------------+
| Value (Payload)            |
+----------------------------+
| Headers                    |
+----------------------------+
```

Example:

Headers

```
eventType = ORDER_CREATED
eventVersion = v1
source = springboot-kafka-lab
```

Payload

```json
{
    "orderId":1001,
    "customerName":"Edgar Hernandez",
    "totalAmount":150000
}
```

Both parts travel together inside the Kafka record.

---

# Reading Headers with Spring Kafka

Spring Kafka allows headers to be injected directly into the listener method using the `@Header` annotation.

Example:

```java
@KafkaListener(topics = "orders")
public void consume(

        Order order,

        @Header("eventType") String eventType,
        @Header("eventVersion") String eventVersion,
        @Header("source") String source

) {

    log.info("Event Type : {}", eventType);
    log.info("Version    : {}", eventVersion);
    log.info("Source     : {}", source);

}
```

Spring automatically extracts each header and converts it to the desired Java type.

---

# Consumer Output

Example log:

```
--------------------------------
Message Headers
--------------------------------

Event Type : ORDER_CREATED
Version    : v1
Source     : springboot-kafka-lab
```

The payload is still available:

```
Order ID : 1001
Customer : Edgar Hernandez
```

This demonstrates that both metadata and business data can be processed together.

---

# Why Not Store This Information in the Payload?

A common question is:

> Why not include eventType and version inside the Order object?

Example:

```json
{
    "eventType":"ORDER_CREATED",
    "eventVersion":"v1",
    "orderId":1001
}
```

Although possible, this mixes business data with transport metadata.

Headers provide a cleaner separation of concerns.

Business object:

```json
{
    "orderId":1001,
    "customerName":"Edgar Hernandez"
}
```

Transport metadata:

```
eventType
eventVersion
source
```

This keeps the domain model independent from the messaging infrastructure.

---

# Common Header Use Cases

Consumers frequently read headers to:

- Route events
- Validate message versions
- Enable distributed tracing
- Correlate requests
- Identify the source service
- Handle retries
- Apply security policies

Headers make consumers more flexible without changing the payload structure.

---

# Real-world Example

Imagine an e-commerce platform.

A producer publishes:

Headers

```
eventType = ORDER_CREATED
eventVersion = v2
source = order-service
```

Payload

```json
{
    "orderId":1001,
    "customerName":"John Doe"
}
```

A consumer can immediately identify:

- Which service produced the event
- Which payload version is being used
- Which business operation occurred

without modifying the Order object.

---

# Benefits

Reading Kafka headers provides several advantages:

- Keeps business objects clean.
- Separates metadata from business data.
- Simplifies event processing.
- Improves interoperability.
- Enables flexible consumer behavior.
- Supports future event evolution.
- Facilitates distributed system observability.

---

# Version Summary

Implemented features:

- Reading custom Kafka headers.
- Using the `@Header` annotation.
- Logging event metadata.
- Accessing payload and headers simultaneously.
- Verifying header values during message consumption.

Current project evolution:

- Producer
- Consumer
- Consumer Groups
- Manual Acknowledgement
- Retry Mechanism
- Dead Letter Topic (DLT)
- Non-Retryable Exceptions
- Sending Custom Headers
- Reading Custom Headers (current version)

Next step:

Implement Correlation IDs to uniquely identify and trace events across multiple microservices.

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