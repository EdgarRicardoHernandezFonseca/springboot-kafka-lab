# Spring Boot Kafka Lab

# Version v1.6.2 – Retry con DefaultErrorHandler

## Objective

This version demonstrates how to configure automatic retries in Spring Kafka using `DefaultErrorHandler`.

When a consumer throws an exception during message processing, Spring Kafka automatically retries the operation before considering the message as failed.

This mechanism is commonly used in production systems to recover from temporary failures such as:

- Database unavailable
- REST API timeout
- Network connectivity issues
- Temporary infrastructure failures

---

# What is DefaultErrorHandler?

`DefaultErrorHandler` is the standard error handler provided by Spring Kafka.

It intercepts exceptions thrown by a `@KafkaListener` and determines how the application should react.

Possible actions include:

- Retry the message
- Wait between retries
- Skip the message
- Send the message to a Dead Letter Topic (DLT)

---

# Retry Flow

Producer

↓

Kafka Topic

↓

Consumer

↓

Business Processing

↓

Exception?

├── No
│
└── Commit Offset
│
└── Yes
        ↓
DefaultErrorHandler
        ↓
Retry #1
        ↓
Retry #2
        ↓
Retry #3
        ↓
If retries are exhausted
        ↓
Message remains failed

---

# FixedBackOff

Retries are configured using `FixedBackOff`.

Example:

```java
new FixedBackOff(2000L, 3);