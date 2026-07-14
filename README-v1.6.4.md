# README v1.6.4 – Non-Retryable Exceptions

## Objective

This version introduces the concept of **Non-Retryable Exceptions** in Spring Kafka.

In many real-world applications, not every error should trigger message retries.

Some failures are temporary (network problems, external services unavailable, database timeout), while others are permanent (invalid input data, business validation errors).

Spring Kafka allows developers to distinguish between these scenarios.

---

# What are Non-Retryable Exceptions?

A Non-Retryable Exception represents an error that **cannot be solved by retrying the same message**.

Typical examples include:

- Missing mandatory fields
- Invalid business rules
- Malformed payloads
- Invalid customer information
- Validation failures

Retrying these messages only wastes processing time and system resources.

Instead, the message should be sent directly to a Dead Letter Topic (DLT).

---

# Retryable vs Non-Retryable

## Retryable Exceptions

Examples:

- Database temporarily unavailable
- Network timeout
- REST API unavailable
- Messaging system temporarily down

Processing flow:

Producer
↓

Kafka Topic

↓

Consumer

↓

Retry 1

↓

Retry 2

↓

Retry 3

↓

Dead Letter Topic (DLT)

---

## Non-Retryable Exceptions

Examples:

- Invalid customer name
- Invalid order data
- Business validation failure

Processing flow:

Producer

↓

Kafka Topic

↓

Consumer

↓

Dead Letter Topic (DLT)

(No retries)

---

# Spring Kafka Configuration

Spring Kafka provides the `DefaultErrorHandler`.

Retryable exceptions use the configured `FixedBackOff`.

Specific exceptions can be marked as non-retryable using:

```java
errorHandler.addNotRetryableExceptions(
        IllegalArgumentException.class
);