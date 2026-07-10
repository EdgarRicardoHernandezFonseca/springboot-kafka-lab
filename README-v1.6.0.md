# Spring Boot Kafka Lab

# Version 1.6.0

# Offset Management Fundamentals

---

## Objective

Learn how Apache Kafka manages message consumption using offsets and how Spring Boot consumers interact with them.

This version introduces:

- Offset Management
- Consumer Groups
- Partitions
- Auto Commit
- Manual Commit (concept)
- AckMode (concept)
- Delivery Guarantees
- Retry (introduction)
- Dead Letter Topic (introduction)

---

# What is an Offset?

An Offset is the sequential position of a message inside a Kafka partition.

Example:

Partition 0

Offset | Order
------ | -----
0 | Order 1001
1 | Order 1002
2 | Order 1005

Partition 1

Offset | Order
------ | -----
0 | Order 1003
1 | Order 1004

Notice that each partition maintains its own offsets.

Offsets are not shared across partitions.

---

# Why are Offsets important?

Offsets allow Kafka consumers to know:

- which messages have already been processed
- where to resume after a restart
- how to recover from failures

Without offsets Kafka could not provide reliable message processing.

---

# Consumer Groups

Consumers that share the same Group ID cooperate.

Example:

Topic Orders

Partition 0
Partition 1
Partition 2

↓

Consumer Group

OrderConsumer

AuditConsumer

NotificationConsumer

Kafka assigns partitions to consumers so that each partition is processed by only one consumer within the same group.

---

# Current Configuration

```yaml
spring:
  kafka:
    consumer:
      enable-auto-commit: true
      auto-offset-reset: earliest
      auto-commit-interval: 5000
```

---

# Auto Commit

Current version uses:

```
enable-auto-commit=true
```

This means Kafka periodically stores the last processed offset automatically.

Advantages

- Easy configuration
- Less code
- Suitable for simple consumers

Disadvantages

- Messages may be acknowledged before business processing finishes.

---

# Manual Commit (Concept)

Instead of Kafka committing offsets automatically, the application explicitly confirms message processing.

Example flow

Receive message

↓

Process business logic

↓

Commit offset

↓

Next message

This gives much more control and improves reliability.

Manual Commit will be implemented in the next version.

---

# AckMode

Spring Kafka offers different acknowledgment strategies.

Common modes include:

- RECORD
- BATCH
- MANUAL
- MANUAL_IMMEDIATE

Current version still uses the default behavior.

Future versions will demonstrate manual acknowledgment.

---

# Delivery Guarantees

## At Most Once

Message is committed before processing.

Pros

- Fast

Cons

- Messages may be lost.

---

## At Least Once

Message is committed after processing.

Pros

- Reliable

Cons

- Duplicate processing is possible.

This is the most common strategy in enterprise systems.

---

## Exactly Once

Kafka also supports Exactly Once Semantics (EOS).

It guarantees that a message is processed only once.

Requirements include:

- Idempotent Producer
- Transactions
- Transaction-aware Consumers

This topic will be explored in future versions.

---

# Retry

If processing fails, applications often retry the operation.

Typical strategy:

Receive Message

↓

Processing Failed

↓

Retry

↓

Success

or

↓

Dead Letter Topic

---

# Dead Letter Topic (DLT)

Messages that cannot be processed after several retries should not block the application.

Instead they are redirected to another topic.

Example:

orders

↓

Processing Failed

↓

orders-dlt

Operations teams can inspect these failed messages later.

---

# What was implemented in this version?

✔ Producer using message keys

✔ Consumers reading partition information

✔ Offset inspection

✔ Logging with SLF4J

✔ Auto Commit enabled

✔ Consumer Groups

✔ Partition distribution

---

# Example Output

Producer

```
Order sent

Key : 1001

Customer : Edgar
```

Consumer

```
Order received

Order Id : 1001

Partition : 0

Offset : 0
```

Kafka UI

| Partition | Offset | Key |
|------------|--------|-----|
|0|0|1001|
|0|1|1002|
|1|0|1003|
|1|1|1004|

---

# Key Takeaways

- Offsets identify the position of a message inside a partition.
- Every partition has its own offset sequence.
- Consumer Groups distribute partitions across consumers.
- Auto Commit is simple but offers less control.
- Manual Commit provides greater reliability.
- Delivery guarantees depend on when offsets are committed.
- Dead Letter Topics prevent problematic messages from blocking processing.

---

Version

v1.6.0

Author

Edgar Ricardo Hernández Fonseca