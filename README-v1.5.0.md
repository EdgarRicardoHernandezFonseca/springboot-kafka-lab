# Spring Boot Kafka Lab
# v1.5.0 – Consumer Groups & Parallel Processing

## Objective

This version introduces one of Apache Kafka's most important scalability features:

- Consumer Groups
- Parallel Message Processing
- Partition Assignment
- Consumer Rebalancing

Instead of having a single consumer processing all events, multiple consumers now belong to the same consumer group, allowing Kafka to distribute partitions automatically.

---

# What is a Consumer Group?

A Consumer Group is a set of consumers working together to process messages from the same topic.

Kafka guarantees that:

- each partition is consumed by only one consumer inside the same group
- consumers can process data in parallel
- message ordering is preserved inside each partition

Example:

```
Topic: orders

Partitions

P0
P1
P2

Consumer Group: order-group

Consumer A -> P0

Consumer B -> P1

Consumer C -> P2
```

This allows applications to scale horizontally.

---

# Implemented Consumers

This version contains three independent consumers.

## 1. OrderConsumer

Responsible for the business logic.

Example output:

```
Order received

Order Id      : 1015
Customer Name : Michael
Total Amount  : 430000
```

---

## 2. AuditConsumer

Responsible for audit logging.

Example output:

```
AUDIT CONSUMER

Order : 1015
```

---

## 3. NotificationConsumer

Represents another independent service.

Example output

```
NOTIFICATION CONSUMER

Order : 1015
```

Although all consumers subscribe to the same topic, Kafka guarantees that each partition is owned by only one consumer at a time.

---

# Parallel Processing

Kafka distributes partitions among the consumers.

Example

```
orders

Partition 0 -> OrderConsumer

Partition 1 -> AuditConsumer

Partition 2 -> NotificationConsumer
```

Each consumer works independently, increasing throughput.

---

# Consumer Rebalancing

Whenever a consumer joins or leaves the group, Kafka automatically redistributes partitions.

Typical lifecycle:

```
Consumer joins

↓

Partitions revoked

↓

New assignment calculated

↓

Partitions assigned

↓

Consumers continue processing
```

Example logs:

```
(Re-)joining group

Successfully joined group

Finished assignment

Partitions revoked

Partitions assigned
```

This process is called **Consumer Rebalancing**.

---

# Kafka UI Verification

Kafka UI can be used to verify:

- Consumer Groups
- Group Members
- Assigned Partitions
- Consumer Lag
- Current Offsets

Example:

```
Consumer Group

order-group

Members

3

Assigned Partitions

3

Total Lag

0
```

A lag of zero indicates that all messages are being processed successfully.

---

# What was learned

This version demonstrates:

- Consumer Groups
- Multiple Consumers
- Parallel Processing
- Automatic Partition Assignment
- Consumer Rebalancing
- Horizontal Scalability
- Offset Management
- Kafka UI Monitoring

These concepts are fundamental for building scalable event-driven microservices.