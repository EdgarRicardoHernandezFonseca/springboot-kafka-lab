# README-v1.4.0.md

# Spring Boot + Apache Kafka Laboratory

## Version v1.4.0 – Message Keys and Partitions

---

# Overview

In the previous version, the application was able to exchange JSON objects between a Producer and a Consumer using Apache Kafka.

This version introduces one of the most important concepts in Kafka:

- **Message Keys**
- **Topic Partitions**
- **Ordering Guarantee**

Understanding these concepts is essential because they explain how Kafka distributes messages and guarantees their ordering.

---

# Learning Objectives

After completing this laboratory, you will understand:

- What a Message Key is.
- Why Kafka uses partitions.
- How Kafka chooses the destination partition.
- Why messages with the same Key always go to the same partition.
- How Kafka preserves message ordering.
- How to verify partitions using Kafka UI.

---

# What is a Message Key?

A Message Key is an optional identifier attached to every Kafka message.

A Kafka message consists of:

```
Key
Value
```

Example:

```
Key   : 1001

Value : Order(...)
```

The key is **not part of the business data**.

Instead, Kafka uses it internally to determine the destination partition.

---

# Why is a Message Key needed?

Without a Message Key, Kafka distributes messages automatically.

Although this provides good load balancing, there is no guarantee that related events will be stored together.

Using a Message Key ensures that:

- Messages belonging to the same entity stay together.
- Event ordering is preserved.
- Consumers process related events sequentially.

Typical Message Keys include:

| Business Domain | Message Key |
|-----------------|-------------|
| E-commerce | orderId |
| Banking | accountId |
| Inventory | productId |
| Shipping | shipmentId |
| Customer | customerId |

---

# Sending Messages Without a Key

When the Producer sends:

```java
kafkaTemplate.send("orders", order);
```

Kafka receives:

```
Key   : null

Value : Order(...)
```

Kafka uses its default partitioning strategy to distribute messages.

Example:

```
orders

Partition 0
------------
Order 1001

Partition 1
------------
Order 1002

Partition 2
------------
Order 1003
```

The Producer has no control over where messages are stored.

---

# Sending Messages With a Key

In this laboratory the Producer sends:

```java
kafkaTemplate.send(
    "orders",
    order.getOrderId().toString(),
    order
);
```

Kafka now receives:

```
Key   : 1001

Value : Order(...)
```

Instead of distributing messages randomly, Kafka calculates the destination partition.

---

# Kafka Partitioning Algorithm

Kafka uses the following algorithm:

```
partition = hash(key) % numberOfPartitions
```

Example:

```
Key

1001
```

Kafka calculates:

```
hash("1001")
```

Suppose the result is:

```
125847
```

The topic contains three partitions:

```
125847 % 3 = 0
```

The message is stored in:

```
Partition 0
```

The same calculation always produces the same partition.

---

# Same Key → Same Partition

If the Producer sends multiple events with the same Key:

```
1001

1001

1001

1001
```

Kafka always calculates the same hash value.

Result:

```
Partition 0

Create Order

Update Order

Payment Received

Order Shipped
```

Every event remains inside the same partition.

---

# Different Keys

Different keys generate different hash values.

Example:

```
1001

1002

1003

1004
```

Possible distribution:

```
Partition 0

1001

1002
```

```
Partition 1

1003

1004
```

```
Partition 2

(empty)
```

The exact partition assignment depends on the hash function.

It is completely normal for one partition to temporarily receive no messages.

---

# Topic Configuration

The laboratory creates the topic automatically using Spring Boot.

```java
@Bean
public NewTopic ordersTopic() {

    return TopicBuilder
            .name("orders")
            .partitions(3)
            .replicas(1)
            .build();
}
```

Configuration:

- Topic name: **orders**
- Partitions: **3**
- Replication factor: **1**

---

# Producer Implementation

The Producer now sends both the Key and the JSON object.

```java
String key = order.getOrderId().toString();

kafkaTemplate.send(
    "orders",
    key,
    order
);
```

This ensures that all events related to the same order are stored together.

---

# Ordering Guarantee

One of Kafka's most important features is the **Ordering Guarantee**.

Kafka guarantees message ordering **only inside a single partition**.

Example:

```
Order 1001

↓

Create Order

↓

Update Order

↓

Payment Received

↓

Invoice Generated

↓

Order Shipped
```

Because every event uses the same Message Key:

```
1001
```

Kafka stores them sequentially:

```
Partition 0

Offset 10

Create Order
```

```
Offset 11

Update Order
```

```
Offset 12

Payment Received
```

```
Offset 13

Invoice Generated
```

```
Offset 14

Order Shipped
```

Consumers always read these events in the correct order.

---

# Kafka UI Verification

Kafka UI allows visual verification of the partitioning process.

Navigate to:

```
Local

↓

Topics

↓

orders
```

The interface displays:

- Partitions
- Offsets
- Keys
- Values
- Timestamps

By sending multiple messages, it becomes clear that:

- Different keys may be stored in different partitions.
- Identical keys always remain in the same partition.

---

# Laboratory Test

Several HTTP requests were sent using Postman.

Example:

```json
{
    "orderId": 1001,
    "customerName": "Edgar Hernandez",
    "totalAmount": 150000.0
}
```

The Producer generated:

```
Key

1001
```

Kafka stored every event with this key in the same partition, confirming the Ordering Guarantee.

---

# Key Takeaways

This laboratory introduced one of Apache Kafka's core concepts.

The main conclusions are:

- A Message Key identifies related events.
- Kafka uses the key to calculate the destination partition.
- The partition is determined by the hash algorithm.
- The same key always maps to the same partition.
- Kafka guarantees message ordering only within a partition.
- Kafka UI provides an easy way to visualize partitions, offsets, keys, and messages.

Understanding Message Keys and Partitions is fundamental before learning Consumer Groups, Parallel Processing, and Kafka's scalability model.

---

# Next Version

**v1.5.0 – Consumer Groups and Parallel Processing**

Topics to be covered:

- Consumer Groups
- Multiple Consumers
- Partition Assignment
- Rebalancing
- Parallel Processing
- Horizontal Scalability

This version will demonstrate how Kafka distributes partitions among consumers while maintaining ordering guarantees.

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