# Spring Boot Kafka Lab v1.2.0

# Kafka Consumer

## Objective

The objective of this version is to understand how a Kafka Consumer works and how Spring Boot can receive messages asynchronously from an Apache Kafka topic.

Unlike a Producer, which publishes messages, a Consumer continuously listens to one or more topics waiting for new events to arrive.

---

# What is a Kafka Consumer?

A Kafka Consumer is an application responsible for reading messages from one or more Kafka topics.

Consumers subscribe to topics and process every message that producers publish.

Kafka stores the messages, while consumers decide when and how to process them.

```
Producer  --->  Kafka Topic  --->  Consumer
```

---

# Why is a Kafka Consumer needed?

Without consumers, messages stored in Kafka would never be processed.

Consumers make event-driven architectures possible by reacting automatically whenever a new event arrives.

Typical responsibilities include:

- Processing orders
- Sending emails
- Updating databases
- Calling external APIs
- Generating reports
- Triggering business workflows

---

# Kafka Consumer Workflow

The basic workflow is:

```
Producer
    │
    ▼
Kafka Topic
    │
    ▼
Consumer subscribes
    │
    ▼
Kafka delivers the message
    │
    ▼
Consumer processes the message
```

Unlike HTTP requests, the producer does not wait for the consumer.

The producer simply publishes the event.

Kafka guarantees that subscribed consumers can later retrieve the message.

---

# Consumer Groups

Kafka consumers belong to a Consumer Group.

```
Topic
   │
   ▼

Consumer Group

 ├── Consumer A
 ├── Consumer B
 └── Consumer C
```

Kafka distributes partitions among consumers in the same group.

This provides:

- scalability
- load balancing
- fault tolerance

---

# Consumer Groups in this Laboratory

This project uses a single consumer.

```
Producer
      │
      ▼

orders Topic

      │
      ▼

OrderConsumer
(Group: order-group)
```

The consumer listens continuously for new messages.

---

# Spring Boot Consumer

Spring Kafka simplifies consumer creation using the `@KafkaListener` annotation.

Example:

```java
@KafkaListener(
    topics = "orders",
    groupId = "order-group"
)
public void consume(String message) {

    System.out.println(message);

}
```

Spring automatically:

- creates the Kafka consumer
- connects to the broker
- subscribes to the topic
- receives new messages
- invokes the annotated method

---

# Consumer Lifecycle

```
Application starts

        │

        ▼

Kafka Consumer created

        │

        ▼

Subscribe to Topic

        │

        ▼

Wait for new messages

        │

        ▼

Receive message

        │

        ▼

Execute business logic

        │

        ▼

Wait for next message
```

---

# Advantages

Using Kafka Consumers provides several advantages:

- asynchronous processing
- loose coupling
- scalability
- high throughput
- fault tolerance
- automatic load balancing
- event-driven architecture

---

# Laboratory Implementation

This laboratory includes one consumer.

```
OrderConsumer
```

Its responsibilities are:

- subscribe to the `orders` topic
- receive published messages
- display received messages in the console

Example output:

```
--------------------------------
Message received:

First Kafka Message

--------------------------------
```

---

# Project Structure

```
Producer

    POST /orders

        │

        ▼

Kafka Topic (orders)

        │

        ▼

OrderConsumer

        │

        ▼

Console Output
```

---

# Version Summary

Version **v1.2.0** introduces the consumer side of Apache Kafka.

The application now supports complete asynchronous communication:

- Spring Boot Producer
- Apache Kafka Broker
- Spring Boot Consumer
- Kafka UI for topic visualization

This version demonstrates the complete publish-and-consume messaging flow using Apache Kafka and Spring Boot.