# README-v1.8.1 – Multiple Consumers Implementation & Load Distribution

# Multiple Consumers Implementation & Load Distribution

## Overview

This version demonstrates one of the most important capabilities of Apache Kafka: horizontal scalability through Consumer Groups and Partitions.

The project is configured with:

3 partitions for the orders topic.
3 concurrent consumer instances.
A single Consumer Group named order-processing.
Manual acknowledgment (AckMode.MANUAL).
Production-style logging.
Retry and Dead Letter Topic (DLT) support.

When multiple messages are published, Kafka automatically distributes the workload across the available consumer instances while preserving message ordering inside each partition.

---

# Architecture

                          +--------------------+
                          |  Order Producer    |
                          +---------+----------+
                                    |
                                    |
                              Topic: orders
                     +--------------+--------------+
                     |              |              |
                     |              |              |
               Partition 0    Partition 1    Partition 2
                     |              |              |
                     |              |              |
              Consumer #0    Consumer #1    Consumer #2
              (Thread 0)     (Thread 1)     (Thread 2)

Each consumer belongs to the same Consumer Group, allowing Kafka to automatically balance the workload.


---

# Project Configuration

Topic Configuration

The orders topic is configured with three partitions.

@Bean
public NewTopic ordersTopic() {

    return TopicBuilder
            .name("orders")
            .partitions(3)
            .replicas(1)
            .build();
}

---

# Concurrent Consumers

The listener container factory enables three concurrent consumer instances.

factory.setConcurrency(3);

This creates three Kafka consumer threads inside the same Spring Boot application.

---

# Kafka Listener

@KafkaListener(
        topics = "orders",
        groupId = "order-processing",
        containerFactory = "kafkaListenerContainerFactory"
)

All consumers belong to the same Consumer Group.

---

# Execution

Multiple order events were published to the orders topic.

Example:

Order 1001
Order 1002
Order 1003
Order 1004
Order 1005
Order 1006
Order 1007
Order 1008
Order 1009
Order 1010

As new events arrived, Kafka automatically assigned each message to one of the available partitions.

Each consumer thread processed the events assigned to its partition.

---

# Automatic Load Distribution

Kafka guarantees that:

Only one consumer inside a Consumer Group processes a partition.
Messages from the same partition remain ordered.
Different partitions can be processed simultaneously.

Example:

Partition 0
    ↓
Consumer Thread #5-0-C-1

Partition 1
    ↓
Consumer Thread #5-1-C-1

Partition 2
    ↓
Consumer Thread #5-2-C-1

This allows true concurrent processing without duplicate message consumption.

---

# Production Logging

The application produces enterprise-style logs that include operational metadata.

Example:

============================================================

ORDER PROCESSING STARTED

Consumer Thread : KafkaListenerEndpointContainer#5-1-C-1
Consumer Group  : order-processing
Partition       : 1
Offset          : 6
Correlation ID  : da96cb81-5789-4ceb-83fa-d7566fda03d8
Event Type      : ORDER_CREATED
Event Version   : v2
Source          : springboot-kafka-lab
Timestamp        : 2026-07-20T18:10:11.239921900Z

============================================================

These logs provide complete traceability for every processed event.

---

# Processing Flow

Producer
    │
    ▼
Kafka Topic (orders)
    │
    ▼
Partition Assignment
    │
    ▼
Consumer Group
    │
    ▼
Consumer Thread
    │
    ▼
Business Services
    │
    ├── OrderService
    ├── InventoryService
    ├── NotificationService
    └── AuditService
    │
    ▼
Manual Offset Commit

---

# Observed Results

During execution, the following behavior was observed:

Multiple consumer threads were created automatically.
Each consumer processed messages from a different partition.
Message ordering was preserved inside each partition.
Processing occurred concurrently.
Manual acknowledgment committed offsets only after successful processing.
Retry and Dead Letter Topic mechanisms remained available for failed messages.

---

# Screenshots

Kafka UI

Insert a screenshot showing:

Topic: orders
Three partitions
Partition offsets

Example:

docs/images/v1.8.1/...

---

# Spring Boot Logs

Insert screenshots showing:

Consumer Thread #5-0-C-1
Consumer Thread #5-1-C-1
Consumer Thread #5-2-C-1

Example:

docs/images/v1.8.1/consumer-thread-0.png

docs/images/v1.8.1/consumer-thread-1.png

docs/images/v1.8.1/consumer-thread-2.png 

---

# Automatic Message Distribution

Include a screenshot demonstrating different partitions being processed simultaneously.

Example:

docs/images/v1.8.1/load-distribution.png

---

# Concepts Demonstrated

| Feature | Status |
|---------|--------|
| Apache Kafka Producer | ✅ |
| Kafka Consumer | ✅ |
| Consumer Groups | ✅ |
| Topic Partitions | ✅ |
| Concurrent Consumers | ✅ |
| Automatic Load Distribution | ✅ |
| Message Ordering per Partition | ✅ |
| Manual Acknowledgment | ✅ |
| Retry Strategy | ✅ |
| Dead Letter Topic (DLT) | ✅ |
| Production Logging | ✅ |

---

# Key Takeaways

This implementation demonstrates how Apache Kafka distributes workload across multiple consumer instances within the same Consumer Group.

By configuring multiple partitions and enabling concurrent consumers, the application achieves horizontal scalability while ensuring that each message is 
processed exactly once within the group. Kafka automatically assigns partitions to consumer instances, balances the workload, and preserves message ordering 
inside each partition.

Combined with manual acknowledgments, retry handling, Dead Letter Topics, and production-style logging, this version closely resembles the architecture and 
operational behavior of a real-world event-driven microservices application.

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