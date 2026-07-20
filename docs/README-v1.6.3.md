# Spring Boot Kafka Lab v1.6.3

## Dead Letter Topic (DLT)

---

# What is a Dead Letter Topic?

A **Dead Letter Topic (DLT)** is a special Kafka topic used to store messages that cannot be processed successfully after several retry attempts.

Instead of losing failed messages or blocking the consumer indefinitely, Kafka sends them to a dedicated topic where they can be analyzed later.

This is one of the most common patterns used in production systems.

---

# Why is a DLT necessary?

Imagine the following scenario:

```

Order Producer
|
v
orders Topic
|
v
Notification Consumer

```

The consumer receives an order.

Everything works correctly...

until one message causes an unexpected exception.

For example:

```

{
"orderId":1002,
"customerName":"ERROR",
"totalAmount":500000
}

```

The consumer throws an exception:

```

RuntimeException
↓

Retry #1

↓

Retry #2

↓

Retry #3

↓

Still failing

```

Without a DLT, there are only two possibilities:

- Lose the message
- Stop processing the queue

Neither option is acceptable in enterprise applications.

---

# Dead Letter Topic Flow

```

Producer
|
v
orders
|
v
Consumer
|
+----------------------+
| Processing OK |
+----------------------+
|
Business Completed

```

If processing fails:

```

Producer
|
v
orders
|
v
Consumer
|
Exception
|
Retry 1
|
Retry 2
|
Retry 3
|
v
orders.DLT
|
v
DeadLetterConsumer

```

The failed message is safely stored inside the Dead Letter Topic.

---

# Spring Kafka Configuration

The project uses Spring Kafka's **DefaultErrorHandler** together with a **DeadLetterPublishingRecoverer**.

```

@Bean
public DefaultErrorHandler defaultErrorHandler(
KafkaTemplate<Object,Object> kafkaTemplate){

DeadLetterPublishingRecoverer recoverer =
new DeadLetterPublishingRecoverer(kafkaTemplate);

FixedBackOff backOff =
new FixedBackOff(2000L,3);

return new DefaultErrorHandler(
recoverer,
backOff);

}

```

Configuration summary:

- Retry interval: **2 seconds**
- Maximum retries: **3**
- Failed messages are automatically published to **orders.DLT**

---

# Dead Letter Consumer

A dedicated consumer listens for failed messages.

```

@KafkaListener(
topics = "orders.DLT",
groupId = "dlt-group"
)

```

Example log:

```

==============================
MESSAGE ARRIVED TO DLT
==============================
OrderId : 1002
Customer: ERROR
Amount : 500000

```

---

# Kafka UI

Kafka UI allows visual verification of failed messages.

Example:

Topic:

```

orders.DLT

```

Stored message:

```

Offset : 0

Key : 1002

{
"orderId":1002,
"customerName":"ERROR",
"totalAmount":500000
}

```

This confirms that Spring Kafka successfully redirected the failed message to the Dead Letter Topic.

---

# Benefits of Dead Letter Topics

Using a DLT provides several advantages:

- Prevents message loss
- Avoids infinite retry loops
- Prevents consumer blocking
- Enables later investigation of failed events
- Improves system resiliency
- Supports production-grade event-driven architectures

---

# Technologies

- Java 17+
- Spring Boot
- Spring Kafka
- Apache Kafka
- Kafka UI
- Docker Compose

---

# Version

Current version:

**v1.6.3**

Implemented features:

- Producer
- Consumer
- Consumer Groups
- Partitions
- Manual Acknowledgement
- Retry Mechanism
- Dead Letter Topic (DLT)
- Kafka UI Monitoring

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