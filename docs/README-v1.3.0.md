# README-v1.3.0.md

# v1.3.0 - JSON Serialization

## Overview

In the previous versions of this laboratory, the Producer sent only plain text (`String`) messages to Kafka.

Starting with **v1.3.0**, the application exchanges complete Java objects by automatically converting them into JSON before sending them to Apache Kafka and converting them back into Java objects when they are consumed.

This process is known as **JSON Serialization** and **JSON Deserialization**, and it is the standard approach used by most Spring Boot microservices communicating through Kafka.

---

# Learning Objectives

After completing this version, you will understand:

- What serialization is.
- What deserialization is.
- Why JSON is commonly used in event-driven architectures.
- How Spring Boot automatically serializes Java objects.
- How Spring Boot automatically deserializes Kafka messages.
- How to configure JsonSerializer and JsonDeserializer.
- How a REST API publishes JSON events into Kafka.

---

# What is Serialization?

Serialization is the process of converting an object into a format that can be transmitted or stored.

In this laboratory, the Java object:

```java
Order order = new Order(
    1001L,
    "Edgar Hernandez",
    150000.0
);
```

is automatically converted into:

```json
{
  "orderId": 1001,
  "customerName": "Edgar Hernandez",
  "totalAmount": 150000.0
}
```

before being sent to Kafka.

---

# What is JSON?

JSON (**JavaScript Object Notation**) is a lightweight text format used to exchange structured data.

It is:

- Human-readable
- Language-independent
- Compact
- Widely supported

Example:

```json
{
  "orderId": 1001,
  "customerName": "Edgar Hernandez",
  "totalAmount": 150000.0
}
```

Because JSON is language-independent, a Producer written in Java can communicate with Consumers written in Python, Go, Node.js, .NET, or any other language.

---

# What is JSON Serialization?

JSON Serialization is the process of converting a Java object into JSON.

Spring Kafka performs this automatically using:

```yaml
spring:

  kafka:

    producer:

      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
```

Flow:

```
Java Object
      │
      ▼
JsonSerializer
      │
      ▼
JSON
      │
      ▼
Kafka Topic
```

---

# What is JSON Deserialization?

JSON Deserialization is the opposite process.

The JSON message stored inside Kafka is converted back into a Java object.

This is performed automatically by:

```yaml
spring:

  kafka:

    consumer:

      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
```

Flow:

```
Kafka Topic
      │
      ▼
JSON
      │
      ▼
JsonDeserializer
      │
      ▼
Java Object
```

---

# Why is JSON used?

JSON offers several advantages:

- Easy to read.
- Easy to debug.
- Platform independent.
- Supports nested objects.
- Supports arrays.
- Widely adopted by REST APIs.
- Standard format for microservices.

Because of these characteristics, JSON has become the most common event format in Kafka-based applications.

---

# DTO (Data Transfer Object)

This version introduces the `Order` DTO.

```java
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Order {

    private Long orderId;

    private String customerName;

    private Double totalAmount;

}
```

The DTO represents the business data that will travel through Kafka.

---

# Lombok

To reduce boilerplate code, this project uses Lombok.

```java
@Data
```

Generates automatically:

- getters
- setters
- equals()
- hashCode()
- toString()

---

```java
@NoArgsConstructor
```

Generates:

```java
public Order() {
}
```

---

```java
@AllArgsConstructor
```

Generates:

```java
public Order(
    Long orderId,
    String customerName,
    Double totalAmount
)
```

without manually writing constructors.

---

# Producer

The Producer now sends Java objects instead of Strings.

```java
@Service
public class OrderProducer {

    private final KafkaTemplate<String, Order> kafkaTemplate;

    public OrderProducer(KafkaTemplate<String, Order> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(Order order) {
        kafkaTemplate.send("orders", order);
    }

}
```

Notice that:

```java
KafkaTemplate<String, Order>
```

is now configured with `Order` instead of `String`.

Spring automatically serializes the object into JSON before publishing it.

---

# Consumer

The Consumer receives a complete Java object.

```java
@KafkaListener(
    topics = "orders",
    groupId = "order-group"
)
public void consume(Order order) {

    System.out.println(order.getOrderId());

}
```

Spring automatically converts the JSON back into an `Order` instance.

No manual parsing is required.

---

# REST Controller

The REST API receives a JSON request.

```java
@PostMapping
public String sendOrder(@RequestBody Order order) {

    producer.send(order);

    return "Order event sent successfully.";

}
```

The annotation:

```java
@RequestBody
```

instructs Spring Boot to convert the incoming JSON into a Java object.

---

# Request Example

Endpoint:

```
POST
http://localhost:8080/orders
```

Headers:

```
Content-Type: application/json
```

Body:

```json
{
    "orderId":1001,
    "customerName":"Edgar Hernandez",
    "totalAmount":150000.0
}
```

---

# Application Configuration

Producer configuration:

```yaml
spring:

  kafka:

    producer:

      key-serializer: org.apache.kafka.common.serialization.StringSerializer

      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
```

---

Consumer configuration:

```yaml
spring:

  kafka:

    consumer:

      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer

      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer

      properties:

        spring.json.trusted.packages: com.erhernandez.kafka.dto

        spring.json.value.default.type: com.erhernandez.kafka.dto.Order
```

---

# Why is `groupId` required?

Kafka Consumers always belong to a Consumer Group.

```java
@KafkaListener(
    topics = "orders",
    groupId = "order-group"
)
```

The Consumer Group allows Kafka to:

- coordinate consumers
- distribute partitions
- perform rebalancing
- guarantee that each message is processed only once within the same group

Without a `groupId`, Spring Kafka cannot create the Consumer and the application fails to start.

---

# End-to-End Message Flow

```
REST Client
     │
     ▼
POST /orders
     │
     ▼
OrderController
     │
     ▼
OrderProducer
     │
     ▼
JsonSerializer
     │
     ▼
Kafka Topic (orders)
     │
     ▼
JsonDeserializer
     │
     ▼
OrderConsumer
     │
     ▼
Java Order Object
```

---

# Project Structure

```
src
└── main
    ├── java
    │   └── com.erhernandez.kafka
    │       ├── config
    │       │     KafkaTopicConfig
    │       ├── controller
    │       │     OrderController
    │       ├── consumer
    │       │     OrderConsumer
    │       ├── producer
    │       │     OrderProducer
    │       └── dto
    │             Order
    │
    └── resources
            application.yml
```

---

# Technologies Used

- Java 21
- Spring Boot 3.5.x
- Spring for Apache Kafka
- Apache Kafka 3.9.x
- Docker
- Kafka UI
- Maven
- Lombok
- Jackson JSON

---

# Version Summary

Version **v1.3.0** introduces one of the most important capabilities in Kafka-based applications: **sending complete Java objects as JSON events**.

Instead of publishing simple text messages, the Producer now serializes business objects into JSON using Spring Kafka's `JsonSerializer`. Kafka stores these JSON events, and the Consumer automatically reconstructs them into Java objects using `JsonDeserializer`.

This version represents the transition from a basic messaging example to a realistic event-driven communication model, closely resembling how modern Spring Boot microservices exchange business events in production environments.

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