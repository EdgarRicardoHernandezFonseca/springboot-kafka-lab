# README-v2.0.0 — Transactional Outbox + Avro + Kafka Headers + Consumer Flow

## Overview

This release documents an end-to-end Kafka event flow using Spring Boot, PostgreSQL, the Transactional Outbox Pattern, Apache Kafka, Apache Avro, Confluent Schema Registry, Kafka headers, an Avro consumer, and notification events.

The main goal is to guarantee that an order and its corresponding event are persisted atomically, while allowing Kafka publication to happen asynchronously and independently from the database transaction.

---

## 1. Transactional Outbox

The Transactional Outbox Pattern stores the business entity and its event in the same database transaction.

Instead of:

```text
Save order -> Publish Kafka event
```

the application performs:

```text
Save order
    +
Save outbox event
    |
    v
Commit database transaction
    |
    v
Outbox Publisher
    |
    v
Kafka
```

This avoids the classic dual-write problem.

Without an outbox:

```text
1. Save order       -> SUCCESS
2. Publish Kafka    -> FAILURE

Result:
Order exists, but the event is lost.
```

With an outbox:

```text
1. Save order
2. Save outbox event
3. Commit transaction
4. Publish event asynchronously
```

If the outbox write fails, the order transaction is rolled back.

---

## 2. Atomicity Between `orders` and `outbox_events`

`OrderPersistenceService` uses:

```java
@Transactional
public OrderEntity save(OrderEntity order) {
    ...
}
```

Inside the transaction:

```text
orders
  |
  +-- INSERT order

outbox_events
  |
  +-- INSERT ORDER_CREATED event

       |
       v
   COMMIT
```

Both database operations belong to the same PostgreSQL transaction.

Therefore:

```text
Order saved + Outbox saved = COMMIT
Order saved + Outbox failed = ROLLBACK
```

This is the fundamental reliability guarantee of the implementation.

---

## 3. Outbox Publisher

`OutboxPublisher` periodically searches for pending events:

```java
var events = outboxService.findPendingEvents();
```

Pending events are retrieved using:

```java
findByProcessedAtIsNullOrderByCreatedAtAsc()
```

The publisher then:

1. Reads the outbox event.
2. Deserializes its stored payload.
3. Reconstructs `OrderCreated`.
4. Publishes the Avro event to Kafka.
5. Marks the outbox event as processed.

Flow:

```text
outbox_events
     |
     | processed_at IS NULL
     v
OutboxPublisher
     |
     v
JSON -> OrderCreated
     |
     v
KafkaTemplate<String, OrderCreated>
     |
     v
Kafka topic: orders
```

The outbox therefore acts as a durable buffer between PostgreSQL and Kafka.

---

## 4. Avro Publication

The project publishes `OrderCreated` using Apache Avro.

The producer uses:

```java
KafkaAvroSerializer
```

The Kafka producer configuration contains:

```yaml
value-serializer: io.confluent.kafka.serializers.KafkaAvroSerializer
```

The producer factory also explicitly configures:

```java
ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
KafkaAvroSerializer.class
```

The resulting Kafka value is therefore serialized using the Confluent Avro wire format.

---

## 5. Schema Registry

The application uses Confluent Schema Registry:

```yaml
schema.registry.url: http://localhost:8085
```

The Avro producer uses the registry to manage the schema associated with the event.

Architecture:

```text
Spring Boot
     |
     v
OrderCreated
     |
     v
KafkaAvroSerializer
     |
     +--------> Schema Registry
     |
     v
Kafka
```

The consumer uses the corresponding Avro deserializer and the registered schema to reconstruct the generated `OrderCreated` class.

---

## 6. Kafka Headers

Kafka records contain both a value and optional headers.

Example:

```text
Kafka Record
+------------------------------------------------+
| Key                                            |
| 5001                                           |
+------------------------------------------------+
| Headers                                        |
| eventType     = ORDER_CREATED                  |
| eventVersion  = v2                             |
| correlationId = ...                            |
| source        = springboot-kafka-lab           |
+------------------------------------------------+
| Value                                          |
| OrderCreated Avro event                       |
+------------------------------------------------+
```

Headers are metadata attached to the Kafka record.

---

## 7. `eventType`

`eventType` describes the business event.

Example:

```text
ORDER_CREATED
```

The producer adds it to the record:

```java
record.headers().add(
    new RecordHeader(
        "eventType",
        eventType.name().getBytes(StandardCharsets.UTF_8)
    )
);
```

Other event types in the project include:

```text
ORDER_UPDATED
ORDER_CANCELLED
```

This allows consumers to understand what business action produced the message.

---

## 8. `eventVersion`

`eventVersion` identifies the version of the event contract.

Example:

```text
v2
```

The producer adds:

```java
record.headers().add(
    new RecordHeader(
        "eventVersion",
        "v2".getBytes(StandardCharsets.UTF_8)
    )
);
```

This becomes useful when event schemas evolve:

```text
ORDER_CREATED
    |
    +-- v1
    +-- v2
    +-- v3
```

The header provides metadata that helps consumers and operational tools identify the event version.

---

## 9. `correlationId`

`correlationId` identifies a particular request/event flow.

The producer generates it with:

```java
String correlationId = UUID.randomUUID().toString();
```

Example:

```text
403ec7bd-fabc-4fad-ac3e-e358350671d9
```

It is then attached to the Kafka record:

```java
record.headers().add(
    "correlationId",
    correlationId.getBytes(StandardCharsets.UTF_8)
);
```

The identifier can be propagated to downstream events.

Example:

```text
HTTP request
    |
    v
ORDER_CREATED
    |
    | correlationId
    v
OrderConsumer
    |
    v
Notification
```

This makes distributed troubleshooting and tracing much easier.

---

## 10. Consumer Avro

The consumer is configured with:

```yaml
value-deserializer: io.confluent.kafka.serializers.KafkaAvroDeserializer
```

and:

```yaml
specific.avro.reader: true
```

The expected flow is:

```text
Kafka
  |
  | Avro binary value
  v
KafkaAvroDeserializer
  |
  v
OrderCreated
  |
  v
OrderConsumer
```

The consumer therefore receives the generated Avro class instead of a generic JSON object.

---

## 11. Notification Event

After consuming and processing the order event, the application publishes a notification event.

Observed example:

```json
{
  "orderId": 5001,
  "message": "Order processed successfully"
}
```

The complete flow becomes:

```text
POST /orders/v2/create
          |
          v
       orders DB
          |
          v
    outbox_events
          |
          v
    OutboxPublisher
          |
          v
     Kafka orders
          |
          v
      OrderConsumer
          |
          v
 Kafka notifications
```

The notification can also propagate event metadata such as:

```text
eventType
eventVersion
correlationId
source
```

---

## 12. Payload vs Headers

The Kafka payload contains the business event.

Example:

```json
{
  "orderId": 5001,
  "customerName": "Edgar",
  "priority": "HIGH",
  "product": "Laptop",
  "quantity": 1,
  "price": 2500.0,
  "createdAt": "..."
}
```

Headers contain metadata:

```text
eventType     = ORDER_CREATED
eventVersion  = v2
correlationId = ...
source        = springboot-kafka-lab
```

A useful mental model is:

```text
Payload = WHAT business data is being transported?

Headers = HOW should the message be identified,
          classified, versioned, or traced?
```

The payload and headers are independent parts of a Kafka record.

A valid Avro payload does not guarantee that the required headers are present.

---

## 13. `processed_at`

The `processed_at` column represents the processing state of an outbox event.

Initially:

```text
processed_at = NULL
```

means:

```text
The event is pending.
```

After successful processing:

```text
processed_at = timestamp
```

means:

```text
The publisher processed the event.
```

Useful query:

```sql
SELECT *
FROM outbox_events
WHERE processed_at IS NULL
ORDER BY created_at;
```

This identifies events that remain pending.

---

## 14. Error: `Unknown magic byte!`

The project previously produced:

```text
SerializationException: Unknown magic byte!
```

The cause was a serialization mismatch.

The consumer expected:

```text
KafkaAvroDeserializer
```

but the record had been published using a non-Avro serialization format such as plain JSON/String.

The Confluent Avro deserializer expects the Confluent Avro wire format.

Therefore:

```text
Producer:
StringSerializer / JSON

        !=

Consumer:
KafkaAvroDeserializer
```

causes:

```text
Unknown magic byte!
```

The correct pairing is:

```text
Producer:
KafkaAvroSerializer

Consumer:
KafkaAvroDeserializer
```

with Schema Registry available.

The previous `OutboxPublisher` used:

```java
KafkaTemplate<String, String>
```

and a `StringSerializer`.

That was incompatible with the Avro consumer.

The corrected publisher uses:

```java
KafkaTemplate<String, OrderCreated>
```

so the outbox event is converted back into the Avro object and serialized using the Avro producer configuration.

---

## 15. Error: `Missing header 'eventType'`

Another observed error was:

```text
Missing header 'eventType'
```

The consumer expected:

```java
@Header("eventType") String eventType
```

but the Kafka record did not contain that header.

This is different from an Avro deserialization problem.

The flow was:

```text
Avro value
    |
    +----> Deserialization SUCCESS
    |
    +----> eventType header MISSING
                     |
                     v
              Listener failure
```

The producer must explicitly add the header:

```java
record.headers().add(
    new RecordHeader(
        "eventType",
        eventType.name().getBytes(StandardCharsets.UTF_8)
    )
);
```

The same principle applies to:

```text
eventVersion
correlationId
source
```

Important lesson:

> Kafka payload and Kafka headers are independent.

---

## 16. Why `KafkaTemplate.send()` Is Not Immediate Confirmation

Kafka producer sends are asynchronous.

Calling:

```java
kafkaTemplate.send(record);
```

means that the producer accepted the send request. It does not automatically mean that Kafka has already acknowledged successful publication.

Conceptually:

```text
kafkaTemplate.send(record)
          |
          v
   asynchronous result
       /             /          SUCCESS        FAILURE
```

When publication confirmation matters, the application should observe the returned future.

Example:

```java
kafkaTemplate.send(record)
    .whenComplete((result, exception) -> {

        if (exception != null) {
            // publication failed
        } else {
            // Kafka acknowledged the record
        }
    });
```

This distinction is especially important for the Transactional Outbox Pattern.

### Important implementation caveat

The current publisher follows this sequence:

```java
kafkaTemplate.send(...);

outboxService.markAsProcessed(event);
```

This means `processed_at` can be updated before the asynchronous Kafka operation has actually succeeded.

A more robust design is:

```text
Outbox event
    |
    v
Kafka send
    |
    v
Kafka acknowledgement
    |
    +---- FAILURE ---> keep processed_at NULL
    |
    +---- SUCCESS ---> set processed_at
```

This prevents an event from being considered processed before Kafka confirms the publication.

For a production-grade implementation, the publisher should therefore coordinate `processed_at` with the actual send result and also consider retry, idempotency, and duplicate-delivery scenarios.

---

## 17. End-to-End Consumer Flow

The completed architecture is:

```text
                    HTTP
                     |
                     v
          POST /orders/v2/create
                     |
                     v
          OrderPersistenceService
                     |
              @Transactional
                     |
          +----------+----------+
          |                     |
          v                     v
       orders              outbox_events
          |                     |
          +----------+----------+
                     |
                  COMMIT
                     |
                     v
             OutboxPublisher
                     |
                     v
          OrderCreated Avro
                     |
                     v
                  Kafka
               topic: orders
                     |
                     v
              OrderConsumer
                     |
              Avro deserialize
                     |
              Header extraction
                     |
                     v
             Business processing
                     |
                     v
             Notification event
                     |
                     v
                  Kafka
            topic: notifications
```

---

## 18. Verified Example

Endpoint:

```text
POST /orders/v2/create
```

Request:

```json
{
  "orderId": 5001,
  "customerName": "Edgar",
  "priority": "HIGH",
  "product": "Laptop",
  "quantity": 1,
  "price": 2500.00
}
```

Response:

```text
200 OK
ORDER_CREATED stored in Transactional Outbox successfully.
```

Database:

```text
orders
-------
5001 | Edgar | HIGH | Laptop | 1 | 2500
```

Outbox:

```text
aggregate_id  = 5001
aggregate_type = Order
event_type     = ORDER_CREATED
processed_at   = populated
```

Kafka `orders`:

```text
key = 5001

value = OrderCreated Avro event

headers:
    eventVersion  = v2
    correlationId = ...
    eventType     = ORDER_CREATED
    source        = springboot-kafka-lab
```

Kafka `notifications`:

```text
key = 5001

value:
{
  "orderId": 5001,
  "message": "Order processed successfully"
}
```

This validates the complete flow:

```text
HTTP
 -> PostgreSQL
 -> Transactional Outbox
 -> Outbox Publisher
 -> Avro
 -> Schema Registry
 -> Kafka
 -> Avro Consumer
 -> Notification
```

---

## 19. Key Interview Questions

### What is the Transactional Outbox Pattern?

It is a pattern that stores the business entity and its corresponding event in the same database transaction. A separate publisher later sends the event to Kafka.

### Why use an Outbox instead of publishing directly to Kafka?

Because PostgreSQL and Kafka are different systems. A direct dual write can leave the database and Kafka inconsistent if one operation succeeds and the other fails.

### What does `processed_at` mean?

It represents the processing state of an outbox event. `NULL` normally means pending; a timestamp means that the publisher has processed it.

### What is Avro?

Avro is a schema-based serialization format commonly used for Kafka event contracts.

### What is Schema Registry?

Schema Registry stores and manages schemas used by serialized Kafka messages, including Avro schemas.

### What are Kafka headers?

Headers are metadata attached to a Kafka record. They can carry event type, event version, correlation IDs, source information, and similar metadata.

### What is the difference between payload and headers?

The payload contains business data. Headers contain metadata used to identify, classify, version, or trace the event.

### What causes `Unknown magic byte!`?

Usually a serialization mismatch: an Avro deserializer receives data that was not serialized using the expected Confluent Avro wire format.

### What causes `Missing header 'eventType'`?

The consumer expects an `eventType` header, but the producer did not include it in the Kafka record.

### Does `KafkaTemplate.send()` immediately confirm publication?

No. The producer operation is asynchronous. The application should observe the returned future when it needs confirmation of the Kafka send result.

### Why is this important for the Outbox Pattern?

Because marking an event as processed before Kafka acknowledges the send can incorrectly remove the event from the pending queue.

---

## 20. Validation

The implementation was validated with:

```text
mvn clean test
6/6 SUCCESS

mvn install
BUILD SUCCESS

Spring Boot
Started successfully

POST /orders/v2/create
200 OK

orders
Record verified

outbox_events
Record created and processed

Kafka orders
Avro ORDER_CREATED verified

Kafka headers
eventVersion verified
correlationId verified
eventType verified
source verified

Kafka notifications
Notification event verified

Consumer
Avro deserialization verified
```

---

## 21. Release Status

# v2.0.0 — COMPLETED

Implemented and documented:

- [x] Transactional Outbox
- [x] Atomic `orders` + `outbox_events`
- [x] Outbox Publisher
- [x] Avro publication
- [x] Schema Registry
- [x] Kafka headers
- [x] `eventType`
- [x] `eventVersion`
- [x] `correlationId`
- [x] Avro consumer
- [x] Notification event
- [x] Payload vs headers
- [x] `processed_at`
- [x] `Unknown magic byte` diagnosis
- [x] `Missing header 'eventType'` diagnosis
- [x] Asynchronous behavior of `KafkaTemplate.send()`
- [x] End-to-end consumer flow

---

## 22. Main Takeaways

1. **Transactional Outbox provides reliable persistence of events together with business data.**
2. **`orders` and `outbox_events` must be committed atomically.**
3. **The Outbox Publisher decouples database persistence from Kafka availability.**
4. **Avro producer and consumer serializers must be compatible.**
5. **Schema Registry manages the Avro schema contract.**
6. **Kafka headers carry metadata, not the main business payload.**
7. **`eventType` identifies the business event.**
8. **`eventVersion` identifies the event contract version.**
9. **`correlationId` enables request/event tracing.**
10. **`processed_at` represents the processing state of an outbox record.**
11. **`Unknown magic byte!` usually indicates a serialization-format mismatch.**
12. **`Missing header 'eventType'` indicates missing Kafka metadata.**
13. **An Avro payload can be valid while Kafka headers are incorrect or missing.**
14. **`KafkaTemplate.send()` is asynchronous and should not be treated as an immediate delivery confirmation.**
15. **A production-grade outbox publisher should coordinate `processed_at` with successful Kafka acknowledgement and consider retries and idempotency.**

---

## Version

```text
v2.0.0
Transactional Outbox + Avro + Kafka Headers + Consumer Flow

STATUS: COMPLETED
```
