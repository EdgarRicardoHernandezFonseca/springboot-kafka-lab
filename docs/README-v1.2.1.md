# README-v1.2.1.md

# Kafka Dual Listener Configuration

## Overview

One of the most common networking issues when running Apache Kafka inside Docker is that different clients connect to Kafka from different network environments.

For example:

- Spring Boot runs directly on the host machine.
- Kafka UI runs inside the Docker network.
- Kafka itself runs inside Docker.

Although all of them communicate with the same Kafka broker, they cannot use the same network address.

This laboratory solves this problem by configuring **two Kafka listeners**.

---

# Why is a Dual Listener Required?

When a Kafka client connects, the process occurs in two steps.

### Step 1

The client connects using the bootstrap server.

Example:

```
localhost:9092
```

### Step 2

Kafka responds with its metadata.

The metadata contains the broker address that the client must use for all future communication.

For example:

```
Broker:
kafka:9093
```

or

```
Broker:
localhost:9092
```

If Kafka advertises an address that the client cannot reach, the connection fails.

Typical errors include:

```
UnknownHostException
```

```
TimeoutException
```

```
Node disconnected
```

---

# Laboratory Architecture

```
                   Host Machine

        Spring Boot
             │
             │ localhost:9092
             ▼

        +-----------------------+
        |       Kafka Broker    |
        |                       |
        | HOST Listener         |
        | localhost:9092        |
        |                       |
        | Docker Listener       |
        | kafka:9093            |
        +-----------------------+
                  ▲
                  │
                  │ kafka:9093
                  │
           Kafka UI (Docker)
```

---

# Listener Configuration

## Host Listener

```
PLAINTEXT_HOST://localhost:9092
```

Used by:

- Spring Boot
- Postman
- Kafka CLI running on the host

---

## Docker Listener

```
PLAINTEXT://kafka:9093
```

Used by:

- Kafka UI
- Docker containers
- Future microservices running inside Docker

---

# Docker Compose Configuration

## Listeners

```yaml
KAFKA_LISTENERS: >
  PLAINTEXT://:9093,
  PLAINTEXT_HOST://:9092,
  CONTROLLER://:9094
```

---

## Advertised Listeners

```yaml
KAFKA_ADVERTISED_LISTENERS: >
  PLAINTEXT://kafka:9093,
  PLAINTEXT_HOST://localhost:9092
```

---

# Why are Advertised Listeners Important?

Kafka clients do not continue using the bootstrap server after connecting.

Instead, they use the addresses returned by Kafka itself.

For this reason:

- Docker clients must receive

```
kafka:9093
```

while

- Host applications must receive

```
localhost:9092
```

Otherwise, clients will attempt to connect to an address that does not exist in their network.

---

# Benefits of the Dual Listener Architecture

This configuration allows:

- Spring Boot to connect from the host machine.
- Kafka UI to connect from Docker.
- Future Docker microservices to connect without configuration changes.
- Local development without modifying the application.
- Production-like networking behavior.

---

# Common Errors

| Error | Cause |
|--------|-------|
| UnknownHostException | Kafka advertised an unreachable hostname. |
| TimeoutException | Client cannot reach the advertised listener. |
| Kafka UI Offline | Bootstrap server points to the wrong listener. |
| Spring Boot disconnects | Kafka advertises the Docker hostname instead of localhost. |

---

# Version

Laboratory Version

```
v1.2.1
```

Topic

```
Kafka Dual Listener Configuration
```

---

# References

Apache Kafka Documentation

https://kafka.apache.org/documentation/

Kafka Listeners Explained

https://rmoff.net/2018/08/02/kafka-listeners-explained/

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