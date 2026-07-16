# Spring Boot Kafka Lab v1.0.0

# Hello Kafka – Basic Infrastructure

## Objective

The objective of this first version is to build a minimal Apache Kafka environment using Docker and verify that the broker starts successfully.

At this stage, there is no producer or consumer application. The goal is simply to understand the infrastructure required to run Kafka locally.

---

# What is Apache Kafka?

Apache Kafka is an open-source distributed event streaming platform designed to exchange data between applications through asynchronous messages.

Instead of applications communicating directly with each other, they exchange events through Kafka.

```
Application A

      │

      ▼

Apache Kafka

      │

      ▼

Application B
```

Kafka acts as an intermediary that stores and distributes messages efficiently.

---

# Why is Kafka needed?

Kafka solves several common problems in distributed systems:

- asynchronous communication
- loose coupling between applications
- high throughput
- scalability
- fault tolerance
- reliable message storage
- event-driven architecture

Instead of calling another application directly, a service publishes an event to Kafka.

Other applications can process the event whenever they are ready.

---

# Laboratory Architecture

The initial laboratory contains only one component.

```
+-----------------------+
|       Docker          |
|                       |
|   +---------------+   |
|   | Apache Kafka  |   |
|   +---------------+   |
+-----------------------+
```

The objective is simply to have a running Kafka broker.

---

# Why Docker?

Docker allows Kafka to run inside an isolated container without installing Kafka directly on the operating system.

Advantages include:

- simple installation
- isolated environment
- reproducible configuration
- easy cleanup
- portable development environment

The entire infrastructure is defined in a single `docker-compose.yml` file.

---

# Basic Infrastructure

The first version contains a minimal Docker Compose configuration.

```
services:

  kafka:

    image: apache/kafka:4.2.1

    container_name: kafka

    ports:
      - "9092:9092"
```

This configuration creates one Kafka broker that listens on port **9092**.

---

# Starting the Infrastructure

The Kafka container is started with:

```bash
docker compose up -d
```

Docker downloads the image (only the first time) and starts the Kafka broker in the background.

---

# Verifying the Container

The running containers can be verified using:

```bash
docker ps
```

Expected output:

```
CONTAINER ID

IMAGE

apache/kafka:4.2.1

STATUS

Up
```

The **Up** status confirms that the broker is running correctly.

---

# Verifying Kafka

Kafka provides command-line utilities for administration.

One way to verify that the broker is available is to enter the container:

```bash
docker exec -it kafka bash
```

From inside the container, Kafka CLI tools are available under:

```text
/opt/kafka/bin
```

These tools allow you to:

- create topics
- list topics
- publish messages
- consume messages
- inspect broker metadata

---

# What Has Been Achieved?

At the end of version **v1.0.0**, the laboratory has:

- Docker installed
- Apache Kafka running
- Kafka broker available on port 9092
- Kafka CLI tools ready to use

Although no messages are exchanged yet, the messaging infrastructure is fully operational.

---

# What's Next?

The next version introduces the first Kafka client.

```
Spring Boot Producer

        │

        ▼

Apache Kafka
```

The producer will publish messages to Kafka, allowing us to begin exploring asynchronous communication.

---

# Version Summary

Version **v1.0.0** focuses entirely on infrastructure.

The goal is to understand how to start a Kafka broker using Docker and verify that the environment is ready before introducing producers and consumers.

This version provides the foundation for all subsequent laboratory exercises.