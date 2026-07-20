# Spring Boot Kafka Lab
# v1.5.1 – Consumer Group Rebalancing

## Objective

This version focuses on one of Apache Kafka's most important mechanisms for fault tolerance and scalability:

- Consumer Group Rebalancing
- Dynamic Partition Redistribution
- Group Coordinator
- Heartbeats

The goal is to demonstrate how Kafka automatically redistributes partitions whenever consumers join or leave a consumer group.

---

# What is Consumer Rebalancing?

Consumer Rebalancing is the process by which Kafka redistributes topic partitions among consumers in the same consumer group.

A rebalance is triggered whenever the membership of the group changes.

Typical events include:

- A new consumer joins the group
- A consumer shuts down
- A consumer crashes
- A consumer stops sending heartbeats

Kafka temporarily pauses message consumption, calculates a new partition assignment, and then resumes processing.

---

# Initial Consumer Group

Example:

```
Topic: orders

Partitions

P0
P1
P2

Consumer Group

order-group

Consumer A -> P0

Consumer B -> P1

Consumer C -> P2
```

Each partition is owned by exactly one consumer.

---

# Rebalance When a Consumer Leaves

If Consumer C is stopped, Kafka performs a rebalance.

Before

```
Consumer A -> P0

Consumer B -> P1

Consumer C -> P2
```

After

```
Consumer A -> P0

Consumer B -> P1

Consumer B -> P2
```

No messages are lost.

Kafka automatically redistributes the partitions.

---

# Rebalance When a Consumer Joins

If a new consumer joins the group:

Before

```
Consumer A -> P0

Consumer B -> P1

Consumer B -> P2
```

After

```
Consumer A -> P0

Consumer B -> P1

Consumer D -> P2
```

Kafka balances the workload automatically.

---

# The Group Coordinator

Each Consumer Group has a Group Coordinator.

The coordinator is responsible for:

- Tracking active consumers
- Detecting failed consumers
- Triggering rebalances
- Assigning partitions
- Managing committed offsets

Consumers communicate continuously with the coordinator while running.

---

# Heartbeats

Kafka consumers periodically send Heartbeats to the Group Coordinator.

Purpose:

- Indicate that the consumer is still alive
- Maintain membership in the consumer group
- Prevent unnecessary rebalances

If heartbeats stop arriving before the configured session timeout, Kafka assumes that the consumer has failed and removes it from the group.

This automatically triggers a rebalance.

---

# Rebalance Lifecycle

The rebalance process typically follows these steps:

```
Consumer joins or leaves

↓

Group Coordinator detects membership change

↓

Partitions are revoked

↓

New partition assignment is calculated

↓

Consumers synchronize

↓

New partitions are assigned

↓

Message processing resumes
```

---

# Example Log Messages

During this lab, the following Kafka log messages can be observed:

```
(Re-)joining group

Successfully joined group

Finished assignment

Partitions revoked

Partitions assigned

Successfully synced group
```

These messages confirm that the rebalance process completed successfully.

---

# Kafka UI Verification

Kafka UI can be used to monitor:

- Consumer Groups
- Active Members
- Assigned Partitions
- Consumer Lag
- Offsets

Example:

```
Consumer Group

order-group

Members

3

Assigned Partitions

3

Lag

0
```

A lag of zero indicates that consumers are processing messages correctly after the rebalance.

---

# What was learned

This version demonstrates:

- Consumer Group membership
- Dynamic Consumer Rebalancing
- Automatic Partition Redistribution
- Group Coordinator responsibilities
- Heartbeat mechanism
- Fault tolerance
- Horizontal scalability
- High availability in Kafka consumers

Understanding Consumer Rebalancing is essential for designing reliable event-driven microservices capable of handling failures and scaling automatically.

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