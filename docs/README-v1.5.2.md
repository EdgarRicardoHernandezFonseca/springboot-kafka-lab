# Spring Boot Kafka Lab

# Version 1.5.2 – Horizontal Scaling with Multiple Spring Boot Instances

---

# Objective

The goal of this version is to demonstrate how Apache Kafka enables horizontal scalability by running multiple instances of the same Spring Boot application.

Unlike the previous version, where multiple consumers were running inside a single JVM, this experiment uses two completely independent Spring Boot processes.

This is the same architecture commonly used in production environments deployed on Kubernetes, Docker Swarm, ECS, or virtual machines.

---

# Architecture

```
                 Producer

                    │
                    ▼

              Kafka Topic
             +------------+
             | orders     |
             | Partition0 |
             | Partition1 |
             | Partition2 |
             +------------+

               │          │

      ┌────────┘          └─────────┐

      ▼                            ▼

Spring Boot Instance 1      Spring Boot Instance 2
      Port 8080                  Port 8082

        Consumer Group: order-group
```

Both applications belong to the same Consumer Group.

Kafka automatically distributes partitions among the running instances.

---

# Running Multiple Instances

Instance 1

```
Port: 8080
```

Instance 2

```
Port: 8082
```

The second instance only changes its server port.

Example:

```properties
server.port=8082
```

Everything else remains exactly the same.

No Kafka configuration changes are required.

---

# Sending Messages

Several order events were published using the REST endpoint.

Example:

```
POST /orders
```

Example payload

```json
{
  "orderId": 1016,
  "customerName": "N",
  "totalAmount": 250000
}
```

Kafka distributed the records across the topic partitions.

---

# Load Distribution

After both applications joined the Consumer Group, Kafka assigned partitions automatically.

Example assignment

```
Instance 8080

orders-2
```

```
Instance 8082

orders-0
orders-1
```

The assignment is managed by the Group Coordinator.

No manual configuration is necessary.

---

# Simulating a Failure

The second Spring Boot instance (port 8082) was stopped.

Kafka detected that the consumer left the group.

Console log:

```
Member sending LeaveGroup request...
```

Immediately afterward, Kafka started a rebalance.

```
Request joining group due to:
group is already rebalancing
```

The remaining instance received all partitions.

```
orders-0
orders-1
orders-2
```

No producer changes were required.

The application continued consuming messages without interruption.

---

# Restarting the Second Instance

After restarting the application on port 8082:

```
Successfully joined group
```

Kafka executed another rebalance.

The partitions were redistributed between both instances again.

Example:

```
Instance 8080

orders-2
```

```
Instance 8082

orders-0
orders-1
```

This demonstrates Kafka's dynamic load balancing capabilities.

---

# Key Concepts Learned

- Horizontal Scaling
- Multiple Spring Boot instances
- Consumer Groups
- Dynamic Partition Assignment
- Group Coordinator
- Automatic Failover
- Load Balancing
- High Availability
- Fault Tolerance
- Zero Producer Changes During Scaling

---

# Real-World Use Cases

This architecture is widely used in production environments.

Examples include:

- Kubernetes Deployments
- Docker Swarm
- Amazon ECS
- Azure Container Apps
- Google Kubernetes Engine (GKE)
- Red Hat OpenShift

Increasing the number of application replicas automatically increases message processing capacity without modifying the producer application.

---

# Conclusion

Version 1.5.2 demonstrates one of Apache Kafka's most important production features: horizontal scalability.

By running multiple Spring Boot instances within the same Consumer Group, Kafka automatically distributes partitions, balances the workload, detects failures, and reassigns partitions whenever consumers join or leave the group.

This allows distributed applications to scale efficiently while maintaining high availability and fault tolerance without requiring any changes to the producer code.

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