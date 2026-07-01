# Kafka Documentation - Complete Guide

## Overview

This directory contains comprehensive documentation for Apache Kafka and Spring Kafka integration in our microservices architecture. The documentation is organized to support learning Kafka from fundamentals through advanced patterns.

---

## Documentation Structure

### Core Concepts
1. **[Kafka Fundamentals](./01-kafka-fundamentals.md)** - Core Kafka concepts, architecture, topics, partitions, replication
2. **[Kafka Configuration](./02-kafka-configuration.md)** - Broker, producer, consumer, and Spring Kafka configuration
3. **[Producers & Consumers](./03-producers-consumers.md)** - Publishing and consuming messages with Spring Kafka

### Advanced Topics
4. **[Kafka Streams](./04-kafka-streams.md)** - Stream processing, joins, aggregations, state stores
5. **[Error Handling](./05-error-handling.md)** - Retries, dead letter queues, poison pills, fault tolerance
6. **[Testing Strategies](./06-testing-strategies.md)** - Unit testing with EmbeddedKafka, integration testing

### Patterns & Best Practices
7. **[Event-Driven Patterns](./07-event-driven-patterns.md)** - SAGA, Event Sourcing, CQRS, choreography vs orchestration
8. **[Idempotency & Transactions](./08-idempotency-transactions.md)** - Exactly-once semantics, transactional producers
9. **[Performance Tuning](./09-performance-tuning.md)** - Throughput optimization, batching, compression
10. **[Monitoring & Observability](./10-monitoring-observability.md)** - Metrics, logging, distributed tracing

---

## Quick Navigation by Use Case

### I'm just getting started with Kafka
Start here:
1. [Kafka Fundamentals](./01-kafka-fundamentals.md) - Understand topics, partitions, offsets
2. [Kafka Configuration](./02-kafka-configuration.md) - Set up your first producer/consumer
3. [Producers & Consumers](./03-producers-consumers.md) - Send and receive your first message

### I need to implement Kafka in our order-service
Read these in order:
1. [Kafka Configuration](./02-kafka-configuration.md) - Spring Kafka setup
2. [Producers & Consumers](./03-producers-consumers.md) - KafkaTemplate and @KafkaListener
3. [Error Handling](./05-error-handling.md) - Production-ready error handling

### I'm implementing the SAGA pattern
Focus on:
1. [Kafka Streams](./04-kafka-streams.md) - Stream joins for response coordination
2. [Event-Driven Patterns](./07-event-driven-patterns.md) - SAGA orchestration patterns
3. [Idempotency & Transactions](./08-idempotency-transactions.md) - Reliable distributed transactions

### I need to test Kafka code
See:
1. [Testing Strategies](./06-testing-strategies.md) - EmbeddedKafka, test containers, mocking

### I'm troubleshooting production issues
Check:
1. [Error Handling](./05-error-handling.md) - Common failure scenarios
2. [Monitoring & Observability](./10-monitoring-observability.md) - Metrics and debugging
3. [Performance Tuning](./09-performance-tuning.md) - Latency and throughput issues

---

## Project-Specific Context

### Our Architecture
This project implements a **SAGA pattern** for distributed transactions across three microservices:
- **order-service** - Orchestrator (Kafka Streams joins)
- **payment-service** - Participant (reserves/commits funds)
- **stock-service** - Participant (reserves/commits inventory)

### Current Status
- **Phase 1 Complete:** REST API with DTOs and validation
- **Phase 2 In Progress:** Error handling + Kafka producer setup
- **Phase 3 Upcoming:** Kafka Streams implementation

### Reference Implementation
We are following the architecture from [piomin/sample-spring-kafka-microservices](https://github.com/piomin/sample-spring-kafka-microservices).

---

## Learning Path Recommendations

### Week 1: Kafka Basics
- Read [Kafka Fundamentals](./01-kafka-fundamentals.md)
- Complete exercises: Create topics, produce/consume messages manually
- Lab: Use Kafka UI (localhost:8080) to explore topics

### Week 2: Spring Kafka Integration
- Read [Kafka Configuration](./02-kafka-configuration.md) and [Producers & Consumers](./03-producers-consumers.md)
- Implement: Add KafkaTemplate to order-service
- Lab: Publish OrderCreated events to `orders` topic

### Week 3: Kafka Streams & SAGA
- Read [Kafka Streams](./04-kafka-streams.md) and [Event-Driven Patterns](./07-event-driven-patterns.md)
- Implement: Stream joins for payment + stock responses
- Lab: Build state store for queryable orders

### Week 4: Production Readiness
- Read [Error Handling](./05-error-handling.md), [Testing](./06-testing-strategies.md), [Monitoring](./10-monitoring-observability.md)
- Implement: Retry logic, DLQ, unit tests
- Lab: Simulate failures and verify compensation logic

---

## Prerequisites

Before diving into these docs, ensure you have:
- Java 17+ installed
- Docker Compose running (Kafka cluster)
- Basic understanding of Spring Boot (see `../02-spring-boot/`)
- Familiarity with REST APIs and JSON

## Tools You'll Use

- **Kafka CLI** - Command-line tools for topic management (`kafka-topics.sh`, `kafka-console-producer.sh`)
- **Kafka UI** - Web interface at http://localhost:8080 for visualizing topics, messages, consumer groups
- **Spring Kafka** - Spring Boot integration library (`KafkaTemplate`, `@KafkaListener`)
- **Kafka Streams** - Stream processing library (DSL for joins, aggregations, state stores)

---

## Getting Help

- **Official Docs:** [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- **Spring Kafka Docs:** [Spring for Apache Kafka](https://docs.spring.io/spring-kafka/reference/)
- **Kafka Streams:** [Kafka Streams Developer Guide](https://kafka.apache.org/documentation/streams/)
- **Project-Specific:** See `../ARCHITECTURE-OVERVIEW.md` for our implementation details

---

## Contributing to This Documentation

As you learn, please enhance these docs:
- Add real code examples from our services
- Document gotchas and debugging tips
- Include diagrams (Mermaid format)
- Update with production lessons learned

See `../DECISION.md` for architectural decisions and `../PROJECT-PLAN.md` for the implementation roadmap.

---

## Next Steps

1. Start with [Kafka Fundamentals](./01-kafka-fundamentals.md) if you're new to Kafka
2. Jump to [Kafka Configuration](./02-kafka-configuration.md) if you're ready to integrate
3. Check [Event-Driven Patterns](./07-event-driven-patterns.md) for SAGA pattern details

Happy learning!
