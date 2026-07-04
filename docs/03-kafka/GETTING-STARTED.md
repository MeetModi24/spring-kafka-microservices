# Getting Started with Kafka - Learning Path

## Welcome!

This guide provides a structured learning path through the Kafka documentation. Follow these steps to go from beginner to implementing production-ready Kafka microservices.

---

## Prerequisites

Before starting, ensure you have:
- ✅ Java 17+ installed
- ✅ Docker and Docker Compose running
- ✅ Basic Spring Boot knowledge (see `../02-spring-boot/`)
- ✅ Understanding of REST APIs and JSON

**Verify setup:**
```bash
# Check Java
java --version

# Check Docker
docker --version
docker compose version

# Start Kafka
cd /Users/mhiteshkumar/spring-kafka-microservices
docker compose up -d

# Verify services
docker ps
# Should show: zookeeper, kafka, kafka-ui

# Access Kafka UI
open http://localhost:8080
```

---

## Learning Path

### Phase 1: Fundamentals (Week 1) - Understand Kafka Core Concepts

#### Day 1-2: Core Concepts
**Read:**
- [Kafka Fundamentals](./01-kafka-fundamentals.md)
  - Topics, partitions, offsets
  - Producers and consumers
  - Consumer groups
  - Replication

**Hands-On:**
```bash
# Create a topic
docker exec -it kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --topic test-topic --partitions 3

# Produce messages
docker exec -it kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic test-topic

# Consume messages
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic test-topic \
  --from-beginning
```

**Lab:** Explore Kafka UI (http://localhost:8080)
- Create topics
- Send messages
- View consumer groups

#### Day 3-4: Configuration
**Read:**
- [Kafka Configuration](./02-kafka-configuration.md)
  - Producer configuration (acks, retries, serializers)
  - Consumer configuration (group-id, offsets, deserializers)
  - Spring Kafka setup

**Hands-On:**
```yaml
# Add to application.yml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
    consumer:
      group-id: my-service
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        '[spring.json.trusted.packages]': com.example.*
```

#### Day 5-7: Producers & Consumers
**Read:**
- [Producers & Consumers](./03-producers-consumers.md)
  - KafkaTemplate usage
  - @KafkaListener annotation
  - Message keys and partitioning
  - Manual offset commits

**Hands-On:** Implement in order-service
```java
// Producer
@Service
public class OrderProducer {
    @Autowired
    private KafkaTemplate<String, Order> kafkaTemplate;
    
    public void sendOrder(Order order) {
        kafkaTemplate.send("orders", order.getOrderId(), order)
            .whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("Sent: {}", order.getOrderId());
                } else {
                    log.error("Failed: {}", order.getOrderId(), ex);
                }
            });
    }
}

// Consumer
@Service
public class PaymentConsumer {
    @KafkaListener(topics = "orders", groupId = "payment-service")
    public void consume(Order order) {
        log.info("Received: {}", order.getOrderId());
        processOrder(order);
    }
}
```

**Lab:**
1. Create `orders` topic
2. Publish OrderCreated event from order-service
3. Consume in payment-service
4. Verify in Kafka UI

---

### Phase 2: Advanced Patterns (Week 2) - Event-Driven Architecture

#### Day 8-10: Event-Driven Patterns
**Read:**
- [Event-Driven Patterns](./07-event-driven-patterns.md)
  - SAGA pattern (choreography vs orchestration)
  - Event sourcing
  - CQRS
  - Outbox pattern

**Focus:** SAGA Orchestration (our project uses this)

**Understand the flow:**
```
1. order-service creates Order (status=NEW)
2. order-service publishes OrderCreated to "orders" topic
3. payment-service listens → reserves funds → publishes to "payment-orders"
4. stock-service listens → reserves inventory → publishes to "stock-orders"
5. order-service joins responses → decides CONFIRMED or ROLLBACK
6. order-service publishes final decision to "orders" topic
7. payment & stock services listen → commit or compensate
```

**Hands-On:** Draw sequence diagram for your order flow

#### Day 11-12: Error Handling
**Read:**
- [Error Handling](./05-error-handling.md)
  - Retry strategies
  - Dead Letter Queue (DLQ)
  - Poison pills
  - Circuit breaker

**Hands-On:** Add error handling to consumers
```java
@KafkaListener(topics = "orders", groupId = "payment-service")
@RetryableTopic(
    attempts = "3",
    backoff = @Backoff(delay = 1000, multiplier = 2)
)
public void consume(Order order) {
    if (shouldFail(order)) {
        throw new RetriableException("Temporary failure");
    }
    processOrder(order);
}

@DltHandler
public void handleDlt(Order order) {
    log.error("Order {} sent to DLT", order.getOrderId());
}
```

**Lab:**
1. Simulate failures (throw exceptions)
2. Verify retry behavior
3. Check DLT topic in Kafka UI

#### Day 13-14: Quick Reference
**Read:**
- [Kafka Quick Reference](./KAFKA-QUICK-REFERENCE.md)
  - Common commands
  - Code snippets
  - Troubleshooting

**Hands-On:** Bookmark this page for quick lookups

---

### Phase 3: Implementation (Week 3) - Build the SAGA

#### Milestone 1: Basic Event Flow
1. ✅ order-service publishes OrderCreated
2. ✅ payment-service consumes and processes
3. ✅ stock-service consumes and processes
4. ✅ Verify in Kafka UI

**Topics to create:**
```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic orders --partitions 3
  
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic payment-orders --partitions 3
  
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic stock-orders --partitions 3
```

#### Milestone 2: Response Handling
1. ✅ payment-service publishes PaymentReserved/Rejected
2. ✅ stock-service publishes InventoryReserved/Rejected
3. ✅ order-service consumes responses
4. ✅ Log final status

#### Milestone 3: Kafka Streams Join (Advanced)
**Note:** This is the most complex part. Read these resources:
- [Kafka Streams documentation](https://kafka.apache.org/documentation/streams/)
- Reference implementation: [piomin/sample-spring-kafka-microservices](https://github.com/piomin/sample-spring-kafka-microservices)

**Goal:** Join payment and stock responses in order-service

#### Milestone 4: Compensation Logic
1. ✅ Implement rollback in payment-service
2. ✅ Implement rollback in stock-service
3. ✅ Test failure scenarios

---

## Key Topics Summary

### Must-Know Topics
| Topic | Importance | When You'll Use It |
|---|---|---|
| Topics & Partitions | ⭐⭐⭐⭐⭐ | Day 1 |
| Producers & Consumers | ⭐⭐⭐⭐⭐ | Week 1 |
| Consumer Groups | ⭐⭐⭐⭐⭐ | Week 1 |
| Manual Commits | ⭐⭐⭐⭐ | Week 2 |
| Error Handling | ⭐⭐⭐⭐⭐ | Week 2 |
| SAGA Pattern | ⭐⭐⭐⭐ | Week 3 |
| Idempotency | ⭐⭐⭐⭐ | Week 3 |

### Optional Topics (Future Learning)
- Kafka Streams (for joins) - Week 4+
- Kafka Connect - If you need ETL
- Schema Registry (Avro) - For schema evolution
- Kafka Security (SSL, SASL) - For production

---

## Common Pitfalls & How to Avoid Them

### 1. Not Using Manual Commits
**Problem:** Auto-commit commits before processing, losing messages on failure.

**Solution:**
```yaml
spring:
  kafka:
    consumer:
      enable-auto-commit: false
    listener:
      ack-mode: manual
```

```java
@KafkaListener(topics = "orders", groupId = "payment-service")
public void consume(Order order, Acknowledgment ack) {
    try {
        processOrder(order);
        ack.acknowledge();  // Commit only on success
    } catch (Exception e) {
        // Don't commit → message will be redelivered
    }
}
```

### 2. Ignoring Idempotency
**Problem:** Redelivered messages processed twice (duplicate charges).

**Solution:** Track processed message IDs
```java
if (processedRepo.existsById(order.getOrderId())) {
    log.info("Already processed: {}", order.getOrderId());
    ack.acknowledge();
    return;
}
```

### 3. Blocking Partition on Poison Pills
**Problem:** One bad message blocks entire partition.

**Solution:** Send to DLQ after N attempts
```java
@RetryableTopic(attempts = "3")
public void consume(Order order) {
    processOrder(order);
}

@DltHandler
public void handleDlt(Order order) {
    log.error("Sent to DLT: {}", order.getOrderId());
}
```

### 4. Wrong Advertised Listeners
**Problem:** Spring Boot can't connect to Kafka.

**Error:** `TimeoutException: Topic orders not present in metadata`

**Solution:** Check docker-compose.yml
```yaml
KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:29092,EXTERNAL://localhost:9092
# Spring Boot uses localhost:9092
```

### 5. Deserialization Errors
**Problem:** `The class 'com.example.Order' is not in the trusted packages`

**Solution:**
```yaml
spring:
  kafka:
    consumer:
      properties:
        '[spring.json.trusted.packages]': com.example.*
```

---

## Testing Your Knowledge

### Quiz 1: Basics
1. What is a partition? Why are partitions important?
2. What is a consumer group? How does Kafka distribute messages?
3. What happens if a consumer crashes?

### Quiz 2: Configuration
1. What does `acks=all` mean?
2. When should you use `auto-offset-reset: earliest` vs `latest`?
3. Why is `enable-idempotence: true` important?

### Quiz 3: Patterns
1. What is the difference between choreography and orchestration?
2. How does the SAGA pattern handle failures?
3. What is a compensation transaction?

**Answers:** See respective documentation sections

---

## Troubleshooting Checklist

When something doesn't work:

**Step 1: Verify Kafka is running**
```bash
docker ps
# Should show: zookeeper, kafka, kafka-ui
```

**Step 2: Check topic exists**
```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
```

**Step 3: Check consumer group**
```bash
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group payment-service
```

**Step 4: Check application logs**
```bash
# In order-service directory
mvn spring-boot:run
# Look for Kafka connection errors
```

**Step 5: Use Kafka UI**
- Open http://localhost:8080
- Check topics, messages, consumer groups

---

## Resources

### Project-Specific
- [Architecture Overview](../ARCHITECTURE-OVERVIEW.md)
- [Project Plan](../PROJECT-PLAN.md)
- [Spring Boot Fundamentals](../02-spring-boot/spring-framework-fundamentals.md)

### External Resources
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Spring Kafka Reference](https://docs.spring.io/spring-kafka/reference/)
- [Confluent Kafka Guide](https://docs.confluent.io/platform/current/kafka/introduction.html)
- [Reference Repository](https://github.com/piomin/sample-spring-kafka-microservices)

### Books
- "Kafka: The Definitive Guide" by Neha Narkhede (O'Reilly)
- "Designing Data-Intensive Applications" by Martin Kleppmann

---

## Next Steps

Once you've completed this learning path:

1. ✅ Implement order-service Kafka producer
2. ✅ Implement payment-service Kafka consumer
3. ✅ Implement stock-service Kafka consumer
4. ✅ Add error handling (retries, DLQ)
5. ✅ Implement SAGA orchestration (Kafka Streams joins)
6. ✅ Add compensation logic
7. ✅ Write integration tests
8. ✅ Add monitoring (metrics, logs)

---

## Need Help?

- **Check [Quick Reference](./KAFKA-QUICK-REFERENCE.md)** for code snippets
- **Review [Error Handling](./05-error-handling.md)** for troubleshooting
- **See [README](./README.md)** for navigation
- **Check Kafka UI** (http://localhost:8080) for message inspection

---

**Good luck with your Kafka learning journey!** 🚀

Remember: Start simple (producer + consumer), then add complexity (error handling, SAGA). Don't try to implement everything at once.
