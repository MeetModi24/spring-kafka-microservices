# SAGA Orchestration Architecture

> **Document Type:** Architecture Design Document (ADD)  
> **Version:** 1.0  
> **Last Updated:** July 2026  
> **Status:** Phase 4 Implementation Guide

---

## Table of Contents

1. [Overview](#overview)
2. [SAGA Pattern Theory](#saga-pattern-theory)
3. [Event Flow Architecture](#event-flow-architecture)
4. [State Management Design](#state-management-design)
5. [Idempotency Implementation](#idempotency-implementation)
6. [Error Handling Strategies](#error-handling-strategies)
7. [Trade-offs and Best Practices](#trade-offs-and-best-practices)
8. [Production Considerations](#production-considerations)

---

## Overview

### What is SAGA Orchestration?

**SAGA** is a design pattern for managing **distributed transactions** across microservices without using two-phase commit (2PC). Instead of locking resources across services, SAGA breaks a distributed transaction into a series of **local transactions**, each with a **compensating transaction** to undo changes if the overall transaction fails.

### Why SAGA?

Traditional distributed transactions (2PC) don't scale in microservices architectures because:

1. **Tight Coupling:** All services must participate in the same transaction
2. **Reduced Availability:** Any service failure blocks the entire transaction
3. **Database Lock Contention:** Locks held across services reduce throughput
4. **Not Cloud-Native:** Many cloud databases don't support distributed transactions

SAGA solves these problems by accepting **eventual consistency** instead of immediate consistency.

---

## SAGA Pattern Theory

### Two SAGA Variants

#### 1. Choreography (Event-Driven)

**How it works:**
- Each service publishes events after completing its local transaction
- Other services listen to events and react autonomously
- No central coordinator

**Pros:**
- Loose coupling
- No single point of failure
- Scalable (services don't wait for each other)

**Cons:**
- Hard to understand overall flow
- Difficult to debug (events scattered across services)
- Cyclic dependencies possible

**When to use:**
- Simple workflows (2-3 services)
- Services owned by different teams
- High autonomy requirements

---

#### 2. Orchestration (Command-Driven)

**How it works:**
- Central orchestrator tells each service what to do
- Orchestrator maintains SAGA state
- Services execute commands and return results

**Pros:**
- Clear workflow visibility
- Easier to debug (single place to look)
- Centralized state management

**Cons:**
- Single point of failure (orchestrator)
- Orchestrator can become complex
- Tight coupling to orchestrator

**When to use:**
- Complex workflows (4+ services)
- Need centralized monitoring/auditing
- Workflow changes frequently

---

### Our Implementation: Hybrid Approach

We use **choreography with implicit orchestration**:

- **Choreography:** Services publish events (not commands)
- **Orchestration:** order-service acts as coordinator by publishing decision events
- **Autonomy:** Services react to events independently

**Why hybrid?**
- Keeps loose coupling (choreography)
- Provides clear decision point (orchestration)
- Avoids complex orchestrator logic
- Scales better than pure orchestration

---

## Event Flow Architecture

### Phase 4 Event Flow (2 Services)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     SAGA EVENT FLOW DIAGRAM (PHASE 4)                        │
└─────────────────────────────────────────────────────────────────────────────┘

Actors:
  [Client] - REST API consumer (Postman, frontend, etc.)
  [OS] - order-service (REST API + Kafka producer/consumer + orchestrator)
  [PS] - payment-service (Kafka consumer + producer)
  [K] - Kafka broker (message bus)

═══════════════════════════════════════════════════════════════════════════════
HAPPY PATH: Payment Accepted
═══════════════════════════════════════════════════════════════════════════════

[Client]                [OS]                [K]                [PS]
   │                     │                   │                   │
   │ POST /orders        │                   │                   │
   ├────────────────────>│                   │                   │
   │                     │                   │                   │
   │                     │ 1. Create Order   │                   │
   │                     │    status=PENDING │                   │
   │                     │ 2. Save to DB     │                   │
   │                     │ 3. Store in       │                   │
   │                     │    StateStore     │                   │
   │                     │                   │                   │
   │                     │ OrderCreatedEvent │                   │
   │                     ├──────────────────>│                   │
   │                     │   (order-events)  │                   │
   │                     │                   │                   │
   │ 201 Created         │                   │ OrderCreatedEvent │
   │<────────────────────┤                   ├──────────────────>│
   │ orderId=abc-123     │                   │                   │
   │                     │                   │                   │
   │                     │                   │                   │ 4. Find customer
   │                     │                   │                   │ 5. Check balance
   │                     │                   │                   │ 6. Reserve funds
   │                     │                   │                   │    available→reserved
   │                     │                   │                   │ 7. Save customer
   │                     │                   │                   │
   │                     │                   │ PaymentProcessedEvent
   │                     │                   │<──────────────────┤
   │                     │                   │  (payment-events) │
   │                     │                   │  status=ACCEPT    │
   │                     │                   │                   │
   │                     │ PaymentProcessedEvent                 │
   │                     │<──────────────────┤                   │
   │                     │                   │                   │
   │                     │ 8. Get order from │                   │
   │                     │    StateStore     │                   │
   │                     │ 9. Decision:      │                   │
   │                     │    CONFIRMED      │                   │
   │                     │ 10. Update order  │                   │
   │                     │     status        │                   │
   │                     │ 11. Mark processed│                   │
   │                     │     (idempotency) │                   │
   │                     │                   │                   │
   │                     │ FinalDecisionEvent│                   │
   │                     ├──────────────────>│                   │
   │                     │ (order-events)    │                   │
   │                     │  SAME TOPIC!      │                   │
   │                     │ status=CONFIRMED  │                   │
   │                     │                   │                   │
   │                     │                   │ FinalDecisionEvent│
   │                     │                   ├──────────────────>│
   │                     │                   │                   │
   │                     │                   │                   │ 12. Find customer
   │                     │                   │                   │ 13. Confirm payment
   │                     │                   │                   │     reserved→deducted
   │                     │                   │                   │ 14. Save customer
   │                     │                   │                   │ 15. Mark processed
   │                     │                   │                   │
   │ GET /orders/abc-123 │                   │                   │
   ├────────────────────>│                   │                   │
   │                     │ 16. Get from      │                   │
   │                     │     StateStore    │                   │
   │ 200 OK              │                   │                   │
   │<────────────────────┤                   │                   │
   │ status=CONFIRMED    │                   │                   │
   │                     │                   │                   │

═══════════════════════════════════════════════════════════════════════════════
REJECTION PATH: Insufficient Balance
═══════════════════════════════════════════════════════════════════════════════

[Client]                [OS]                [K]                [PS]
   │                     │                   │                   │
   │ POST /orders        │                   │                   │
   ├────────────────────>│                   │                   │
   │                     │                   │                   │
   │                     │ OrderCreatedEvent │                   │
   │                     ├──────────────────>│──────────────────>│
   │ 201 Created         │                   │                   │
   │<────────────────────┤                   │                   │
   │ status=PENDING      │                   │                   │
   │                     │                   │                   │ balance < amount
   │                     │                   │                   │ ❌ REJECT
   │                     │                   │                   │
   │                     │                   │ PaymentProcessedEvent
   │                     │                   │<──────────────────┤
   │                     │ PaymentProcessedEvent status=REJECT   │
   │                     │<──────────────────┤ reason="Insufficient"
   │                     │                   │                   │
   │                     │ Decision: REJECTED│                   │
   │                     │                   │                   │
   │                     │ FinalDecisionEvent│                   │
   │                     ├──────────────────>│──────────────────>│
   │                     │ status=REJECTED   │                   │
   │                     │                   │                   │
   │                     │                   │                   │ Nothing to rollback
   │                     │                   │                   │ (never reserved)
   │ GET /orders/abc-123 │                   │                   │
   ├────────────────────>│                   │                   │
   │ 200 OK              │                   │                   │
   │<────────────────────┤                   │                   │
   │ status=REJECTED     │                   │                   │
```

---

### Phase 5 Event Flow (3 Services - Future)

In Phase 5, we add **stock-service** and implement **partial rollback** logic:

```
Scenario: Payment Accepts, Stock Rejects → Rollback Payment

order-service publishes:
  FinalDecisionEvent {
    status: ROLLBACK,
    source: "stock"  ← Indicates stock caused failure
  }

payment-service receives decision:
  if (status == ROLLBACK && source != "payment") {
    rollback();  ← Compensate because we accepted
  }

stock-service receives decision:
  if (status == ROLLBACK && source != "stock") {
    rollback();  ← We rejected, nothing to rollback
  }
```

---

## State Management Design

### OrderStateStore Implementation

**Purpose:** Track order state during SAGA execution

**Design Choice:** In-memory `ConcurrentHashMap`

**Why not Kafka Streams state store?**

| Approach | Pros | Cons | Our Choice |
|----------|------|------|------------|
| **In-Memory Map** | Simple, fast, no dependencies | Lost on restart, not scalable | ✅ Phase 4 (learning) |
| **Kafka Streams State Store** | Persistent, scalable, queryable | Complex setup, heavy | ⏳ Phase 6 (production) |
| **Redis** | Fast, persistent, TTL support | Extra infrastructure | ⏳ Phase 7 (production) |
| **Database Table** | ACID guarantees, queryable | Slower, schema management | ❌ Not event-sourced |

**Current Implementation:**

```java
@Component
public class OrderStateStore {
    private final Map<String, Order> orderState = new ConcurrentHashMap<>();
    
    public void put(String orderId, Order order) { ... }
    public Optional<Order> get(String orderId) { ... }
    public void update(String orderId, Order order) { ... }
    public void remove(String orderId) { ... }
}
```

**State Lifecycle:**

```
Order Created → put(orderId, order)  [status=PENDING]
              ↓
Payment Response → update(orderId, order)  [status=CONFIRMED/REJECTED]
              ↓
Query Endpoint → get(orderId)  [return current status]
              ↓
(Optional) Cleanup → remove(orderId)  [after X days]
```

---

### State Consistency Guarantees

**What happens if order-service restarts?**

| Event | Before Restart | After Restart |
|-------|----------------|---------------|
| Order Created | In StateStore | ❌ LOST (in-memory) |
| Payment Response | In Kafka | ✅ Re-delivered (offset not committed) |
| Final Decision | Published | ✅ Re-published (idempotency prevents duplicates) |

**Problem:** Orders in-flight during restart are orphaned.

**Solutions:**

1. **Phase 4 (acceptable):** Manual recovery via Kafka UI (replay topic)
2. **Phase 6 (production):** Kafka Streams state store (persistent)
3. **Phase 7 (production):** Redis with TTL (automatic expiry)

---

## Idempotency Implementation

### Why Idempotency Matters

Kafka uses **at-least-once delivery**:

- Messages can be re-delivered (network retries, consumer restarts)
- Same event may be processed multiple times
- Without idempotency: double-reserve, double-confirm, double-rollback

### Idempotency Strategy: Processed Decisions Set

**Implementation:**

```java
@Service
public class OrderOrchestrationService {
    private final Set<String> processedDecisions = new HashSet<>();
    
    public void handlePaymentResponse(PaymentProcessedEvent event) {
        if (processedDecisions.contains(event.getOrderId())) {
            log.warn("Already processed, skipping");
            return;  // ← Prevents duplicate
        }
        
        // ... decision logic ...
        
        processedDecisions.add(event.getOrderId());  // ← Mark processed
    }
}
```

**Same pattern in payment-service:**

```java
@Service
public class PaymentService {
    private final Set<String> processedDecisions = new HashSet<>();
    
    public void processFinalDecision(FinalDecisionEvent event) {
        if (processedDecisions.contains(event.getOrderId())) {
            return;  // Already confirmed/rolled back
        }
        
        // ... confirm/rollback logic ...
        
        processedDecisions.add(event.getOrderId());
    }
}
```

---

### Idempotency Trade-offs

| Approach | Pros | Cons | Our Choice |
|----------|------|------|------------|
| **In-Memory Set** | Fast, simple | Lost on restart, memory leak risk | ✅ Phase 4 |
| **Redis Set with TTL** | Persistent, auto-cleanup | Extra infrastructure | ⏳ Phase 7 |
| **Database Table** | ACID guarantees | Slower, requires schema | ❌ Over-engineered |
| **Kafka Deduplication** | Built-in feature | Complex config | ⏳ Phase 8 |

**Memory Leak Concern:**

In-memory Set grows unbounded. Solutions:

1. **TTL-based cleanup:** Remove entries after 24 hours
2. **LRU cache:** Evict oldest entries (Guava Cache)
3. **Redis:** Use `SETEX` with automatic expiry

**Phase 4 Approach:** Accept memory growth (development only)

**Production Approach:** Redis Set with `EXPIRE 86400` (24 hour TTL)

---

## Error Handling Strategies

### Error Categories

#### 1. Transient Errors (Retry-able)

**Examples:**
- Network timeout to Kafka
- Database connection timeout
- Temporary Kafka broker unavailability

**Handling:**

```java
@KafkaListener(topics = "payment-events")
public void consumePaymentEvent(PaymentProcessedEvent event) {
    try {
        orchestrationService.handlePaymentResponse(event);
    } catch (TransientException e) {
        log.warn("Transient error, will retry: {}", e.getMessage());
        throw e;  // Kafka re-delivers message
    }
}
```

**Kafka Retry Config:**

```yaml
spring.kafka.consumer:
  properties:
    retry.backoff.ms: 1000  # Wait 1s before retry
    max.poll.interval.ms: 300000  # 5 min max processing time
```

---

#### 2. Permanent Errors (Non-Retry-able)

**Examples:**
- Invalid event format (JSON deserialization error)
- Business rule violation (customer not found)
- Data integrity error (order already completed)

**Handling:**

```java
@KafkaListener(topics = "payment-events")
public void consumePaymentEvent(PaymentProcessedEvent event) {
    try {
        orchestrationService.handlePaymentResponse(event);
    } catch (ValidationException e) {
        log.error("Permanent error, sending to DLQ: {}", e.getMessage());
        sendToDeadLetterQueue(event, e);
        // Don't throw → commit offset (don't retry)
    }
}
```

**Dead Letter Queue (DLQ):**

```yaml
spring.kafka.consumer:
  properties:
    # Send failed messages to DLQ topic
    default.api.timeout.ms: 60000
```

**Manual DLQ Topic:**

```java
kafkaTemplate.send("payment-events-dlq", event);
```

---

#### 3. Poison Pill Messages

**Problem:** Malformed message blocks consumer forever

**Example:**

```json
{
  "orderId": null,  ← Invalid
  "amount": "not-a-number"  ← Deserialization fails
}
```

**Handling:**

```java
@Bean
public DefaultErrorHandler errorHandler(
    KafkaTemplate<String, Object> kafkaTemplate) {
    
    return new DefaultErrorHandler(
        // Send to DLQ after 3 retries
        new DeadLetterPublishingRecoverer(kafkaTemplate),
        new FixedBackOff(1000L, 3)
    );
}
```

---

### Timeout Handling

**Problem:** Participant never responds (service down, network partition)

**Example:**

```
order-service publishes OrderCreatedEvent
   ↓
payment-service is DOWN
   ↓
order-service waits forever for PaymentProcessedEvent
```

**Solution: Timeout + Compensation**

```java
@Scheduled(fixedDelay = 60000)  // Run every 60 seconds
public void checkTimedOutOrders() {
    LocalDateTime timeout = LocalDateTime.now().minusMinutes(5);
    
    orderStateStore.getAll().values().stream()
        .filter(order -> order.getStatus() == OrderStatus.PENDING)
        .filter(order -> order.getCreatedAt().isBefore(timeout))
        .forEach(order -> {
            log.warn("Order timed out: {}", order.getOrderId());
            
            // Publish rejection decision
            FinalDecisionEvent decision = FinalDecisionEvent.builder()
                .orderId(order.getOrderId())
                .status(DecisionStatus.REJECTED)
                .reason("Timeout: No response after 5 minutes")
                .build();
            
            kafkaTemplate.send("order-events", decision);  // Same topic as OrderCreatedEvent
        });
}
```

---

## Trade-offs and Best Practices

### SAGA vs Two-Phase Commit (2PC)

| Aspect | SAGA | Two-Phase Commit |
|--------|------|------------------|
| **Consistency** | Eventual | Immediate (ACID) |
| **Availability** | High (services independent) | Low (coordinator blocks) |
| **Latency** | Higher (multiple round-trips) | Lower (single transaction) |
| **Complexity** | High (compensation logic) | Low (database handles it) |
| **Scalability** | High (no distributed locks) | Low (locks across services) |
| **Data Integrity** | Temporary inconsistency | Always consistent |

**When to use SAGA:**
- Microservices architecture
- Services owned by different teams
- Need high availability
- Can tolerate eventual consistency

**When to use 2PC:**
- Monolithic application
- Financial transactions (banks)
- Cannot tolerate inconsistency
- Low transaction volume

---

### Choreography vs Orchestration

| Aspect | Choreography (Ours) | Orchestration |
|--------|---------------------|---------------|
| **Coupling** | Loose (event-driven) | Tight (orchestrator knows all services) |
| **Debugging** | Hard (events scattered) | Easy (single place) |
| **SPOF** | None (decentralized) | Orchestrator is SPOF |
| **Complexity** | Increases with services | Centralized in orchestrator |
| **Autonomy** | High (services independent) | Low (orchestrator controls) |

**Our Hybrid Approach:**

✅ **Keeps:** Event-driven communication (choreography)  
✅ **Adds:** Decision point in order-service (orchestration)  
✅ **Avoids:** Complex orchestrator logic (keeps decision simple)

---

### State Management Trade-offs

| Approach | Consistency | Scalability | Complexity | Our Phase |
|----------|-------------|-------------|------------|-----------|
| **In-Memory Map** | Lost on restart | Single instance only | Low | Phase 4 ✅ |
| **Kafka Streams State Store** | Persistent, replicated | High (partitioned) | High | Phase 6 ⏳ |
| **Redis** | Persistent | High (clustered) | Medium | Phase 7 ⏳ |
| **PostgreSQL** | ACID | Medium | Medium | ❌ Not event-sourced |

**Recommendation:**

- **Development:** In-memory Map (Phase 4)
- **Production:** Kafka Streams state store (Phase 6) or Redis (Phase 7)

---

### Idempotency Trade-offs

| Approach | Correctness | Performance | Memory | Our Phase |
|----------|-------------|-------------|--------|-----------|
| **No Idempotency** | ❌ Duplicates | Fastest | None | ❌ Never |
| **In-Memory Set** | ✅ Correct | Fast | Grows unbounded | Phase 4 ✅ |
| **Redis Set with TTL** | ✅ Correct | Medium | Auto-cleanup | Phase 7 ⏳ |
| **Database Unique Constraint** | ✅ Correct | Slow | Bounded | ❌ Over-engineered |

**Best Practice:** Use Redis with TTL in production

---

## Production Considerations

### Observability

**Required Metrics:**

1. **SAGA Duration:**
   ```
   order.saga.duration_ms{status=CONFIRMED} = 450ms  (p95)
   order.saga.duration_ms{status=REJECTED} = 200ms  (p95)
   ```

2. **Success Rate:**
   ```
   order.saga.success_rate = CONFIRMED / (CONFIRMED + REJECTED) = 95%
   ```

3. **State Store Size:**
   ```
   order.state.store.size = 1200 orders  (in-flight)
   ```

4. **Consumer Lag:**
   ```
   kafka.consumer.lag{topic=payment-events} = 0 messages
   ```

**Required Logging:**

```java
log.info("SAGA_START orderId={} customerId={} amount={}", ...);
log.info("SAGA_PAYMENT_RESPONSE orderId={} status={} duration={}ms", ...);
log.info("SAGA_DECISION orderId={} finalStatus={} totalDuration={}ms", ...);
log.info("SAGA_COMPLETE orderId={} finalStatus={} totalDuration={}ms", ...);
```

**Required Tracing:**

- Add Spring Cloud Sleuth for distributed tracing
- Trace ID propagated through Kafka headers
- Visualize in Zipkin/Jaeger

---

### Scalability

**Horizontal Scaling:**

| Service | Scaling Strategy | Max Instances | Bottleneck |
|---------|-----------------|---------------|------------|
| **order-service** | Multiple instances + load balancer | Unlimited | State store (use Redis) |
| **payment-service** | Kafka consumer group partitioning | = Kafka partitions | Database connections |

**Kafka Partitioning:**

```yaml
# Create topics with 10 partitions
kafka-topics --create --topic order-events --partitions 10
kafka-topics --create --topic payment-events --partitions 10
```

**Consumer Scaling:**

```bash
# Run 5 payment-service instances
# Kafka distributes partitions: 2+2+2+2+2 = 10 partitions
docker-compose up --scale payment-service=5
```

---

### Security

**Required Security Measures:**

1. **Kafka Authentication:**
   ```yaml
   spring.kafka.properties:
     security.protocol: SASL_SSL
     sasl.mechanism: SCRAM-SHA-512
     sasl.jaas.config: "username=xxx password=xxx"
   ```

2. **Kafka Authorization (ACLs):**
   ```bash
   kafka-acls --add --allow-principal User:order-service \
     --operation Write --topic order-events
   
   kafka-acls --add --allow-principal User:payment-service \
     --operation Read --topic order-events
   ```

3. **Encryption in Transit:**
   - Use TLS for Kafka connections
   - Use TLS for database connections

4. **Sensitive Data:**
   - Don't log credit card numbers
   - Don't publish PII to Kafka topics
   - Use Kafka encryption for sensitive events

---

### Disaster Recovery

**Required Backups:**

1. **Kafka Topics:** Enable replication factor 3
2. **State Store:** Periodic snapshots (Kafka Streams handles this)
3. **Database:** Daily backups + transaction logs

**Recovery Scenarios:**

| Failure | Impact | Recovery |
|---------|--------|----------|
| **Kafka Broker Down** | Messages queued | Auto-failover to replica |
| **order-service Restart** | In-memory state lost | Kafka re-delivers messages |
| **payment-service Restart** | Processing paused | Kafka re-delivers after restart |
| **Database Corruption** | Payment data lost | Restore from backup + replay Kafka |
| **Entire Datacenter Down** | Full outage | Failover to DR datacenter |

**Testing:**

```bash
# Chaos Engineering: Kill random service
docker-compose kill payment-service
sleep 10
docker-compose up -d payment-service

# Verify: Orders processed after restart
```

---

### Cost Optimization

**Cost Drivers:**

1. **Kafka Storage:** Events retained forever
2. **State Store:** Growing indefinitely
3. **Logs:** Verbose logging in production

**Optimizations:**

1. **Kafka Retention:**
   ```yaml
   retention.ms: 604800000  # 7 days (not forever)
   ```

2. **State Store Cleanup:**
   ```java
   @Scheduled(cron = "0 0 2 * * *")  // 2am daily
   public void cleanupOldOrders() {
       LocalDateTime cutoff = LocalDateTime.now().minusDays(7);
       orderStateStore.removeOlderThan(cutoff);
   }
   ```

3. **Log Retention:**
   ```yaml
   logging:
     level:
       com.example: INFO  # Not DEBUG in production
   ```

---

## Summary

### Key Takeaways

1. **SAGA enables distributed transactions without 2PC**
   - Each service has local transaction + compensating transaction
   - Eventual consistency instead of immediate consistency

2. **Choreography vs Orchestration is a spectrum**
   - Pure choreography: Fully decentralized, hard to debug
   - Pure orchestration: Centralized, easier to debug, SPOF
   - Hybrid (ours): Best of both worlds

3. **Idempotency is non-negotiable**
   - Kafka delivers at-least-once
   - Same event can be processed multiple times
   - Must use deduplication (Set, Redis, database constraint)

4. **State management matters**
   - In-memory: Fast but lost on restart
   - Kafka Streams: Persistent and scalable
   - Redis: Good balance for production

5. **Production requires observability**
   - Metrics: Success rate, latency, consumer lag
   - Logging: Structured logs with correlation IDs
   - Tracing: Distributed tracing with Sleuth/Zipkin

---

### Further Reading

- [Microservices.io - SAGA Pattern](https://microservices.io/patterns/data/saga.html)
- [Martin Kleppmann - Designing Data-Intensive Applications](https://dataintensive.net/)
- [Chris Richardson - Microservices Patterns (Book)](https://microservices.io/book)
- [Kafka Documentation - Exactly-Once Semantics](https://kafka.apache.org/documentation/#semantics)
- [Spring Kafka Reference](https://docs.spring.io/spring-kafka/reference/)

---

**Document Version:** 1.0  
**Last Updated:** July 2026  
**Next Review:** After Phase 5 completion (stock-service integration)
