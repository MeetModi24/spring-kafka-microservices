# Topic Architecture Strategy

> **Document Type:** Technical Design Document (TDD)  
> **Version:** 1.0  
> **Last Updated:** July 2026  
> **Status:** Reference Implementation Pattern

---

## Table of Contents

1. [Overview](#overview)
2. [The 2-Topic Pattern](#the-2-topic-pattern)
3. [Reference Repository Comparison](#reference-repository-comparison)
4. [Consumer Group Strategy](#consumer-group-strategy)
5. [Why Not 3 Topics?](#why-not-3-topics)
6. [Implementation Examples](#implementation-examples)
7. [Common Mistakes](#common-mistakes)
8. [Testing Strategy](#testing-strategy)

---

## Overview

### Purpose

This document explains the **2-topic architecture** pattern used in this project, matching the reference repository approach for SAGA orchestration with Kafka.

### Key Principle

**One topic per event stream direction**, not one topic per event type.

---

## The 2-Topic Pattern

### Architecture Summary

```
┌────────────────────────────────────────────────────────────────┐
│                    2-TOPIC ARCHITECTURE                         │
└────────────────────────────────────────────────────────────────┘

Topic 1: "order-events"
├─ Event Type 1: OrderCreatedEvent (status=NEW)
│  ├─ Publisher: order-service
│  └─ Consumer: payment-service (group: payment-service-group)
│
└─ Event Type 2: FinalDecisionEvent (status=CONFIRMED/ROLLBACK)
   ├─ Publisher: order-service
   └─ Consumer: payment-service (group: payment-decision-group)

Topic 2: "payment-events"
└─ Event Type: PaymentProcessedEvent (status=ACCEPT/REJECT)
   ├─ Publisher: payment-service
   └─ Consumer: order-service (group: order-service-orchestrator-group)
```

### Topic Details

| Topic | Purpose | Event Types | Publishers | Consumers |
|-------|---------|-------------|-----------|-----------|
| **order-events** | Bidirectional order lifecycle | OrderCreatedEvent<br>FinalDecisionEvent | order-service | payment-service (2 groups) |
| **payment-events** | Payment validation results | PaymentProcessedEvent | payment-service | order-service |

---

## Reference Repository Comparison

### Reference Repo Structure (piomin/sample-spring-kafka-microservices)

```java
// Topic configuration in order-service
@Bean
public NewTopic orders() {
    return TopicBuilder.name("orders")
        .partitions(3)
        .compact()  // Log compaction for state tracking
        .build();
}

@Bean
public NewTopic paymentOrders() {
    return TopicBuilder.name("payment-orders")
        .partitions(3)
        .build();
}

@Bean
public NewTopic stockOrders() {
    return TopicBuilder.name("stock-orders")
        .partitions(3)
        .build();
}
```

**Notice:** Only **3 topics** for **3 services**, not 4 or 5 topics.

### Our Implementation Mapping

| Reference Repo | Our Implementation | Purpose |
|----------------|-------------------|---------|
| `orders` | `order-events` | Initial orders + final decisions |
| `payment-orders` | `payment-events` | Payment responses |
| `stock-orders` | `stock-events` (Phase 5) | Stock responses |

**Key Insight:** The `orders` topic is **bidirectional** - it carries both requests AND final decisions.

---

## Consumer Group Strategy

### Why Multiple Consumer Groups on Same Topic?

**payment-service has TWO listeners on "order-events":**

```java
// Listener 1: Handle new orders (reserve funds)
@KafkaListener(
    topics = "order-events",
    groupId = "payment-service-group",
    containerFactory = "kafkaListenerContainerFactory"
)
public void consumeOrderEvent(OrderCreatedEvent event) {
    if ("NEW".equals(event.getStatus())) {
        paymentService.reserveFunds(event);
    }
}

// Listener 2: Handle final decisions (confirm/rollback)
@KafkaListener(
    topics = "order-events",
    groupId = "payment-decision-group",  // Different group!
    containerFactory = "kafkaListenerContainerFactory"
)
public void consumeDecisionEvent(FinalDecisionEvent event) {
    if ("CONFIRMED".equals(event.getStatus()) || "ROLLBACK".equals(event.getStatus())) {
        paymentService.processFinalDecision(event);
    }
}
```

### Consumer Group Behavior

| Consumer Group | Topic | Filters | Action |
|----------------|-------|---------|--------|
| `payment-service-group` | order-events | status=NEW | Reserve funds |
| `payment-decision-group` | order-events | status=CONFIRMED/ROLLBACK | Confirm or rollback |
| `order-service-orchestrator-group` | payment-events | All messages | Orchestrate decision |

**Kafka Guarantee:** Each consumer group receives ALL messages independently.

**Offset Tracking:** Each group maintains its own offset, enabling independent processing.

---

## Why Not 3 Topics?

### Common Anti-Pattern ❌

Many developers create separate topics for each event type:

```
order-events             ← Initial orders
payment-events           ← Payment responses
order-decision-events    ← Final decisions  ❌ UNNECESSARY
```

### Problems with 3-Topic Approach

1. **Over-Engineering**
   - Adds unnecessary complexity
   - More topics to manage and monitor
   - More Kafka partitions (resource overhead)

2. **Violates Event Sourcing Principles**
   - Order lifecycle should be in ONE stream
   - Splitting breaks event ordering guarantees
   - Hard to reconstruct order state from events

3. **Breaks Reference Architecture Pattern**
   - Reference repo uses 2 topics
   - Industry best practice is topic-per-aggregate-root

4. **Consumer Confusion**
   - Which topic has the "final" order state?
   - Need to consume multiple topics to track one order

### The 2-Topic Solution ✅

```
order-events     ← BOTH initial orders AND final decisions
payment-events   ← Payment responses only
```

**Benefits:**
- ✅ Clear topic ownership (order-events = order lifecycle)
- ✅ Event ordering guaranteed within topic partition
- ✅ Simpler consumer logic (one topic to track orders)
- ✅ Matches reference architecture
- ✅ Follows event sourcing best practices

---

## Implementation Examples

### Order Service: Publishing to Same Topic

```java
@Service
@RequiredArgsConstructor
public class OrderOrchestrationService {
    
    private static final String TOPIC = "order-events";  // Same topic!
    private final KafkaTemplate<String, Object> kafkaTemplate;
    
    // Step 1: Publish initial order
    public void publishOrder(Order order) {
        OrderCreatedEvent event = OrderCreatedEvent.builder()
            .orderId(order.getOrderId())
            .status("NEW")  // Status discriminator
            .build();
        
        kafkaTemplate.send(TOPIC, order.getOrderId(), event);
    }
    
    // Step 3: Publish final decision (SAME TOPIC)
    public void publishDecision(Order order, String decision) {
        FinalDecisionEvent event = FinalDecisionEvent.builder()
            .orderId(order.getOrderId())
            .status(decision)  // Status discriminator
            .build();
        
        kafkaTemplate.send(TOPIC, order.getOrderId(), event);  // Same topic!
    }
}
```

### Payment Service: Two Listeners on Same Topic

```java
@Component
@RequiredArgsConstructor
@Slf4j
public class OrderEventConsumer {
    
    private final PaymentService paymentService;
    
    /**
     * Consumer Group 1: Handle new orders
     * Consumes: OrderCreatedEvent (status=NEW)
     */
    @KafkaListener(
        topics = "order-events",
        groupId = "payment-service-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeOrderEvent(OrderCreatedEvent event) {
        log.info("Received OrderCreatedEvent: orderId={}, status={}", 
            event.getOrderId(), event.getStatus());
        
        if ("NEW".equals(event.getStatus())) {
            paymentService.reserveFunds(event);
        }
    }
}

@Component
@RequiredArgsConstructor
@Slf4j
public class DecisionEventConsumer {
    
    private final PaymentService paymentService;
    
    /**
     * Consumer Group 2: Handle final decisions
     * Consumes: FinalDecisionEvent (status=CONFIRMED/ROLLBACK)
     */
    @KafkaListener(
        topics = "order-events",  // SAME TOPIC as above!
        groupId = "payment-decision-group",  // DIFFERENT GROUP
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeDecisionEvent(FinalDecisionEvent event) {
        log.info("Received FinalDecisionEvent: orderId={}, status={}", 
            event.getOrderId(), event.getStatus());
        
        if ("CONFIRMED".equals(event.getStatus()) || "ROLLBACK".equals(event.getStatus())) {
            paymentService.processFinalDecision(event);
        }
    }
}
```

### Event Discrimination Pattern

**Key Technique:** Use a status field to discriminate event types:

```java
// Base pattern for all events on order-events topic
interface OrderEvent {
    String getOrderId();      // Correlation ID
    String getStatus();       // Event type discriminator
    LocalDateTime getTimestamp();
}

// Event types
OrderCreatedEvent       → status = "NEW"
FinalDecisionEvent      → status = "CONFIRMED" | "ROLLBACK" | "REJECTED"
```

---

## Common Mistakes

### Mistake 1: Creating order-decision-events Topic ❌

```java
// WRONG: Separate topic for decisions
@Bean
public NewTopic orderDecisionTopic() {
    return TopicBuilder.name("order-decision-events").build();  // ❌
}

kafkaTemplate.send("order-decision-events", decision);  // ❌
```

**Fix:**
```java
// CORRECT: Use same topic as orders
kafkaTemplate.send("order-events", decision);  // ✅
```

### Mistake 2: Same Consumer Group for Both Listeners ❌

```java
// WRONG: Same group ID
@KafkaListener(topics = "order-events", groupId = "payment-service")
public void handleOrder(OrderCreatedEvent event) { ... }

@KafkaListener(topics = "order-events", groupId = "payment-service")  // ❌ Same group!
public void handleDecision(FinalDecisionEvent event) { ... }
```

**Problem:** Kafka will distribute messages across listeners (one listener may miss messages).

**Fix:**
```java
// CORRECT: Different group IDs
@KafkaListener(topics = "order-events", groupId = "payment-service-group")  // ✅
public void handleOrder(OrderCreatedEvent event) { ... }

@KafkaListener(topics = "order-events", groupId = "payment-decision-group")  // ✅
public void handleDecision(FinalDecisionEvent event) { ... }
```

### Mistake 3: Not Filtering by Status ❌

```java
// WRONG: No status check (processes all events)
@KafkaListener(topics = "order-events")
public void consume(Map<String, Object> event) {
    paymentService.reserveFunds(event);  // ❌ May process decisions as orders!
}
```

**Fix:**
```java
// CORRECT: Filter by status
@KafkaListener(topics = "order-events")
public void consume(OrderCreatedEvent event) {
    if ("NEW".equals(event.getStatus())) {  // ✅ Filter
        paymentService.reserveFunds(event);
    }
}
```

---

## Testing Strategy

### Test Scenario 1: Verify Topic Count

**Expected:** Only 2 topics created (order-events, payment-events)

```bash
# List Kafka topics
kafka-topics --list --bootstrap-server localhost:9092

# Expected output:
# order-events
# payment-events
# (NOT order-decision-events)
```

### Test Scenario 2: Verify Consumer Groups

**Expected:** 3 consumer groups total

```bash
# List consumer groups
kafka-consumer-groups --list --bootstrap-server localhost:9092

# Expected output:
# payment-service-group
# payment-decision-group
# order-service-orchestrator-group
```

### Test Scenario 3: Verify Messages in order-events

**Expected:** Both OrderCreatedEvent AND FinalDecisionEvent in SAME topic

```bash
# Consume from order-events topic
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic order-events --from-beginning --property print.key=true

# Expected output (2 messages for one order):
# abc-123    {"orderId":"abc-123","status":"NEW",...}         ← OrderCreatedEvent
# abc-123    {"orderId":"abc-123","status":"CONFIRMED",...}   ← FinalDecisionEvent
```

### Test Scenario 4: Verify Consumer Group Independence

**Expected:** Each group receives ALL messages

```bash
# Check offsets for payment-service-group
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group payment-service-group --describe

# Check offsets for payment-decision-group
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group payment-decision-group --describe

# Expected: Both groups have same number of messages processed
```

### Integration Test

```java
@SpringBootTest
@Testcontainers
class TopicArchitectureTest {
    
    @Container
    static KafkaContainer kafka = new KafkaContainer(...);
    
    @Autowired
    private AdminClient adminClient;
    
    @Test
    void shouldHaveExactlyTwoTopics() throws Exception {
        Set<String> topics = adminClient.listTopics().names().get();
        
        assertThat(topics)
            .contains("order-events", "payment-events")
            .doesNotContain("order-decision-events");
    }
    
    @Test
    void shouldHaveThreeConsumerGroups() throws Exception {
        Collection<ConsumerGroupListing> groups = adminClient.listConsumerGroups()
            .all().get();
        
        List<String> groupIds = groups.stream()
            .map(ConsumerGroupListing::groupId)
            .collect(Collectors.toList());
        
        assertThat(groupIds)
            .contains(
                "payment-service-group",
                "payment-decision-group",
                "order-service-orchestrator-group"
            );
    }
}
```

---

## Summary

### Architecture Principles

1. **Topic Per Aggregate Root**
   - `order-events` = order lifecycle (from NEW to CONFIRMED/REJECTED)
   - `payment-events` = payment validation results

2. **Consumer Groups for Event Type Discrimination**
   - Multiple consumer groups on same topic
   - Each group filters by status field

3. **Event Sourcing Alignment**
   - All events for an order in one topic (partition)
   - Event ordering guaranteed
   - Easy to rebuild order state

### Quick Reference

| Concept | Pattern |
|---------|---------|
| **Number of Topics** | 2 (not 3) |
| **Topic Names** | order-events, payment-events |
| **Consumer Groups** | 3 total (2 on order-events, 1 on payment-events) |
| **Event Discrimination** | Status field ("NEW", "CONFIRMED", "ROLLBACK") |
| **Partitioning** | By orderId (ensures ordering per order) |

---

## Further Reading

- **Reference Repository:** [piomin/sample-spring-kafka-microservices](https://github.com/piomin/sample-spring-kafka-microservices/tree/master/order-service/src/main/resources)
- **Kafka Consumer Groups:** [Kafka Documentation](https://kafka.apache.org/documentation/#consumergroups)
- **Event Sourcing:** [Martin Fowler - Event Sourcing](https://martinfowler.com/eaaDev/EventSourcing.html)
- **Topic Design:** [Confluent - Topic Naming](https://www.confluent.io/blog/kafka-topic-naming-conventions/)

---

**Document Version:** 1.0  
**Last Updated:** July 2026  
**Next Review:** After Phase 5 completion (stock-service integration)  
**Status:** Production Ready
