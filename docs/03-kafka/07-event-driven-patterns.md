# Event-Driven Patterns for Microservices

## Table of Contents
1. [Event-Driven Architecture Overview](#event-driven-architecture-overview)
2. [SAGA Pattern](#saga-pattern)
3. [Choreography vs Orchestration](#choreography-vs-orchestration)
4. [Event Sourcing](#event-sourcing)
5. [CQRS Pattern](#cqrs-pattern)
6. [Outbox Pattern](#outbox-pattern)
7. [Event Notification vs Event-Carried State Transfer](#event-notification-vs-event-carried-state-transfer)

---

## Event-Driven Architecture Overview

### What is Event-Driven Architecture?

An architectural pattern where services communicate through **events** rather than direct synchronous calls.

**Key Concepts:**
- **Event:** A fact that something happened (immutable, past tense)
- **Event Producer:** Service that publishes events
- **Event Consumer:** Service that reacts to events
- **Event Broker:** Infrastructure that routes events (Kafka)

**Example Events:**
```
OrderCreated
PaymentProcessed
InventoryReserved
OrderShipped
PaymentFailed
```

### Benefits

1. **Loose Coupling** - Services don't know about each other
2. **Scalability** - Events processed asynchronously
3. **Resilience** - Services can fail independently
4. **Auditability** - Full event history
5. **Flexibility** - Add new consumers without changing producers

### Drawbacks

1. **Eventual Consistency** - Data not immediately consistent
2. **Complexity** - Distributed tracing, debugging harder
3. **Ordering Challenges** - Events may arrive out of order
4. **Duplication** - Need idempotency to handle duplicate events

---

## SAGA Pattern

### What is SAGA?

A pattern for managing **distributed transactions** across multiple microservices without a distributed transaction coordinator (2PC).

**Key Idea:** Break long transaction into sequence of local transactions, each with a compensating transaction for rollback.

### Problem: Distributed Transactions

**Scenario:** Customer places an order
```
1. order-service creates order
2. payment-service reserves funds
3. stock-service reserves inventory
4. shipping-service schedules delivery
```

**What if step 3 fails?** Need to rollback steps 1 and 2.

**Traditional 2PC (Two-Phase Commit):**
- Coordinator locks all resources
- All services vote commit/abort
- Coordinator decides final outcome

**Problems with 2PC:**
- Locks resources (poor performance)
- Single point of failure (coordinator)
- Not supported by NoSQL databases

**SAGA Solution:**
- Each service commits locally
- If any fails, run compensating transactions to undo

### SAGA Types

#### 1. Choreography-Based SAGA (Decentralized)

Services communicate via events without a coordinator.

**Flow:**
```
order-service:
  1. Creates order with status=NEW
  2. Publishes OrderCreated event
  
payment-service (listens to OrderCreated):
  3. Reserves funds
  4. Publishes PaymentReserved event
  
stock-service (listens to OrderCreated):
  5. Reserves inventory
  6. Publishes InventoryReserved event
  
order-service (listens to PaymentReserved + InventoryReserved):
  7. Marks order as CONFIRMED
  8. Publishes OrderConfirmed event
```

**Failure Scenario:**
```
stock-service:
  5. Insufficient inventory
  6. Publishes InventoryFailed event
  
order-service (listens to InventoryFailed):
  7. Marks order as REJECTED
  8. Publishes OrderRejected event
  
payment-service (listens to OrderRejected):
  9. Releases reserved funds (compensation)
```

**Pros:**
- No single point of failure
- Services fully decoupled

**Cons:**
- Harder to understand (logic spread across services)
- Cyclic dependencies possible

#### 2. Orchestration-Based SAGA (Centralized)

**Our project uses this approach.**

A coordinator (orchestrator) tells services what to do.

**Flow:**
```
order-service (orchestrator):
  1. Creates order with status=NEW
  2. Publishes OrderCreated event
  3. Waits for responses from payment-service and stock-service
  
payment-service:
  4. Receives OrderCreated
  5. Reserves funds
  6. Publishes PaymentReserved to payment-orders topic
  
stock-service:
  7. Receives OrderCreated
  8. Reserves inventory
  9. Publishes InventoryReserved to stock-orders topic
  
order-service:
  10. Receives both responses via Kafka Streams join
  11. If both success → Publishes OrderConfirmed (status=CONFIRMED)
  12. If any failed → Publishes OrderRejected (status=ROLLBACK)
  
payment-service & stock-service:
  13. Listen for OrderConfirmed or OrderRejected
  14. If CONFIRMED → Commit reservations
  15. If ROLLBACK → Release reservations (compensation)
```

**Pros:**
- Easier to understand (centralized logic)
- Better observability
- Easier to add new steps

**Cons:**
- Orchestrator is a single point of failure (mitigated by replication)
- Orchestrator can become complex

### Implementing Orchestration SAGA

**1. Order Service (Orchestrator)**

```java
@Service
public class OrderOrchestrator {
    
    @Autowired
    private KafkaTemplate<String, Order> kafkaTemplate;
    
    // Create order and start SAGA
    public void createOrder(CreateOrderRequest request) {
        Order order = new Order(
            UUID.randomUUID().toString(),
            request.getCustomerId(),
            request.getItems(),
            OrderStatus.NEW
        );
        
        // Publish OrderCreated event
        kafkaTemplate.send("orders", order.getOrderId(), order);
        log.info("SAGA started for order {}", order.getOrderId());
    }
}
```

**2. Payment Service (Participant)**

```java
@Service
public class PaymentService {
    
    @Autowired
    private KafkaTemplate<String, PaymentResponse> kafkaTemplate;
    
    // Listen to OrderCreated
    @KafkaListener(topics = "orders", groupId = "payment-service")
    public void reserveFunds(Order order) {
        if (order.getStatus() != OrderStatus.NEW) {
            return; // Only process NEW orders
        }
        
        PaymentResponse response;
        try {
            // Reserve funds logic
            boolean success = reserveFundsForCustomer(order.getCustomerId(), order.getTotalAmount());
            
            response = new PaymentResponse(
                order.getOrderId(),
                success ? PaymentStatus.ACCEPT : PaymentStatus.REJECT
            );
        } catch (Exception e) {
            response = new PaymentResponse(order.getOrderId(), PaymentStatus.REJECT);
        }
        
        // Publish response
        kafkaTemplate.send("payment-orders", order.getOrderId(), response);
    }
    
    // Listen to OrderConfirmed/OrderRejected for compensation
    @KafkaListener(topics = "orders", groupId = "payment-service")
    public void handleOrderDecision(Order order) {
        if (order.getStatus() == OrderStatus.CONFIRMED) {
            commitReservation(order.getOrderId());
        } else if (order.getStatus() == OrderStatus.ROLLBACK) {
            releaseReservation(order.getOrderId());  // Compensation
        }
    }
}
```

**3. Order Service - Kafka Streams Join**

```java
@Configuration
@EnableKafkaStreams
public class OrderStreamsConfig {
    
    @Bean
    public KStream<String, Order> orderResponseStream(StreamsBuilder builder) {
        
        // Stream of orders
        KStream<String, Order> orders = builder.stream("orders");
        
        // Stream of payment responses
        KStream<String, PaymentResponse> paymentResponses = 
            builder.stream("payment-orders");
        
        // Stream of stock responses
        KStream<String, StockResponse> stockResponses = 
            builder.stream("stock-orders");
        
        // Join payment + stock responses
        KStream<String, CombinedResponse> combined = paymentResponses
            .join(
                stockResponses,
                (payment, stock) -> new CombinedResponse(payment, stock),
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)),
                StreamJoined.with(Serdes.String(), paymentSerde, stockSerde)
            );
        
        // Determine final status
        combined.foreach((orderId, response) -> {
            if (response.isSuccess()) {
                publishOrderConfirmed(orderId);
            } else {
                publishOrderRollback(orderId, response.getFailureSource());
            }
        });
        
        return orders;
    }
    
    private void publishOrderConfirmed(String orderId) {
        Order order = new Order(orderId, OrderStatus.CONFIRMED);
        kafkaTemplate.send("orders", orderId, order);
    }
    
    private void publishOrderRollback(String orderId, String failureSource) {
        Order order = new Order(orderId, OrderStatus.ROLLBACK, failureSource);
        kafkaTemplate.send("orders", orderId, order);
    }
}
```

### SAGA State Machine

```
┌─────────────┐
│    NEW      │  ← Order created
└──────┬──────┘
       │
       ├──→ Publish OrderCreated
       │
       ↓
┌─────────────┐
│  PENDING    │  ← Waiting for payment + stock responses
└──────┬──────┘
       │
       ├──→ Receive PaymentReserved + InventoryReserved
       │
       ↓
    Decision
       │
  ┌────┴────┐
  │         │
Success   Failure
  │         │
  ↓         ↓
┌──────┐  ┌──────────┐
│CONFIRMED│ │ROLLBACK  │
└────┬───┘  └────┬─────┘
     │           │
     ↓           ↓
 Commit     Compensate
```

---

## Choreography vs Orchestration

### Comparison

| Aspect | Choreography | Orchestration |
|---|---|---|
| **Coordination** | Decentralized | Centralized |
| **Complexity** | Distributed | Concentrated |
| **Coupling** | Loosely coupled | Tightly coupled to orchestrator |
| **Observability** | Harder (trace across services) | Easier (single entry point) |
| **Failure Handling** | Complex (distributed logic) | Simpler (orchestrator decides) |
| **Scalability** | High (no bottleneck) | Medium (orchestrator can bottleneck) |
| **Use Case** | Simple workflows, few steps | Complex workflows, many steps |

### When to Use Choreography

- Few services involved (2-3)
- Simple workflows
- Services owned by different teams
- Need maximum decoupling

**Example:** Notification service subscribes to all events and sends emails (no coordination needed).

### When to Use Orchestration

- Many services involved (4+)
- Complex workflows with conditional logic
- Need centralized visibility
- Need to enforce business rules

**Example:** Our order processing (order → payment → stock → shipping) - easier to manage from one place.

---

## Event Sourcing

### What is Event Sourcing?

Storing **state changes as a sequence of events** instead of current state.

**Traditional Approach:**
```sql
UPDATE orders SET status='CONFIRMED' WHERE id='O1';
-- Overwrites previous state (lost history)
```

**Event Sourcing:**
```
Event 1: OrderCreated(id=O1, amount=100)
Event 2: PaymentReserved(id=O1, amount=100)
Event 3: InventoryReserved(id=O1, items=[...])
Event 4: OrderConfirmed(id=O1)
```

Current state = replay all events.

### Benefits

1. **Complete Audit Trail** - Every change is recorded
2. **Time Travel** - Replay events to any point in time
3. **Debugging** - Reproduce bugs by replaying events
4. **Analytics** - Query historical data

### Drawbacks

1. **Complexity** - Need event store, replaying logic
2. **Performance** - Replaying events can be slow (use snapshots)
3. **Schema Evolution** - Old events may not match new schema

### Implementation

**Event Store (Kafka):**
```java
@Service
public class OrderEventStore {
    
    @Autowired
    private KafkaTemplate<String, OrderEvent> kafkaTemplate;
    
    public void append(OrderEvent event) {
        kafkaTemplate.send("order-events", event.getOrderId(), event);
    }
}
```

**Replaying Events:**
```java
@Service
public class OrderProjection {
    
    public Order rebuild(String orderId) {
        // Read all events for order
        List<OrderEvent> events = kafkaConsumer.poll("order-events", orderId);
        
        // Replay events
        Order order = new Order();
        for (OrderEvent event : events) {
            order.apply(event);
        }
        
        return order;
    }
}
```

---

## CQRS Pattern

### What is CQRS?

**Command Query Responsibility Segregation:** Separate **write model** (commands) from **read model** (queries).

**Traditional Approach:**
```java
// Same model for read and write
Order order = orderRepository.findById(id);  // Read
order.setStatus(OrderStatus.CONFIRMED);
orderRepository.save(order);                 // Write
```

**CQRS:**
```java
// Write model (optimized for writes)
commandService.confirmOrder(orderId);  // Publishes event

// Read model (optimized for reads, materialized view)
OrderView view = queryService.getOrderView(orderId);  // From denormalized table
```

### Benefits

1. **Scalability** - Scale reads and writes independently
2. **Performance** - Optimize read model for queries (denormalized)
3. **Flexibility** - Different storage for read/write (SQL for write, Elasticsearch for read)

### Implementation with Kafka

**Write Side (Command):**
```java
@Service
public class OrderCommandService {
    
    @Autowired
    private KafkaTemplate<String, OrderEvent> kafkaTemplate;
    
    public void confirmOrder(String orderId) {
        OrderConfirmedEvent event = new OrderConfirmedEvent(orderId, Instant.now());
        kafkaTemplate.send("order-events", orderId, event);
    }
}
```

**Read Side (Query):**
```java
@Service
public class OrderQueryService {
    
    @Autowired
    private OrderViewRepository repository;  // Denormalized view
    
    // Listen to events and update read model
    @KafkaListener(topics = "order-events", groupId = "order-view-service")
    public void updateView(OrderEvent event) {
        OrderView view = repository.findById(event.getOrderId())
            .orElse(new OrderView(event.getOrderId()));
        
        view.apply(event);  // Update denormalized view
        repository.save(view);
    }
    
    public OrderView getOrderView(String orderId) {
        return repository.findById(orderId).orElseThrow();
    }
}
```

---

## Outbox Pattern

### Problem: Dual Write

Writing to database and Kafka is not atomic:
```java
// Problem: What if Kafka send fails after DB commit?
orderRepository.save(order);           // Success
kafkaTemplate.send("orders", order);   // Fails → Data inconsistency
```

### Solution: Outbox Pattern

1. Write to database + outbox table in **same transaction**
2. Separate process polls outbox and publishes to Kafka

**Implementation:**

```java
@Entity
public class OutboxEvent {
    @Id
    private String id;
    private String aggregateId;
    private String eventType;
    private String payload;
    private LocalDateTime createdAt;
    private boolean published;
}

@Service
public class OrderService {
    
    @Autowired
    private OrderRepository orderRepository;
    
    @Autowired
    private OutboxRepository outboxRepository;
    
    @Transactional
    public void createOrder(Order order) {
        // Save order
        orderRepository.save(order);
        
        // Save event to outbox (same transaction)
        OutboxEvent event = new OutboxEvent(
            UUID.randomUUID().toString(),
            order.getOrderId(),
            "OrderCreated",
            toJson(order),
            LocalDateTime.now(),
            false
        );
        outboxRepository.save(event);
        
        // If transaction fails, both rollback (atomic)
    }
}

@Scheduled(fixedDelay = 1000)
public void publishOutboxEvents() {
    List<OutboxEvent> events = outboxRepository.findByPublishedFalse();
    
    for (OutboxEvent event : events) {
        kafkaTemplate.send("orders", event.getAggregateId(), event.getPayload());
        
        event.setPublished(true);
        outboxRepository.save(event);
    }
}
```

**Better: Debezium CDC (Change Data Capture)**
- Captures changes from outbox table
- Publishes to Kafka automatically
- No polling needed

---

## Event Notification vs Event-Carried State Transfer

### Event Notification

Event contains minimal info (notification only).

```json
{
  "eventType": "OrderCreated",
  "orderId": "O123",
  "timestamp": "2026-07-01T10:00:00Z"
}
```

**Consumer must call service to get details:**
```java
@KafkaListener(topics = "orders")
public void handle(OrderCreatedEvent event) {
    // Call order-service REST API to get full order details
    Order order = restTemplate.getForObject(
        "http://order-service/orders/" + event.getOrderId(),
        Order.class
    );
}
```

**Pros:** Small messages, no data duplication  
**Cons:** Coupling (consumer depends on producer's API), latency

### Event-Carried State Transfer

Event contains full state.

```json
{
  "eventType": "OrderCreated",
  "orderId": "O123",
  "customerId": "C456",
  "items": [...],
  "totalAmount": 100.0,
  "timestamp": "2026-07-01T10:00:00Z"
}
```

**Consumer has all data in event:**
```java
@KafkaListener(topics = "orders")
public void handle(Order order) {
    // No API call needed, all data in event
    processOrder(order);
}
```

**Pros:** Decoupled (no API calls), faster  
**Cons:** Larger messages, data duplication

**Our project uses Event-Carried State Transfer** for better decoupling.

---

## Summary

| Pattern | Use Case | Complexity | Our Project Uses |
|---|---|---|---|
| **SAGA** | Distributed transactions | High | Yes (Orchestration) |
| **Choreography** | Simple workflows | Medium | No |
| **Orchestration** | Complex workflows | Medium | Yes |
| **Event Sourcing** | Audit, time travel | High | No (future) |
| **CQRS** | Scalable reads/writes | Medium | No (future) |
| **Outbox** | Atomic DB + Kafka | Medium | No (future) |

---

## Next Steps

1. [Kafka Streams](./04-kafka-streams.md) - Implement joins for SAGA orchestration
2. [Error Handling](./05-error-handling.md) - Handle failures and compensation
3. [Idempotency](./08-idempotency-transactions.md) - Exactly-once semantics

---

**Project Context:** This documentation supports the SAGA orchestration pattern in our order-service, payment-service, and stock-service microservices.
