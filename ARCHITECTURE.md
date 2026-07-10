# Spring Kafka Microservices - Architecture Documentation

## Project Overview

This is a **3-service microservices system** implementing the **SAGA pattern** with **Kafka Streams** for distributed transaction orchestration.

**Services:**
- `order-service` (Port 8081) - Order management + Kafka Streams orchestrator
- `payment-service` (Port 8082) - Payment processing
- `stock-service` (Port 8083) - Inventory management

**Infrastructure:**
- Kafka Broker (Port 9092)
- Zookeeper (Port 2181)
- Kafka UI (Port 8080)

---

## Complete Request Flow

### Step 1: User Creates Order
```
POST http://localhost:8081/api/orders
{
  "customerId": "CUST-001",
  "items": [
    {"productId": "PROD-001", "productName": "Laptop", "quantity": 2, "price": 1000.00}
  ]
}
```

**What happens:**
- `OrderController` receives request
- `OrderService` validates and saves order (status: PENDING)
- `OrderService` publishes `OrderCreatedEvent` → `order-created` topic

---

### Step 2: Payment Service Processes Payment
**File:** `payment-service/src/main/java/com/example/paymentservice/consumer/OrderEventConsumer.java`

```java
@KafkaListener(topics = "order-created", groupId = "payment-service-group")
public void consumeOrderCreatedEvent(OrderCreatedEvent event) {
    // Process payment
    PaymentProcessedEvent paymentEvent = processPayment(event);
    // Publish result → payment-events topic
    kafkaTemplate.send("payment-events", orderId, paymentEvent);
}
```

**Output:** `PaymentProcessedEvent` → `payment-events` topic
- Status: `ACCEPT` or `REJECT`
- Key: `orderId`

---

### Step 3: Stock Service Checks Inventory
**File:** `stock-service/src/main/java/com/example/stockservice/consumer/OrderEventConsumer.java`

```java
@KafkaListener(topics = "order-created", groupId = "stock-service-group")
public void consumeOrderCreatedEvent(OrderCreatedEvent event) {
    // Check stock availability
    StockProcessedEvent stockEvent = checkStock(event);
    // Publish result → stock-events topic
    kafkaTemplate.send("stock-events", orderId, stockEvent);
}
```

**Output:** `StockProcessedEvent` → `stock-events` topic
- Status: `ACCEPT` or `REJECT`
- Key: `orderId`

---

### Step 4: Kafka Streams Joins Payment + Stock Results

**File:** `order-service/src/main/java/com/example/orderservice/stream/OrderStreamProcessor.java`

This is the **KEY COMPONENT** that makes this Phase 5 (Kafka Streams orchestration).

```java
@Configuration
@EnableKafkaStreams
public class OrderStreamProcessor {
    
    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {
        // Create stream from payment-events topic
        KStream<String, PaymentProcessedEvent> paymentStream = 
            streamsBuilder.stream("payment-events", Consumed.with(Serdes.String(), paymentSerde));
        
        // Create stream from stock-events topic
        KStream<String, StockProcessedEvent> stockStream = 
            streamsBuilder.stream("stock-events", Consumed.with(Serdes.String(), stockSerde));
        
        // JOIN both streams within 10-second time window
        KStream<String, FinalDecisionEvent> decisionsStream = paymentStream.join(
            stockStream,
            (payment, stock) -> makeDecision(payment, stock),
            JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(10)),
            StreamJoined.with(Serdes.String(), paymentSerde, stockSerde)
        );
        
        // Publish final decision → order-events topic
        decisionsStream.to("order-events", Produced.with(Serdes.String(), decisionSerde));
    }
    
    private FinalDecisionEvent makeDecision(PaymentProcessedEvent payment, 
                                           StockProcessedEvent stock) {
        if (payment.ACCEPT && stock.ACCEPT) return CONFIRMED;
        if (payment.REJECT && stock.REJECT) return REJECTED;
        if (payment.ACCEPT && stock.REJECT) return ROLLBACK(source=STOCK);
        if (payment.REJECT && stock.ACCEPT) return ROLLBACK(source=PAYMENT);
    }
}
```

**Key Kafka Streams Concepts:**
- **KStream-KStream Join:** Correlates payment and stock events by `orderId` (key)
- **Time Window:** 10-second window - both events must arrive within 10s
- **RocksDB State Store:** Kafka Streams stores join state at `/tmp/kafka-streams`
- **Exactly-Once Semantics:** `processing.guarantee: exactly_once_v2` prevents duplicates

**Output:** `FinalDecisionEvent` → `order-events` topic

---

### Step 5: Services React to Final Decision

#### Payment Service
**File:** `payment-service/src/main/java/com/example/paymentservice/consumer/DecisionEventConsumer.java`

```java
@KafkaListener(topics = "order-events", groupId = "payment-decision-group")
public void consumeDecision(FinalDecisionEvent decision) {
    if (decision.status == CONFIRMED) {
        // Commit payment
        confirmPayment(decision.orderId);
    } else if (decision.status == ROLLBACK && decision.source == "STOCK") {
        // Rollback payment (stock failed)
        refundPayment(decision.orderId);
    }
}
```

#### Stock Service
**File:** `stock-service/src/main/java/com/example/stockservice/consumer/DecisionEventConsumer.java`

```java
@KafkaListener(topics = "order-events", groupId = "stock-decision-group")
public void consumeDecision(FinalDecisionEvent decision) {
    if (decision.status == CONFIRMED) {
        // Confirm stock reservation
        confirmStock(decision.orderId);
    } else if (decision.status == ROLLBACK && decision.source == "PAYMENT") {
        // Release stock (payment failed)
        releaseStock(decision.orderId);
    }
}
```

#### Order Service
**File:** `order-service/src/main/java/com/example/orderservice/consumer/DecisionEventConsumer.java`

```java
@KafkaListener(topics = "order-events", groupId = "order-decision-group")
public void consumeDecision(FinalDecisionEvent decision) {
    Order order = orderService.getOrderById(decision.orderId);
    order.setStatus(decision.status); // CONFIRMED / REJECTED / ROLLBACK
    orderRepository.save(order);
}
```

---

## Kafka Topics

| Topic | Producer | Consumer | Purpose |
|-------|----------|----------|---------|
| `order-created` | order-service | payment-service, stock-service | Broadcast new order |
| `payment-events` | payment-service | **Kafka Streams** (order-service) | Payment result |
| `stock-events` | stock-service | **Kafka Streams** (order-service) | Stock result |
| `order-events` | **Kafka Streams** (order-service) | payment-service, stock-service, order-service | Final decision |

---

## SAGA Decision Matrix

| Payment | Stock | Final Decision | Action |
|---------|-------|----------------|--------|
| ACCEPT | ACCEPT | **CONFIRMED** | Commit both |
| REJECT | REJECT | **REJECTED** | Nothing to rollback |
| ACCEPT | REJECT | **ROLLBACK** (source=STOCK) | Refund payment |
| REJECT | ACCEPT | **ROLLBACK** (source=PAYMENT) | Release stock |

---

## Key Implementation Files

### Order Service
```
order-service/
├── src/main/java/com/example/orderservice/
│   ├── controller/OrderController.java          # REST API endpoint
│   ├── service/OrderService.java                # Business logic
│   ├── stream/OrderStreamProcessor.java         # ⭐ KAFKA STREAMS JOIN
│   ├── consumer/DecisionEventConsumer.java      # Consumes final decisions
│   ├── event/
│   │   ├── OrderCreatedEvent.java
│   │   ├── PaymentProcessedEvent.java
│   │   ├── StockProcessedEvent.java
│   │   └── FinalDecisionEvent.java
│   └── config/KafkaProducerConfig.java
└── pom.xml                                      # ⭐ kafka-streams:3.8.0 dependency
```

### Payment Service
```
payment-service/
└── src/main/java/com/example/paymentservice/
    ├── consumer/
    │   ├── OrderEventConsumer.java              # Listens to order-created
    │   └── DecisionEventConsumer.java           # Listens to order-events
    ├── service/PaymentService.java
    └── event/PaymentProcessedEvent.java
```

### Stock Service
```
stock-service/
└── src/main/java/com/example/stockservice/
    ├── consumer/
    │   ├── OrderEventConsumer.java              # Listens to order-created
    │   └── DecisionEventConsumer.java           # Listens to order-events
    ├── service/StockService.java
    └── event/StockProcessedEvent.java
```

---

## Kafka Streams Configuration

**File:** `order-service/src/main/resources/application.yml`

```yaml
spring:
  kafka:
    streams:
      application-id: order-stream-processor        # Unique stream app ID
      bootstrap-servers: localhost:9092
      properties:
        processing.guarantee: exactly_once_v2       # Exactly-once semantics
        commit.interval.ms: 1000                    # State commit frequency
        state.dir: /tmp/kafka-streams               # RocksDB state location
```

---

## Running the System

### 1. Start Infrastructure
```bash
docker-compose up -d
# Starts: Kafka, Zookeeper, Kafka UI
```

### 2. Start Services
```bash
./start.sh
# Compiles and starts all 3 services
```

### 3. Create Test Order
```bash
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-001",
    "items": [
      {"productId": "PROD-001", "productName": "Laptop", "quantity": 2, "price": 1000.00}
    ]
  }'
```

### 4. Check Kafka Streams Logs
```bash
tail -f logs/order-service.log | grep -E "Joining events|Decision"
```

**Expected output:**
```
Joining events for orderId: abc-123 | Payment: ACCEPT, Stock: ACCEPT
Decision for abc-123: Both services ACCEPTED → CONFIRMED
SAGA Decision Made: orderId=abc-123, status=CONFIRMED
```

---

## Architecture Diagram

```
┌──────────────┐
│    User      │
└──────┬───────┘
       │ POST /api/orders
       ▼
┌─────────────────────────────────────────────┐
│         ORDER-SERVICE (8081)                │
│  ┌─────────────────────────────────────┐   │
│  │      OrderController                │   │
│  │           ↓                          │   │
│  │      OrderService                   │   │
│  └──────────┬──────────────────────────┘   │
└─────────────┼──────────────────────────────┘
              │ publishes OrderCreatedEvent
              ▼
       ┌──────────────┐
       │ order-created│ (Kafka Topic)
       └──┬────────┬──┘
          │        │
    ┌─────▼──┐  ┌─▼──────┐
    │PAYMENT │  │ STOCK  │
    │SERVICE │  │SERVICE │
    │ (8082) │  │ (8083) │
    └───┬────┘  └───┬────┘
        │           │
        │ publishes │ publishes
        │ Payment   │ Stock
        │ Event     │ Event
        ▼           ▼
  ┌───────────┐  ┌────────────┐
  │payment-   │  │stock-      │
  │events     │  │events      │
  └─────┬─────┘  └─────┬──────┘
        │              │
        └──────┬───────┘
               ▼
    ┏━━━━━━━━━━━━━━━━━━━━━━━━━┓
    ┃  KAFKA STREAMS          ┃
    ┃  (OrderStreamProcessor) ┃
    ┃                         ┃
    ┃  KStream.join(          ┃
    ┃    paymentStream,       ┃
    ┃    stockStream,         ┃
    ┃    10-second window     ┃
    ┃  )                      ┃
    ┃  → makeDecision()       ┃
    ┗━━━━━━━━━┯━━━━━━━━━━━━━━━┛
              │ publishes FinalDecisionEvent
              ▼
       ┌──────────────┐
       │ order-events │ (Kafka Topic)
       └──┬────┬───┬──┘
          │    │   │
    ┌─────▼┐ ┌─▼───▼─────┐
    │ORDER │ │PAYMENT    │
    │SRVC  │ │& STOCK    │
    │      │ │SERVICES   │
    └──────┘ └───────────┘
     Update    Commit/Rollback
     Status    Transactions
```

---

## Testing Scenarios

### Scenario 1: Both Accept → CONFIRMED
```bash
# Order: PROD-001, quantity=2, customer has $5000, stock available
# Payment: ACCEPT
# Stock: ACCEPT
# Result: CONFIRMED
```

### Scenario 2: Both Reject → REJECTED
```bash
# Order: PROD-001, quantity=100, customer has $10, stock=0
# Payment: REJECT (insufficient funds)
# Stock: REJECT (out of stock)
# Result: REJECTED
```

### Scenario 3: Payment OK, Stock Fails → ROLLBACK
```bash
# Order: PROD-001, quantity=50, customer has $5000, stock=10
# Payment: ACCEPT
# Stock: REJECT (insufficient stock)
# Result: ROLLBACK (source=STOCK) → Refund payment
```

### Scenario 4: Stock OK, Payment Fails → ROLLBACK
```bash
# Order: PROD-001, quantity=2, customer has $10, stock=100
# Payment: REJECT (insufficient funds)
# Stock: ACCEPT
# Result: ROLLBACK (source=PAYMENT) → Release stock reservation
```

---

## What Makes This "Kafka Streams"?

**NOT Kafka Streams (manual approach):**
```java
// Store events in memory manually
Map<String, PaymentEvent> paymentResults = new ConcurrentHashMap<>();
Map<String, StockEvent> stockResults = new ConcurrentHashMap<>();

public void handlePayment(PaymentEvent event) {
    paymentResults.put(event.orderId, event);
    if (stockResults.containsKey(event.orderId)) {
        // Manual correlation
        makeDecision(paymentResults.get(orderId), stockResults.get(orderId));
    }
}
```

**Kafka Streams (current implementation):**
```java
// Kafka Streams handles correlation, windowing, state management
KStream<String, FinalDecisionEvent> decisions = paymentStream.join(
    stockStream,
    (payment, stock) -> makeDecision(payment, stock),
    JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(10))
);
```

**Benefits:**
- ✅ Automatic state management (RocksDB)
- ✅ Time-based windowing
- ✅ Exactly-once guarantees
- ✅ Horizontal scalability
- ✅ Fault tolerance

---

## Dependencies

### Order Service (pom.xml)
```xml
<!-- Apache Kafka Streams -->
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-streams</artifactId>
    <version>3.8.0</version>
</dependency>

<!-- Spring Kafka -->
<dependency>
    <groupId>org.springframework.kafka</groupId>
    <artifactId>spring-kafka</artifactId>
    <version>3.3.2</version>
</dependency>
```

---

## Summary

**What this project implements:**

1. **3-Service Microservices Architecture** with order, payment, and stock services
2. **SAGA Pattern** for distributed transactions
3. **Kafka Streams Orchestration** with KStream-KStream joins
4. **Event-Driven Communication** via Kafka topics
5. **Exactly-Once Semantics** for reliable processing
6. **Compensating Transactions** (rollback logic)

**Key Point:** The `OrderStreamProcessor` with `KStream.join()` is what makes this a **Kafka Streams implementation**. Without it, this would be manual event correlation (Phase 4).
