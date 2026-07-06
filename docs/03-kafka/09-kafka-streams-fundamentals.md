# Kafka Streams Fundamentals

## Table of Contents
1. [What is Kafka Streams?](#what-is-kafka-streams)
2. [Kafka Streams vs @KafkaListener](#kafka-streams-vs-kafkalistener)
3. [Core Concepts](#core-concepts)
4. [Stream Operations](#stream-operations)
5. [Stateful Processing](#stateful-processing)
6. [Windowing Operations](#windowing-operations)
7. [Joins in Kafka Streams](#joins-in-kafka-streams)
8. [Spring Kafka Streams Configuration](#spring-kafka-streams-configuration)
9. [Practical Examples](#practical-examples)
10. [Common Patterns](#common-patterns)
11. [Testing Kafka Streams](#testing-kafka-streams)
12. [Production Considerations](#production-considerations)

---

## What is Kafka Streams?

### Definition

Kafka Streams is a **client library** for building real-time stream processing applications that process data stored in Kafka topics.

**Key Characteristics:**
- **No separate cluster** - Runs as part of your application (unlike Spark/Flink)
- **Exactly-once semantics** - Guaranteed delivery with no duplicates
- **Fault-tolerant** - Automatic state recovery and rebalancing
- **Elastic** - Scale up/down by adding/removing instances
- **Interactive queries** - Query the state of your streams in real-time

**Real-World Use Cases:**
1. **SAGA Orchestration** (our project) - Join payment + stock responses before confirming order
2. **Real-time Analytics** - Windowed aggregations, counting, averaging
3. **Stream Enrichment** - Join streams with reference data (KTable)
4. **Fraud Detection** - Pattern matching across event streams
5. **Monitoring & Alerting** - Compute metrics from event streams

### Why Kafka Streams?

**Before Kafka Streams (Problems):**
```java
// Using @KafkaListener for SAGA orchestration
@Service
public class OrderOrchestrator {
    private Map<String, OrderState> pendingOrders = new ConcurrentHashMap<>();
    
    @KafkaListener(topics = "payment-orders")
    public void handlePayment(PaymentResponse payment) {
        OrderState state = pendingOrders.get(payment.getOrderId());
        state.setPaymentStatus(payment.getStatus());
        
        // Need to check if we have stock response too
        if (state.hasStockResponse()) {
            completeOrder(state);  // Both responses received
        }
        // What if payment-orders arrives before stock-orders?
        // What if service crashes before receiving stock response?
        // State stored in-memory → LOST on restart!
    }
    
    @KafkaListener(topics = "stock-orders")
    public void handleStock(StockResponse stock) {
        // Similar coordination logic...
        // Duplicate code, manual state management, no fault tolerance
    }
}
```

**Problems:**
1. **Manual state management** - You manage in-memory maps/caches
2. **No fault tolerance** - State lost on crash
3. **Coordination complexity** - Manually join payment + stock responses
4. **No exactly-once** - Duplicates possible on rebalance
5. **Scalability limits** - Hard to partition state across instances

**After Kafka Streams (Solution):**
```java
@Bean
public KStream<String, Order> orderSaga(StreamsBuilder builder) {
    // Input streams
    KStream<String, Order> orders = builder.stream("orders");
    KTable<String, PaymentResponse> payments = builder.table("payment-orders");
    KTable<String, StockResponse> stocks = builder.table("stock-orders");
    
    // Join payment + stock responses (Kafka Streams handles coordination!)
    return orders
        .join(payments, (order, payment) -> order.withPayment(payment))
        .join(stocks, (order, stock) -> order.withStock(stock))
        .mapValues(order -> {
            // Both payment + stock responses available
            if (order.getPaymentStatus() == ACCEPT && order.getStockStatus() == ACCEPT) {
                order.setStatus(OrderStatus.CONFIRMED);
            } else {
                order.setStatus(OrderStatus.ROLLBACK);
            }
            return order;
        })
        .to("order-confirmations");  // Output confirmed/rejected orders
}
```

**Benefits:**
- ✅ **Automatic state management** - State stored in RocksDB (local disk)
- ✅ **Fault-tolerant** - State backed to Kafka changelog topic
- ✅ **Built-in joins** - Declarative stream-stream and stream-table joins
- ✅ **Exactly-once** - Handled by Kafka Streams framework
- ✅ **Scalable** - State partitioned across instances automatically

---

## Kafka Streams vs @KafkaListener

### When to Use Each

| Feature | @KafkaListener | Kafka Streams |
|---|---|---|
| **Use Case** | Simple consume-process-produce | Complex stream processing |
| **State Management** | Manual (in-memory, DB) | Automatic (local state stores) |
| **Fault Tolerance** | Manual (external DB) | Built-in (changelog topics) |
| **Joins** | Manual coordination | Built-in join operators |
| **Windowing** | Manual aggregation | Built-in window operators |
| **Exactly-Once** | Manual (idempotency checks) | Built-in (transactional) |
| **Complexity** | Low | Medium-High |
| **Latency** | Low (10-100ms) | Low-Medium (50-200ms) |
| **Throughput** | High | Very High |

### Decision Matrix

**Use @KafkaListener when:**
- ✅ Simple consume → process → produce (stateless)
- ✅ No need for joins or aggregations
- ✅ Low learning curve required
- ✅ Integration with external systems (REST calls, databases)

**Example:**
```java
// Perfect for @KafkaListener - Simple notification service
@KafkaListener(topics = "order-confirmed")
public void sendEmail(Order order) {
    emailService.send(order.getCustomerEmail(), "Order confirmed!");
}
```

**Use Kafka Streams when:**
- ✅ Joining multiple streams (payment + stock responses)
- ✅ Aggregations (count orders per customer)
- ✅ Windowed operations (orders per hour)
- ✅ Stateful transformations (enrichment, filtering)
- ✅ Need exactly-once guarantees
- ✅ Building event-driven workflows (SAGA)

**Example:**
```java
// Perfect for Kafka Streams - Join payment + stock
orders
    .join(payments, ...)
    .join(stocks, ...)
    .to("order-results");
```

### Can You Use Both?

**Yes! Common pattern:**

```java
// Kafka Streams for complex orchestration
@Bean
public KStream<String, Order> orderOrchestration(StreamsBuilder builder) {
    return orders
        .join(payments, ...)
        .join(stocks, ...)
        .to("order-confirmations");
}

// @KafkaListener for side effects (DB, email, etc.)
@KafkaListener(topics = "order-confirmations")
public void handleConfirmation(Order order) {
    orderRepository.save(order);           // Save to DB
    emailService.send(order.getEmail());   // Send email
    metricsService.record("order.confirmed");  // Record metric
}
```

**Why this works:**
- Kafka Streams handles **stream processing logic** (stateful, fault-tolerant)
- @KafkaListener handles **integration logic** (DB writes, external APIs)

---

## Core Concepts

### 1. KStream (Event Stream)

A **KStream** represents an unbounded, continuously updating stream of events. Each record is an **independent event**.

**Characteristics:**
- **Append-only** - Records never updated
- **Unbounded** - Infinite stream of events
- **All records matter** - Every event processed

**Think of KStream as:** Transaction log, click stream, sensor readings

**Example:**
```java
KStream<String, Order> orders = builder.stream("orders");

// Input topic "orders":
// [OrderCreated(id=1, amount=100), OrderCreated(id=2, amount=200), OrderCreated(id=3, amount=150)]

// Each record is independent:
// - Record 1: New order from customer A
// - Record 2: New order from customer B
// - Record 3: New order from customer A (separate from Record 1)
```

**Java API:**
```java
// Create KStream from topic
KStream<String, Order> orders = builder.stream("orders");

// Operations (return new KStream)
KStream<String, Order> filtered = orders.filter((key, order) -> order.getAmount() > 100);
KStream<String, String> mapped = orders.mapValues(order -> order.getCustomerId());
```

### 2. KTable (Changelog Stream / State Table)

A **KTable** represents a **changelog stream** where each record is an **update** to the current state. Only the **latest value per key** matters.

**Characteristics:**
- **Updates** - New records update existing state
- **Bounded** - Finite set of keys (though unbounded over time)
- **Latest value matters** - Only current state is relevant

**Think of KTable as:** Database table, cache, latest status per key

**Example:**
```java
KTable<String, PaymentResponse> payments = builder.table("payment-orders");

// Input topic "payment-orders":
// [Payment(orderId=1, status=PENDING), Payment(orderId=2, status=ACCEPT), Payment(orderId=1, status=ACCEPT)]
//                                                                           ↑
//                                                                    Updates orderId=1

// KTable state after all records:
// {
//   "1": Payment(status=ACCEPT),   // Latest value for key "1"
//   "2": Payment(status=ACCEPT)
// }
// 
// The first PENDING status for orderId=1 is overwritten by ACCEPT
```

**Key Difference:**
```
KStream: [Event1, Event2, Event3, ...]  (all events kept)
KTable:  {key1: LatestValue1, key2: LatestValue2}  (only latest per key)
```

**Java API:**
```java
// Create KTable from topic (compacted topic recommended)
KTable<String, PaymentResponse> payments = builder.table("payment-orders");

// Convert KStream → KTable (group by key, aggregate)
KTable<String, Long> orderCounts = orders
    .groupByKey()
    .count();

// Convert KTable → KStream (emit all updates as events)
KStream<String, PaymentResponse> paymentStream = payments.toStream();
```

### 3. GlobalKTable (Replicated State Table)

A **GlobalKTable** is like a KTable but replicated to **all instances** (not partitioned).

**When to use:**
- Reference data (small datasets: currencies, product catalog)
- Need to join on non-key field
- Data fits in memory on every instance

**Example:**
```java
// Product catalog (small dataset)
GlobalKTable<String, Product> products = builder.globalTable("products");

// Join orders with products (join on productId, not key)
orders.join(products,
    (orderId, order) -> order.getProductId(),  // Key extractor
    (order, product) -> order.withProduct(product)
);
```

**KTable vs GlobalKTable:**

| Feature | KTable | GlobalKTable |
|---|---|---|
| **Partitioning** | Partitioned (like topic) | Replicated to all instances |
| **Memory** | Partial data per instance | Full data on every instance |
| **Join Key** | Must match record key | Can join on any field |
| **Use Case** | Large datasets | Small reference data |

### 4. State Stores (Local State)

**State stores** are local databases (in-memory or RocksDB) that Kafka Streams uses to store intermediate state.

**Why needed?**
- Aggregations (count, sum)
- Joins (store one side to join with other)
- Windowing (maintain window state)

**Types:**

**1. In-Memory State Store:**
```java
StoreBuilder<KeyValueStore<String, Long>> storeBuilder = 
    Stores.keyValueStoreBuilder(
        Stores.inMemoryKeyValueStore("order-counts"),
        Serdes.String(),
        Serdes.Long()
    );
builder.addStateStore(storeBuilder);
```

**Pros:** Fast (no disk I/O)
**Cons:** Limited by heap size, lost on crash (rebuilt from changelog)

**2. RocksDB State Store (Default):**
```java
StoreBuilder<KeyValueStore<String, Order>> storeBuilder = 
    Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore("orders-store"),
        Serdes.String(),
        orderSerde
    );
builder.addStateStore(storeBuilder);
```

**Pros:** Scales beyond memory, persisted to disk
**Cons:** Slower than in-memory (disk I/O)

**State Store Properties:**
- **Local** - Each instance has its own state store (partitioned)
- **Fault-tolerant** - State backed to Kafka changelog topic
- **Queryable** - Can query state via Interactive Queries API

**Changelog Topics:**

Kafka Streams automatically creates **changelog topics** to back up state stores:

```
State Store: "order-counts-store"
   ↓
Changelog Topic: "app-order-counts-store-changelog"
```

**How it works:**
```
1. State store update: orders-store.put("order-1", order)
2. Kafka Streams writes to changelog: "app-orders-store-changelog"
3. On crash: New instance reads changelog to rebuild state
```

**Configuration:**
```yaml
spring:
  kafka:
    streams:
      properties:
        # State store directory
        state.dir: /tmp/kafka-streams
        
        # Changelog topic replication
        replication.factor: 3
```

### 5. Topology (Stream Processing DAG)

A **topology** is a directed acyclic graph (DAG) of stream processing operations.

**Visual Representation:**
```
Source Topic: orders
     ↓
  filter (amount > 100)
     ↓
  mapValues (enrich with customer data)
     ↓
  groupByKey
     ↓
  count (aggregate)
     ↓
Sink Topic: order-counts
```

**Java Code:**
```java
StreamsBuilder builder = new StreamsBuilder();

KStream<String, Order> orders = builder.stream("orders");

orders
    .filter((key, order) -> order.getAmount() > 100)
    .mapValues(order -> enrichOrder(order))
    .groupByKey()
    .count()
    .toStream()
    .to("order-counts");

Topology topology = builder.build();
System.out.println(topology.describe());  // Print topology
```

**Topology Description Output:**
```
Topologies:
   Sub-topology: 0
    Source: KSTREAM-SOURCE-0000000000 (topics: [orders])
      --> KSTREAM-FILTER-0000000001
    Processor: KSTREAM-FILTER-0000000001 (stores: [])
      --> KSTREAM-MAPVALUES-0000000002
      <-- KSTREAM-SOURCE-0000000000
    Processor: KSTREAM-MAPVALUES-0000000002 (stores: [])
      --> KSTREAM-AGGREGATE-0000000003
      <-- KSTREAM-FILTER-0000000001
    ...
```

---

## Stream Operations

### 1. Stateless Operations (No State Store Required)

#### filter / filterNot

```java
// Keep only high-value orders
KStream<String, Order> highValueOrders = orders
    .filter((key, order) -> order.getAmount() > 1000);

// Reject fraud orders
KStream<String, Order> validOrders = orders
    .filterNot((key, order) -> fraudService.isFraud(order));
```

#### map / mapValues

```java
// map: Transform both key and value (may change key → repartition)
KStream<String, String> result = orders
    .map((key, order) -> 
        KeyValue.pair(order.getCustomerId(), order.getOrderId()));

// mapValues: Transform only value (no repartition, more efficient)
KStream<String, OrderDTO> dtos = orders
    .mapValues(order -> new OrderDTO(order));
```

**Key vs Value Transformation:**
```
map():
  Before: (orderId="O1", Order{customerId="C1", amount=100})
  After:  (customerId="C1", orderId="O1")
  ↑ Key changed → Triggers repartition (expensive!)

mapValues():
  Before: (orderId="O1", Order{customerId="C1", amount=100})
  After:  (orderId="O1", OrderDTO{...})
  ↑ Key unchanged → No repartition (efficient!)
```

**Rule:** Use `mapValues()` instead of `map()` when key doesn't change.

#### flatMap / flatMapValues

```java
// flatMap: One input record → Multiple output records
KStream<String, Item> items = orders
    .flatMap((key, order) -> 
        order.getItems().stream()
            .map(item -> KeyValue.pair(item.getProductId(), item))
            .collect(Collectors.toList())
    );

// Example:
// Input:  Order(id=O1, items=[Item1, Item2, Item3])
// Output: [
//   (productId=P1, Item1),
//   (productId=P2, Item2),
//   (productId=P3, Item3)
// ]
```

#### branch

```java
// Split stream into multiple branches
Map<String, KStream<String, Order>> branches = orders
    .split()
    .branch((key, order) -> order.getAmount() > 1000, 
        Branched.as("high-value"))
    .branch((key, order) -> order.getAmount() > 100, 
        Branched.as("medium-value"))
    .defaultBranch(Branched.as("low-value"));

KStream<String, Order> highValue = branches.get("high-value");
KStream<String, Order> mediumValue = branches.get("medium-value");
KStream<String, Order> lowValue = branches.get("low-value");

// Route to different topics
highValue.to("high-value-orders");
mediumValue.to("medium-value-orders");
lowValue.to("low-value-orders");
```

#### merge

```java
// Combine multiple streams
KStream<String, Order> onlineOrders = builder.stream("online-orders");
KStream<String, Order> retailOrders = builder.stream("retail-orders");

KStream<String, Order> allOrders = onlineOrders.merge(retailOrders);
```

#### peek

```java
// Side effect (logging, debugging) without modifying stream
orders
    .peek((key, order) -> log.info("Processing order: {}", key))
    .filter(...)
    .peek((key, order) -> log.info("After filter: {}", key))
    .to("output");
```

### 2. Stateful Operations (Require State Store)

#### groupByKey / groupBy

```java
// groupByKey: Group by existing key (no repartition)
KGroupedStream<String, Order> grouped = orders.groupByKey();

// groupBy: Group by custom key (triggers repartition)
KGroupedStream<String, Order> byCustomer = orders
    .groupBy((key, order) -> order.getCustomerId());
```

**Repartitioning:**
```
Original Topic: orders (key=orderId)
Partition 0: [Order1(orderId=O1, customerId=C2)]
Partition 1: [Order2(orderId=O2, customerId=C1)]
Partition 2: [Order3(orderId=O3, customerId=C1)]

After groupBy(customerId):
Repartition Topic: orders-REPARTITION (key=customerId)
Partition 0: [Order1(customerId=C2)]
Partition 1: [Order2(customerId=C1), Order3(customerId=C1)]
                     ↑
        Orders from same customer now in same partition
```

#### count

```java
// Count records per key
KTable<String, Long> orderCounts = orders
    .groupBy((key, order) -> order.getCustomerId())
    .count();

// Example:
// Input:  [Order(C1), Order(C1), Order(C2), Order(C1)]
// Output: {C1: 3, C2: 1}
```

#### aggregate

```java
// Custom aggregation
KTable<String, OrderStats> stats = orders
    .groupBy((key, order) -> order.getCustomerId())
    .aggregate(
        OrderStats::new,  // Initializer (starting value)
        (key, order, aggregate) -> {  // Adder (process each record)
            aggregate.incrementCount();
            aggregate.addAmount(order.getAmount());
            return aggregate;
        },
        Materialized.with(Serdes.String(), orderStatsSerde)
    );

// Example:
// Input:  [Order(C1, amount=100), Order(C1, amount=200)]
// Output: {C1: OrderStats(count=2, totalAmount=300)}
```

**Aggregate Execution:**
```
Step 1: Initialize
  state = OrderStats(count=0, totalAmount=0)

Step 2: Process Order(C1, amount=100)
  state.incrementCount()       // count=1
  state.addAmount(100)         // totalAmount=100

Step 3: Process Order(C1, amount=200)
  state.incrementCount()       // count=2
  state.addAmount(200)         // totalAmount=300

Final state: {C1: OrderStats(count=2, totalAmount=300)}
```

#### reduce

```java
// Reduce to single value (similar to aggregate, but value type must match)
KTable<String, Order> latestOrders = orders
    .groupByKey()
    .reduce((oldOrder, newOrder) -> newOrder);  // Keep latest order
```

---

## Windowing Operations

### Why Windowing?

**Problem:** How to aggregate unbounded streams?

```java
// This doesn't make sense for unbounded stream:
orders.groupByKey().count()  // Count grows forever!
```

**Solution:** Group events into **windows** (time buckets) and aggregate per window.

### Window Types

#### 1. Tumbling Windows (Fixed, Non-Overlapping)

```
Timeline: 0─────5─────10────15────20────25────30
Window 1: [─────]
Window 2:       [─────]
Window 3:             [─────]
Window 4:                   [─────]
```

**Use case:** "Count orders every 5 minutes"

```java
KTable<Windowed<String>, Long> windowedCounts = orders
    .groupByKey()
    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
    .count();

// Example:
// Time 00:00-00:05: 10 orders → Window1: 10
// Time 00:05-00:10: 15 orders → Window2: 15
// Time 00:10-00:15: 8 orders  → Window3: 8
```

#### 2. Hopping Windows (Fixed, Overlapping)

```
Timeline: 0─────5─────10────15────20────25────30
Window 1: [───────────]
Window 2:       [───────────]
Window 3:             [───────────]
```

**Use case:** "Count orders in 10-minute windows, updated every 5 minutes"

```java
KTable<Windowed<String>, Long> hoppingCounts = orders
    .groupByKey()
    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(10))
        .advanceBy(Duration.ofMinutes(5)))
    .count();

// Example:
// Window1 (00:00-00:10): 20 orders
// Window2 (00:05-00:15): 25 orders (includes orders from 00:05-00:10 again)
// Window3 (00:10-00:20): 18 orders
```

#### 3. Session Windows (Gap-Based)

```
Timeline: 0───1───2───────────8───9─────────15──16
Events:   [E1][E2]            [E3][E4]       [E5][E6]
Session1: [─────]  (gap > 5 min)
Session2:                     [─────]  (gap > 5 min)
Session3:                                   [─────]
```

**Use case:** "Group user activity into sessions (5-minute inactivity = session end)"

```java
KTable<Windowed<String>, Long> sessionCounts = orders
    .groupByKey()
    .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMinutes(5)))
    .count();

// Example:
// User C1:
//   00:00: Order1
//   00:02: Order2
//   (5 min gap)
//   00:10: Order3  → New session starts
// 
// Sessions:
//   Session1 (00:00-00:02): 2 orders
//   Session2 (00:10-00:10): 1 order
```

### Grace Period (Late Events)

**Problem:** Events may arrive late due to network delays.

```
Event Time: 10:05:30
Arrival Time: 10:11:00  (late by 5.5 minutes)
```

**Grace Period:** Allow late events within grace period to update windows.

```java
TimeWindows.ofSizeAndGrace(
    Duration.ofMinutes(5),    // Window size
    Duration.ofMinutes(1)     // Grace period
)

// Window 10:00-10:05:
//   10:05:00 → Window closes
//   10:06:00 → Grace period ends (window finalized)
//   
// Events arriving before 10:06:00 update the window
// Events arriving after 10:06:00 are dropped
```

**Trade-off:**
- **No grace** (`ofSizeWithNoGrace`): Fast, but drops late events
- **Long grace**: Includes late events, but delays window finalization

### Window Retention

**State stores keep window state for retention period:**

```java
TimeWindows
    .ofSizeAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(1))
    .until(Duration.ofDays(7));  // Keep window state for 7 days

// After 7 days, old window state is deleted
```

---

## Joins in Kafka Streams

### Join Types Overview

| Join Type | Left Input | Right Input | When to Use |
|---|---|---|---|
| **Stream-Stream** | KStream | KStream | Join events within time window |
| **Stream-Table** | KStream | KTable | Enrich stream with latest state |
| **Table-Table** | KTable | KTable | Join two changelogs |
| **Stream-GlobalTable** | KStream | GlobalKTable | Enrich with reference data (any key) |

### 1. Stream-Stream Join (Windowed Join)

**Use case:** Join payment events with stock events within a time window.

**Key Requirement:** Events must arrive **within the time window**.

```java
KStream<String, PaymentResponse> payments = builder.stream("payment-orders");
KStream<String, StockResponse> stocks = builder.stream("stock-orders");

// Inner Join (both sides required)
KStream<String, OrderResult> results = payments.join(
    stocks,
    (payment, stock) -> new OrderResult(payment, stock),  // ValueJoiner
    JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)),  // Window
    StreamJoined.with(Serdes.String(), paymentSerde, stockSerde)
);

// Timeline:
// 10:00:00 → Payment(orderId=O1, status=ACCEPT)
// 10:02:00 → Stock(orderId=O1, status=ACCEPT)
// 
// Result: Joined within 5-minute window
// OrderResult(payment=ACCEPT, stock=ACCEPT)
```

**Window Semantics:**
```
Payment arrives at T=0
Window: [T-5 min, T+5 min]

Stock arrives at T+2 min → JOINED (within window)
Stock arrives at T+6 min → NOT JOINED (outside window)
```

**Join Types:**
```java
// Inner Join (both sides required)
payments.join(stocks, ...)

// Left Join (payment always emitted, stock may be null)
payments.leftJoin(stocks, (payment, stock) -> {
    if (stock == null) {
        return new OrderResult(payment, null);  // Stock not yet received
    }
    return new OrderResult(payment, stock);
})

// Outer Join (both sides emitted, either may be null)
payments.outerJoin(stocks, ...)
```

**State Store Requirement:**

Stream-stream joins maintain **two state stores** (one per stream):

```
State Store 1: payments-store
  {O1: Payment(status=ACCEPT)}  (waiting for stock)

State Store 2: stocks-store
  {O2: Stock(status=ACCEPT)}  (waiting for payment)

When stock(O1) arrives:
  1. Lookup payments-store.get("O1")
  2. Join: Payment(O1) + Stock(O1)
  3. Emit result
```

### 2. Stream-Table Join (Enrich Stream)

**Use case:** Enrich order stream with latest customer data.

```java
KStream<String, Order> orders = builder.stream("orders");
KTable<String, Customer> customers = builder.table("customers");

// Enrich order with customer data
KStream<String, EnrichedOrder> enrichedOrders = orders
    .join(
        customers,
        (order, customer) -> new EnrichedOrder(order, customer)
    );

// Example:
// Order: {orderId=O1, customerId=C1, amount=100}
// Customer Table: {C1: Customer(name="Alice", tier="Gold")}
// 
// Result: EnrichedOrder(orderId=O1, customerName="Alice", tier="Gold", amount=100)
```

**Key Requirement:** Join key must match.

```java
// BAD: Keys don't match (order key=orderId, customer key=customerId)
orders.join(customers, ...)  // ERROR: Keys mismatch

// GOOD: Rekey orders by customerId first
orders
    .selectKey((key, order) -> order.getCustomerId())  // Repartition
    .join(customers, ...)
```

**Join Semantics:**
```
KTable: Latest value per key
  {C1: Customer(tier="Gold")}

KStream event: Order(customerId=C1)
  → Lookup KTable: customers.get("C1")
  → Join: Order + Customer(tier="Gold")
```

**Join Types:**
```java
// Inner Join (customer must exist)
orders.join(customers, ...)

// Left Join (order emitted even if customer not found)
orders.leftJoin(customers, (order, customer) -> {
    if (customer == null) {
        return new EnrichedOrder(order, null);  // New customer
    }
    return new EnrichedOrder(order, customer);
})
```

### 3. Table-Table Join (Join Two Changelogs)

**Use case:** Join user profile changes with preferences changes.

```java
KTable<String, UserProfile> profiles = builder.table("user-profiles");
KTable<String, UserPreferences> preferences = builder.table("user-preferences");

// Join two tables
KTable<String, UserData> userData = profiles
    .join(
        preferences,
        (profile, pref) -> new UserData(profile, pref)
    );

// Example:
// Profiles:    {U1: Profile(name="Alice")}
// Preferences: {U1: Preferences(theme="dark")}
// 
// Result: {U1: UserData(name="Alice", theme="dark")}
// 
// When profile updates:
// Profiles:    {U1: Profile(name="Alice Smith")}
// Result:      {U1: UserData(name="Alice Smith", theme="dark")}
```

**Behavior:**
```
State at T=0:
  Profiles:    {U1: Profile(name="Alice")}
  Preferences: {U1: Preferences(theme="dark")}
  Result:      {U1: UserData(name="Alice", theme="dark")}

Update at T=1:
  Profiles:    {U1: Profile(name="Alice Smith")}
  Result:      {U1: UserData(name="Alice Smith", theme="dark")}  ← Updated

Update at T=2:
  Preferences: {U1: Preferences(theme="light")}
  Result:      {U1: UserData(name="Alice Smith", theme="light")}  ← Updated
```

### 4. Stream-GlobalTable Join (Join on Any Field)

**Use case:** Enrich orders with product data (join on productId, not key).

```java
KStream<String, Order> orders = builder.stream("orders");  // Key=orderId
GlobalKTable<String, Product> products = builder.globalTable("products");  // Key=productId

// Join on productId (not key!)
KStream<String, EnrichedOrder> enriched = orders
    .join(
        products,
        (orderId, order) -> order.getProductId(),  // Key extractor
        (order, product) -> new EnrichedOrder(order, product)
    );

// Example:
// Order: {orderId=O1, productId=P1, quantity=2}
// Products: {P1: Product(name="Laptop", price=1000)}
// 
// Result: EnrichedOrder(orderId=O1, productName="Laptop", totalPrice=2000)
```

**Why GlobalKTable?**

```
KTable (Partitioned):
  Instance 1: {P1, P3, P5}  (Partition 0, 2, 4)
  Instance 2: {P2, P4}      (Partition 1, 3)
  
  Order(orderId=O1, productId=P2) arrives at Instance 1
    → P2 not available! (It's on Instance 2)

GlobalKTable (Replicated):
  Instance 1: {P1, P2, P3, P4, P5}  (All products)
  Instance 2: {P1, P2, P3, P4, P5}  (All products)
  
  Order(orderId=O1, productId=P2) arrives at Instance 1
    → P2 available! (Replicated to all instances)
```

---

## Spring Kafka Streams Configuration

### application.yml

```yaml
spring:
  application:
    name: order-service
  
  kafka:
    bootstrap-servers: localhost:9092
    
    # Kafka Streams specific configuration
    streams:
      application-id: ${spring.application.name}
      # Unique ID for this Streams app (used for consumer group, state store names)
      
      bootstrap-servers: ${spring.kafka.bootstrap-servers}
      
      # State store configuration
      properties:
        # State directory (local disk for RocksDB)
        state.dir: /tmp/kafka-streams/${spring.application.name}
        
        # Commit interval (how often to commit offsets)
        commit.interval.ms: 1000
        
        # Replication for changelog topics
        replication.factor: 1  # Use 3 in production
        
        # Exactly-once semantics
        processing.guarantee: exactly_once_v2
        
        # Topology optimization
        topology.optimization: all
        
        # Number of stream threads
        num.stream.threads: 2
        
        # Cache size (per thread)
        cache.max.bytes.buffering: 10485760  # 10 MB
        
        # Default serdes
        default.key.serde: org.apache.kafka.common.serialization.Serdes$StringSerde
        default.value.serde: org.springframework.kafka.support.serializer.JsonSerde

logging:
  level:
    org.apache.kafka.streams: INFO
    org.springframework.kafka.streams: DEBUG
```

### Java Configuration

```java
package com.example.orderservice.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {
    
    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-service");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        
        return new KafkaStreamsConfiguration(props);
    }
}
```

### Topology Bean

```java
package com.example.orderservice.streams;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OrderStreamsTopology {
    
    @Bean
    public KStream<String, Order> orderStream(StreamsBuilder builder) {
        // Define topology
        KStream<String, Order> orders = builder.stream("orders");
        
        orders
            .filter((key, order) -> order.getAmount() > 100)
            .mapValues(order -> order.withStatus(OrderStatus.VALIDATED))
            .to("validated-orders");
        
        return orders;  // Return for testing purposes
    }
}
```

**Spring Boot auto-starts the topology:**
```
Application Startup:
  1. @EnableKafkaStreams initializes Kafka Streams
  2. StreamsBuilder bean created
  3. @Bean methods using StreamsBuilder build topology
  4. Topology started automatically
```

---

## Practical Examples

### Example 1: Simple Filtering and Transformation

**Scenario:** Filter high-value orders and enrich with customer tier.

```java
@Configuration
public class OrderFilterTopology {
    
    @Bean
    public KStream<String, EnrichedOrder> filterOrders(StreamsBuilder builder) {
        KStream<String, Order> orders = builder.stream("orders");
        KTable<String, Customer> customers = builder.table("customers");
        
        return orders
            // Step 1: Filter high-value orders
            .filter((key, order) -> order.getAmount() > 1000)
            
            // Step 2: Rekey by customerId to join with customers table
            .selectKey((key, order) -> order.getCustomerId())
            
            // Step 3: Enrich with customer data
            .join(customers, (order, customer) -> 
                new EnrichedOrder(order, customer.getTier()))
            
            // Step 4: Write to output topic
            .peek((key, enriched) -> 
                log.info("High-value order: {} from tier {} customer", 
                    enriched.getOrderId(), enriched.getCustomerTier()))
            
            .to("high-value-orders");
    }
}
```

**Flow:**
```
Input: orders topic
  Order(orderId=O1, customerId=C1, amount=1500)

Step 1: Filter
  amount > 1000 → PASS

Step 2: Rekey
  Key changed: orderId=O1 → customerId=C1

Step 3: Join with customers
  Customer(C1, tier="Gold")
  → EnrichedOrder(orderId=O1, customerId=C1, amount=1500, tier="Gold")

Output: high-value-orders topic
```

### Example 2: Counting Events (Aggregation)

**Scenario:** Count orders per customer in 5-minute windows.

```java
@Configuration
public class OrderCountTopology {
    
    @Bean
    public KTable<Windowed<String>, Long> countOrders(StreamsBuilder builder) {
        KStream<String, Order> orders = builder.stream("orders");
        
        KTable<Windowed<String>, Long> windowedCounts = orders
            // Group by customerId
            .groupBy((key, order) -> order.getCustomerId())
            
            // 5-minute tumbling windows
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
            
            // Count orders per customer per window
            .count(Materialized.as("order-counts-store"));
        
        // Convert to stream and log
        windowedCounts.toStream()
            .foreach((windowedKey, count) -> {
                String customerId = windowedKey.key();
                long windowStart = windowedKey.window().start();
                long windowEnd = windowedKey.window().end();
                log.info("Customer {} had {} orders in window [{}-{}]",
                    customerId, count, windowStart, windowEnd);
            });
        
        return windowedCounts;
    }
}
```

**Example Output:**
```
Input events:
  10:01 → Order(customerId=C1)
  10:02 → Order(customerId=C1)
  10:03 → Order(customerId=C2)
  10:06 → Order(customerId=C1)  (New window)

Logs:
  Customer C1 had 2 orders in window [10:00-10:05]
  Customer C2 had 1 orders in window [10:00-10:05]
  Customer C1 had 1 orders in window [10:05-10:10]
```

### Example 3: Stream-Stream Join (SAGA Orchestration)

**Scenario:** Join payment and stock responses to confirm/reject order.

```java
@Configuration
public class OrderSagaTopology {
    
    @Autowired
    private ObjectMapper objectMapper;
    
    @Bean
    public KStream<String, OrderResult> orderSaga(StreamsBuilder builder) {
        // Input streams
        KStream<String, Order> orders = builder.stream("orders",
            Consumed.with(Serdes.String(), orderSerde()));
        
        KStream<String, PaymentResponse> payments = builder.stream("payment-orders",
            Consumed.with(Serdes.String(), paymentSerde()));
        
        KStream<String, StockResponse> stocks = builder.stream("stock-orders",
            Consumed.with(Serdes.String(), stockSerde()));
        
        // Join payment + stock within 10-minute window
        KStream<String, SagaResponse> sagaResponses = payments.join(
            stocks,
            (payment, stock) -> new SagaResponse(payment, stock),
            JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(10)),
            StreamJoined.with(Serdes.String(), paymentSerde(), stockSerde())
        );
        
        // Join with original order
        KStream<String, OrderResult> results = orders
            .join(
                sagaResponses.toTable(),
                (order, saga) -> {
                    // Both responses available - decide order outcome
                    if (saga.getPaymentStatus() == ResponseStatus.ACCEPT &&
                        saga.getStockStatus() == ResponseStatus.ACCEPT) {
                        return new OrderResult(order, OrderStatus.CONFIRMED);
                    } else {
                        return new OrderResult(order, OrderStatus.ROLLBACK);
                    }
                }
            );
        
        // Output results
        results.to("order-results", Produced.with(Serdes.String(), orderResultSerde()));
        
        return results;
    }
    
    private JsonSerde<Order> orderSerde() {
        return new JsonSerde<>(Order.class, objectMapper);
    }
    
    private JsonSerde<PaymentResponse> paymentSerde() {
        return new JsonSerde<>(PaymentResponse.class, objectMapper);
    }
    
    private JsonSerde<StockResponse> stockSerde() {
        return new JsonSerde<>(StockResponse.class, objectMapper);
    }
    
    private JsonSerde<OrderResult> orderResultSerde() {
        return new JsonSerde<>(OrderResult.class, objectMapper);
    }
}
```

**Flow:**
```
Timeline:
10:00:00 → Order(orderId=O1, status=NEW)
10:00:10 → PaymentResponse(orderId=O1, status=ACCEPT)
10:00:15 → StockResponse(orderId=O1, status=ACCEPT)

Processing:
1. payments.join(stocks)
   → SagaResponse(orderId=O1, payment=ACCEPT, stock=ACCEPT)

2. orders.join(sagaResponses)
   → OrderResult(orderId=O1, status=CONFIRMED)

Output:
  order-results topic
  OrderResult(orderId=O1, status=CONFIRMED)
```

### Example 4: Interactive Queries (Query State Store)

**Scenario:** Expose REST API to query order counts from state store.

```java
@RestController
@RequestMapping("/api/orders")
public class OrderStatsController {
    
    @Autowired
    private KafkaStreams kafkaStreams;
    
    @GetMapping("/counts/{customerId}")
    public ResponseEntity<Long> getOrderCount(@PathVariable String customerId) {
        // Query state store
        ReadOnlyKeyValueStore<String, Long> store = kafkaStreams.store(
            StoreQueryParameters.fromNameAndType(
                "order-counts-store",
                QueryableStoreTypes.keyValueStore()
            )
        );
        
        Long count = store.get(customerId);
        
        if (count == null) {
            return ResponseEntity.notFound().build();
        }
        
        return ResponseEntity.ok(count);
    }
    
    @GetMapping("/counts")
    public ResponseEntity<Map<String, Long>> getAllCounts() {
        ReadOnlyKeyValueStore<String, Long> store = kafkaStreams.store(
            StoreQueryParameters.fromNameAndType(
                "order-counts-store",
                QueryableStoreTypes.keyValueStore()
            )
        );
        
        Map<String, Long> counts = new HashMap<>();
        store.all().forEachRemaining(kv -> counts.put(kv.key, kv.value));
        
        return ResponseEntity.ok(counts);
    }
}
```

**Usage:**
```bash
# Query order count for customer C1
curl http://localhost:8081/api/orders/counts/C1
# Response: 15

# Query all counts
curl http://localhost:8081/api/orders/counts
# Response: {"C1": 15, "C2": 8, "C3": 23}
```

---

## Common Patterns

### Pattern 1: Event Aggregation

**Use case:** Calculate daily revenue per product.

```java
KStream<String, Sale> sales = builder.stream("sales");

KTable<Windowed<String>, Double> dailyRevenue = sales
    .groupBy((key, sale) -> sale.getProductId())
    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofDays(1)))
    .aggregate(
        () -> 0.0,  // Initial revenue
        (productId, sale, revenue) -> revenue + sale.getAmount(),
        Materialized.as("daily-revenue-store")
    );
```

### Pattern 2: Stream Enrichment

**Use case:** Enrich orders with product details.

```java
KStream<String, Order> orders = builder.stream("orders");
GlobalKTable<String, Product> products = builder.globalTable("products");

KStream<String, EnrichedOrder> enriched = orders.join(
    products,
    (orderId, order) -> order.getProductId(),
    (order, product) -> new EnrichedOrder(order, product)
);
```

### Pattern 3: Filtering and Routing

**Use case:** Route orders to different topics based on priority.

```java
KStream<String, Order> orders = builder.stream("orders");

Map<String, KStream<String, Order>> branches = orders
    .split()
    .branch((key, order) -> order.getPriority() == Priority.HIGH, 
        Branched.as("high"))
    .branch((key, order) -> order.getPriority() == Priority.MEDIUM, 
        Branched.as("medium"))
    .defaultBranch(Branched.as("low"));

branches.get("high").to("high-priority-orders");
branches.get("medium").to("medium-priority-orders");
branches.get("low").to("low-priority-orders");
```

### Pattern 4: Deduplication

**Use case:** Remove duplicate events within a time window.

```java
KStream<String, Order> orders = builder.stream("orders");

KTable<String, Order> deduplicated = orders
    .groupByKey()
    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
    .reduce((oldOrder, newOrder) -> newOrder)  // Keep latest
    .suppress(Suppressed.untilWindowCloses(unbounded()));  // Emit once per window

deduplicated.toStream().to("deduplicated-orders");
```

### Pattern 5: Complex Event Processing (CEP)

**Use case:** Detect fraud - multiple high-value orders from same customer in 10 minutes.

```java
KStream<String, Order> orders = builder.stream("orders");

KTable<Windowed<String>, Long> orderCounts = orders
    .filter((key, order) -> order.getAmount() > 1000)
    .groupBy((key, order) -> order.getCustomerId())
    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(10)))
    .count();

KStream<Windowed<String>, Long> fraudAlerts = orderCounts
    .toStream()
    .filter((windowedKey, count) -> count > 5);

fraudAlerts.foreach((windowedKey, count) -> 
    log.warn("FRAUD ALERT: Customer {} placed {} high-value orders in 10 minutes",
        windowedKey.key(), count)
);
```

---

## Testing Kafka Streams

### Unit Testing with TopologyTestDriver

```java
package com.example.orderservice.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.*;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class OrderStreamsTopologyTest {
    
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Order> inputTopic;
    private TestOutputTopic<String, Order> outputTopic;
    
    @BeforeEach
    void setup() {
        // Build topology
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Order> orders = builder.stream("orders");
        orders
            .filter((key, order) -> order.getAmount() > 100)
            .to("filtered-orders");
        
        Topology topology = builder.build();
        
        // Configure test driver
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
        
        testDriver = new TopologyTestDriver(topology, props);
        
        // Create test topics
        JsonSerde<Order> orderSerde = new JsonSerde<>(Order.class);
        inputTopic = testDriver.createInputTopic("orders", 
            Serdes.String().serializer(), orderSerde.serializer());
        outputTopic = testDriver.createOutputTopic("filtered-orders", 
            Serdes.String().deserializer(), orderSerde.deserializer());
    }
    
    @AfterEach
    void teardown() {
        testDriver.close();
    }
    
    @Test
    void shouldFilterHighValueOrders() {
        // Given
        Order lowValueOrder = new Order("O1", "C1", 50.0);
        Order highValueOrder = new Order("O2", "C1", 150.0);
        
        // When
        inputTopic.pipeInput("O1", lowValueOrder);
        inputTopic.pipeInput("O2", highValueOrder);
        
        // Then
        assertEquals(1, outputTopic.getQueueSize());
        KeyValue<String, Order> result = outputTopic.readKeyValue();
        assertEquals("O2", result.key);
        assertEquals(150.0, result.value.getAmount());
    }
    
    @Test
    void shouldHandleEmptyStream() {
        // When no input
        // Then
        assertTrue(outputTopic.isEmpty());
    }
}
```

### Integration Testing with EmbeddedKafka

```java
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"orders", "filtered-orders"})
class OrderStreamsIntegrationTest {
    
    @Autowired
    private KafkaTemplate<String, Order> kafkaTemplate;
    
    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;
    
    private Consumer<String, Order> consumer;
    
    @BeforeEach
    void setup() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
            "test-consumer", "true", embeddedKafka);
        consumer = new DefaultKafkaConsumerFactory<String, Order>(consumerProps,
            new StringDeserializer(), new JsonDeserializer<>(Order.class))
            .createConsumer();
        consumer.subscribe(Collections.singletonList("filtered-orders"));
    }
    
    @AfterEach
    void teardown() {
        consumer.close();
    }
    
    @Test
    void shouldProcessOrdersThroughTopology() throws Exception {
        // Given
        Order order = new Order("O1", "C1", 150.0);
        
        // When
        kafkaTemplate.send("orders", "O1", order);
        
        // Then
        ConsumerRecords<String, Order> records = KafkaTestUtils.getRecords(consumer, 5000);
        assertEquals(1, records.count());
        
        ConsumerRecord<String, Order> record = records.iterator().next();
        assertEquals("O1", record.key());
        assertEquals(150.0, record.value().getAmount());
    }
}
```

---

## Production Considerations

### 1. Scaling Kafka Streams

**Horizontal Scaling:**
```
Single Instance:
  Application (3 stream threads)
    Thread 1: Processes Partition 0, 3, 6
    Thread 2: Processes Partition 1, 4, 7
    Thread 3: Processes Partition 2, 5, 8

Two Instances (scale up):
  Instance 1 (3 threads):
    Thread 1: Processes Partition 0, 6
    Thread 2: Processes Partition 1, 7
    Thread 3: Processes Partition 2, 8
  Instance 2 (3 threads):
    Thread 1: Processes Partition 3
    Thread 2: Processes Partition 4
    Thread 3: Processes Partition 5
```

**Configuration:**
```yaml
spring:
  kafka:
    streams:
      properties:
        num.stream.threads: 3  # Threads per instance
```

**Best Practice:**
- `num.stream.threads` ≤ number of partitions
- Scale instances when `num.stream.threads` < partitions

### 2. State Store Management

**RocksDB Tuning:**
```yaml
spring:
  kafka:
    streams:
      properties:
        # RocksDB block cache (off-heap memory)
        rocksdb.config.setter: com.example.CustomRocksDBConfig
        
        # State directory (ensure fast disk: SSD)
        state.dir: /mnt/ssd/kafka-streams
        
        # Commit interval
        commit.interval.ms: 1000
        
        # Cache size (per thread)
        cache.max.bytes.buffering: 10485760  # 10 MB
```

**Custom RocksDB Config:**
```java
public class CustomRocksDBConfig implements RocksDBConfigSetter {
    @Override
    public void setConfig(String storeName, Options options, Map<String, Object> configs) {
        // Optimize for SSD
        options.setCompactionStyle(CompactionStyle.LEVEL);
        options.setWriteBufferSize(16 * 1024 * 1024);  // 16 MB
        options.setMaxWriteBufferNumber(3);
        
        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
        tableConfig.setBlockCache(new LRUCache(50 * 1024 * 1024));  // 50 MB
        options.setTableFormatConfig(tableConfig);
    }
}
```

### 3. Monitoring

**Key Metrics:**
```java
@Component
public class StreamsMetricsReporter {
    
    @Autowired
    private KafkaStreams kafkaStreams;
    
    @Scheduled(fixedRate = 60000)
    public void reportMetrics() {
        Map<MetricName, ? extends Metric> metrics = kafkaStreams.metrics();
        
        // Consumer lag
        metrics.entrySet().stream()
            .filter(e -> e.getKey().name().equals("records-lag-max"))
            .forEach(e -> log.info("Lag: {}", e.getValue().metricValue()));
        
        // Processing rate
        metrics.entrySet().stream()
            .filter(e -> e.getKey().name().equals("process-rate"))
            .forEach(e -> log.info("Rate: {} records/sec", e.getValue().metricValue()));
        
        // State store size
        metrics.entrySet().stream()
            .filter(e -> e.getKey().name().contains("state-store"))
            .forEach(e -> log.info("Store: {}, Size: {}", 
                e.getKey().tags().get("rocksdb-state-id"), 
                e.getValue().metricValue()));
    }
}
```

**Prometheus Metrics:**
```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,metrics,prometheus
  metrics:
    export:
      prometheus:
        enabled: true
```

### 4. Error Handling

**Deserialization Errors:**
```java
@Bean
public KafkaStreamsConfiguration kStreamsConfig() {
    Map<String, Object> props = new HashMap<>();
    // ...
    
    // Handle deserialization errors
    props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
        LogAndContinueExceptionHandler.class);
    
    return new KafkaStreamsConfiguration(props);
}
```

**Processing Errors:**
```java
@Bean
public KStream<String, Order> orderStream(StreamsBuilder builder) {
    return builder.stream("orders")
        .mapValues(order -> {
            try {
                return processOrder(order);
            } catch (Exception e) {
                log.error("Failed to process order: {}", order.getOrderId(), e);
                // Send to DLT (dead-letter topic)
                dlqProducer.send("orders-dlq", order);
                return null;  // Filter out failed record
            }
        })
        .filter((key, order) -> order != null);
}
```

### 5. Exactly-Once Semantics

**Enable EOS:**
```yaml
spring:
  kafka:
    streams:
      properties:
        processing.guarantee: exactly_once_v2
```

**Requirements:**
- ✅ `acks=all` on producers
- ✅ Replication factor ≥ 3
- ✅ Idempotence enabled

**Trade-offs:**
- **Latency:** +20-50% (transactional overhead)
- **Throughput:** -10-20%
- **Guarantee:** No duplicates, no data loss

---

## Key Takeaways

### When to Use Kafka Streams

✅ **Use Kafka Streams when:**
1. Joining multiple streams (payment + stock)
2. Stateful processing (aggregations, windowing)
3. Need exactly-once guarantees
4. Building complex event-driven workflows (SAGA)
5. Real-time analytics (counting, averaging per window)

❌ **Don't use Kafka Streams when:**
1. Simple consume-process-produce (use @KafkaListener)
2. Need low latency (< 10ms) - Kafka Streams adds overhead
3. Heavy external I/O (DB writes, REST calls) - use @KafkaListener
4. Team has no Kafka Streams expertise (learning curve)

### KStream vs KTable

| Feature | KStream | KTable |
|---|---|---|
| **Semantics** | Event stream (append-only) | Changelog stream (updates) |
| **Records** | All records matter | Only latest per key matters |
| **Use Case** | Orders, clicks, logs | User profiles, balances, statuses |
| **Example** | Transaction log | Account balance |

### Joins Summary

| Join Type | Use Case | Key Requirement |
|---|---|---|
| **Stream-Stream** | Join events within time window | Keys match, time window |
| **Stream-Table** | Enrich stream with latest state | Keys match |
| **Table-Table** | Join two changelogs | Keys match |
| **Stream-GlobalTable** | Enrich with reference data | Any field (key extractor) |

### Performance Tips

1. **Prefer `mapValues()` over `map()`** - Avoids repartitioning
2. **Use RocksDB for large state** - Scales beyond memory
3. **Tune `num.stream.threads`** - Match partition count
4. **Enable caching** - Reduces downstream writes
5. **Use tombstones for deletes** - Send null value to delete key

---

## Next Steps

1. **Migrate SAGA orchestration to Kafka Streams**
   - Replace manual state management with KStream joins
   - See [SAGA with Kafka Streams](./10-saga-orchestration-streams.md)

2. **Learn advanced patterns**
   - Windowing for analytics
   - Interactive queries for REST APIs
   - Exactly-once semantics

3. **Explore testing strategies**
   - TopologyTestDriver for unit tests
   - EmbeddedKafka for integration tests

4. **Production deployment**
   - Monitoring and alerting
   - State store management
   - Scaling strategies

---

## Further Reading

- **Kafka Streams Documentation:** https://kafka.apache.org/documentation/streams/
- **Spring Kafka Streams:** https://docs.spring.io/spring-kafka/reference/#kafka-streams
- **Kafka Streams in Action (Book):** William Bejeck
- **Designing Event-Driven Systems (Book):** Ben Stopford (Free: https://www.confluent.io/designing-event-driven-systems/)
- **Kafka Streams Examples:** https://github.com/confluentinc/kafka-streams-examples

---

**Project Context:** This documentation prepares you to migrate from `@KafkaListener`-based SAGA orchestration to Kafka Streams for fault-tolerant, stateful stream processing in the order-service microservice.
