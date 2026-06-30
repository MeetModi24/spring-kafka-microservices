# Spring Boot + Kafka Microservices Project Plan
## Reference Architecture Implementation Roadmap

**Version:** 1.0  
**Last Updated:** July 2026  
**Status:** Phase 2 In Progress  
**Reference:** [piomin/sample-spring-kafka-microservices](https://github.com/piomin/sample-spring-kafka-microservices)

---

## Table of Contents
1. [Executive Summary](#executive-summary)
2. [Reference Architecture Analysis](#reference-architecture-analysis)
3. [Service Architecture Diagram](#service-architecture-diagram)
4. [Technology Stack](#technology-stack)
5. [Phase-by-Phase Implementation Plan](#phase-by-phase-implementation-plan)
6. [Design Patterns & Learning Path](#design-patterns--learning-path)
7. [Frontend Integration Plan](#frontend-integration-plan)
8. [Testing Strategy](#testing-strategy)
9. [Success Criteria](#success-criteria)

---

## Executive Summary

### Project Goals
Build a production-grade event-driven microservices system implementing the **SAGA pattern** for distributed transactions, using Spring Boot and Apache Kafka as the backbone for inter-service communication.

### Current Status
- **Phase 1 Complete:** order-service REST API with DTOs, validation, in-memory storage
- **Phase 2 In Progress:** Error handling + Kafka producer setup
- **Total Duration:** 12-14 weeks (Weeks 1-4 fundamentals, Weeks 5+ reference architecture)

### Key Learning Objectives
1. **LLD (Low-Level Design):** SOLID principles, dependency injection, design patterns (Builder, Factory, Strategy)
2. **HLD (High-Level Design):** Microservices architecture, event-driven design, SAGA orchestration, eventual consistency
3. **Domain Expertise:** Kafka Streams, distributed transactions, compensation logic, idempotency

### Approach
**Hybrid Learning Model:**
- **Weeks 1-4:** Build foundational understanding (REST, Kafka basics, Spring Boot)
- **Week 5+:** Pivot to reference architecture patterns (SAGA, Kafka Streams, state stores)

---

## Reference Architecture Analysis

### Services Overview

The reference repository implements **3 core microservices** coordinating via Kafka to execute distributed transactions:

#### 1. order-service (Orchestrator)
**Role:** SAGA orchestrator - coordinates the distributed transaction  
**Port:** 8080  
**Responsibilities:**
- Accept order creation requests via REST API
- Publish `Order` events with `status=NEW` to Kafka topic `orders`
- Consume responses from `payment-orders` and `stock-orders` topics
- **Join** payment and stock responses using Kafka Streams
- Determine final order status: `CONFIRMED`, `REJECTED`, or `ROLLBACK`
- Publish final decision back to `orders` topic
- Store final orders in Kafka Streams state store (queryable via REST)

**Technology Stack:**
- Spring Boot Web (REST API)
- Spring Kafka (Producer + Consumer)
- **Kafka Streams** (Stream processing, joins, state stores)
- In-memory state store (no database)

**Key APIs:**
```
POST   /orders          - Create order (manual)
POST   /orders/generate - Generate random orders (testing)
GET    /orders          - Query all orders from Kafka state store
```

---

#### 2. payment-service (Participant)
**Role:** Manages customer account balances and payment transactions  
**Port:** Auto-assigned (no REST API, Kafka-only)  
**Responsibilities:**
- Listen to `orders` topic for new orders (`status=NEW`)
- Check customer balance (JPA repository)
- **Reserve funds** if sufficient balance exists
- Publish response: `status=ACCEPT` or `status=REJECT` to `payment-orders` topic
- Listen for final decision on `orders` topic
- **Commit** (confirm reservation) if `status=CONFIRMED`
- **Rollback** (release reservation) if `status=ROLLBACK` and source is NOT payment

**Technology Stack:**
- Spring Boot (no Web starter - Kafka-only)
- Spring Kafka (@KafkaListener)
- Spring Data JPA
- H2 in-memory database (Customer entities)

**Database Schema:**
```sql
Customer {
  id: Long (PK)
  name: String
  amountAvailable: Integer  -- Current available balance
  amountReserved: Integer   -- Funds held for pending orders
}
```

**SAGA Compensation Logic:**
- **Reserve Phase:** `amountAvailable -= price`, `amountReserved += price`
- **Confirm Phase:** `amountReserved -= price` (complete transaction)
- **Rollback Phase:** `amountReserved -= price`, `amountAvailable += price` (restore balance)

---

#### 3. stock-service (Participant)
**Role:** Manages product inventory and stock reservations  
**Port:** Auto-assigned (no REST API, Kafka-only)  
**Responsibilities:**
- Listen to `orders` topic for new orders (`status=NEW`)
- Check product inventory (JPA repository)
- **Reserve items** if sufficient stock exists
- Publish response: `status=ACCEPT` or `status=REJECT` to `stock-orders` topic
- Listen for final decision on `orders` topic
- **Commit** (confirm reservation) if `status=CONFIRMED`
- **Rollback** (release reservation) if `status=ROLLBACK` and source is NOT stock

**Technology Stack:**
- Spring Boot (no Web starter - Kafka-only)
- Spring Kafka (@KafkaListener)
- Spring Data JPA
- H2 in-memory database (Product entities)

**Database Schema:**
```sql
Product {
  id: Long (PK)
  name: String
  availableItems: Integer  -- Current available stock
  reservedItems: Integer   -- Items held for pending orders
}
```

**SAGA Compensation Logic:**
- **Reserve Phase:** `availableItems -= productCount`, `reservedItems += productCount`
- **Confirm Phase:** `reservedItems -= productCount` (complete transaction)
- **Rollback Phase:** `reservedItems -= productCount`, `availableItems += productCount` (restore stock)

---

### Domain Model

#### Shared Domain: `base-domain` module
A shared Maven module containing the `Order` class used by all services.

```java
// Shared across all 3 services
public class Order {
    private Long id;              // Order ID
    private Long customerId;      // FK to Customer
    private Long productId;       // FK to Product
    private int productCount;     // Quantity
    private int price;            // Total price
    private String status;        // NEW, ACCEPT, REJECT, CONFIRMED, ROLLBACK, REJECTED
    private String source;        // "payment" or "stock" (for rollback tracking)
}
```

**Status Flow:**
```
NEW → (sent by order-service)
  ↓
ACCEPT/REJECT → (sent by payment-service and stock-service)
  ↓
CONFIRMED → (both accepted - success!)
REJECTED → (both rejected - order failed)
ROLLBACK → (one accepted, one rejected - compensate the accepted one)
```

---

### Event Flow (SAGA Choreography Pattern)

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          SAGA TRANSACTION FLOW                           │
└─────────────────────────────────────────────────────────────────────────┘

Step 1: Order Creation
─────────────────────────
  Client → POST /orders → order-service
  order-service → Kafka topic "orders" (status=NEW)


Step 2: Parallel Processing (Reserve Phase)
──────────────────────────────────────────────
  payment-service ← Kafka "orders" (status=NEW)
    ├─ Check customer balance
    ├─ Reserve funds (amountAvailable → amountReserved)
    └─ Publish to "payment-orders" (status=ACCEPT or REJECT)

  stock-service ← Kafka "orders" (status=NEW)
    ├─ Check product stock
    ├─ Reserve items (availableItems → reservedItems)
    └─ Publish to "stock-orders" (status=ACCEPT or REJECT)


Step 3: Order Orchestration (Decision Phase)
─────────────────────────────────────────────
  order-service (Kafka Streams)
    ├─ Join "payment-orders" + "stock-orders" by Order ID
    │  (10 second join window)
    ├─ Apply decision logic:
    │  ├─ Both ACCEPT → status=CONFIRMED
    │  ├─ Both REJECT → status=REJECTED
    │  └─ One ACCEPT, one REJECT → status=ROLLBACK (source=rejecting service)
    └─ Publish final decision to "orders"


Step 4: Completion Phase (Commit or Compensate)
────────────────────────────────────────────────
  payment-service ← Kafka "orders" (status=CONFIRMED/ROLLBACK)
    ├─ If CONFIRMED: Commit (amountReserved -= price)
    └─ If ROLLBACK (source != "payment"): Compensate (restore balance)

  stock-service ← Kafka "orders" (status=CONFIRMED/ROLLBACK)
    ├─ If CONFIRMED: Commit (reservedItems -= productCount)
    └─ If ROLLBACK (source != "stock"): Compensate (restore stock)


Step 5: Query Final State
──────────────────────────
  Client → GET /orders → order-service
  order-service → Query Kafka Streams state store → Return order status
```

**Kafka Join Window Explanation:**
- order-service uses `JoinWindows.of(Duration.ofSeconds(10))`
- This means payment and stock responses must arrive within 10 seconds of each other
- If a response is late, the join fails (edge case - requires retry logic in production)

---

## Service Architecture Diagram

```
┌───────────────────────────────────────────────────────────────────────────┐
│                          SYSTEM ARCHITECTURE                               │
└───────────────────────────────────────────────────────────────────────────┘

                                 ┌─────────────┐
                                 │   CLIENT    │
                                 │  (Browser/  │
                                 │   Postman)  │
                                 └──────┬──────┘
                                        │ REST API
                                        │ (HTTP)
                        ┌───────────────▼───────────────┐
                        │     order-service :8080       │
                        │  ┌─────────────────────────┐  │
                        │  │  @RestController        │  │
                        │  │  POST /orders           │  │
                        │  │  GET  /orders           │  │
                        │  └───────────┬─────────────┘  │
                        │              │                 │
                        │  ┌───────────▼─────────────┐  │
                        │  │   KafkaTemplate         │  │
                        │  │   (Producer)            │  │
                        │  └───────────┬─────────────┘  │
                        └──────────────┼─────────────────┘
                                       │
                                       │ Publish Order (status=NEW)
                                       │
        ┌──────────────────────────────▼──────────────────────────────┐
        │                   Apache Kafka Broker                        │
        │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
        │  │ Topic:       │  │ Topic:       │  │ Topic:       │      │
        │  │ orders       │  │ payment-     │  │ stock-       │      │
        │  │ (3 parts)    │  │ orders       │  │ orders       │      │
        │  │              │  │ (3 parts)    │  │ (3 parts)    │      │
        │  └───┬──────▲───┘  └───▲──────────┘  └───▲──────────┘      │
        └──────┼──────┼─────────┼──────────────────┼──────────────────┘
               │      │         │                  │
               │      │         │                  │
     ┌─────────▼──────┼─────────┴──────────────────┘
     │         │      │ Publish Response (ACCEPT/REJECT)
     │         │      │
     │         │      │ Publish Final Decision (CONFIRMED/ROLLBACK/REJECTED)
     │         │      │
     │   ┌─────┴──────▼─────────────┐       ┌────────────────────────┐
     │   │  payment-service         │       │  stock-service         │
     │   │  ┌────────────────────┐  │       │  ┌──────────────────┐  │
     │   │  │  @KafkaListener    │  │       │  │  @KafkaListener  │  │
     │   │  │  topic="orders"    │  │       │  │  topic="orders"  │  │
     │   │  └──────┬─────────────┘  │       │  └──────┬───────────┘  │
     │   │         │                 │       │         │               │
     │   │  ┌──────▼─────────────┐  │       │  ┌──────▼───────────┐  │
     │   │  │ OrderManageService │  │       │  │ OrderManageServ. │  │
     │   │  │  - reserve()       │  │       │  │  - reserve()     │  │
     │   │  │  - confirm()       │  │       │  │  - confirm()     │  │
     │   │  └──────┬─────────────┘  │       │  └──────┬───────────┘  │
     │   │         │                 │       │         │               │
     │   │  ┌──────▼─────────────┐  │       │  ┌──────▼───────────┐  │
     │   │  │ CustomerRepository │  │       │  │ ProductRepository│  │
     │   │  │   (JPA)            │  │       │  │   (JPA)          │  │
     │   │  └──────┬─────────────┘  │       │  └──────┬───────────┘  │
     │   │         │                 │       │         │               │
     │   │  ┌──────▼─────────────┐  │       │  ┌──────▼───────────┐  │
     │   │  │  H2 Database       │  │       │  │  H2 Database     │  │
     │   │  │  (Customers)       │  │       │  │  (Products)      │  │
     │   │  └────────────────────┘  │       │  └──────────────────┘  │
     │   └──────────────────────────┘       └────────────────────────┘
     │
     │
     └──────────────────────────────────────────────────────────────────┐
                                                                          │
        ┌─────────────────────────────────────────────────────────────┐ │
        │  order-service Kafka Streams Processing                     │ │
        │  ┌───────────────────────────────────────────────────────┐  │ │
        │  │  KStream<Long, Order> paymentStream                   │  │ │
        │  │     ← Consume from "payment-orders"                   │  │ │
        │  │                                                        │  │ │
        │  │  KStream<Long, Order> stockStream                     │  │ │
        │  │     ← Consume from "stock-orders"                     │  │ │
        │  │                                                        │  │ │
        │  │  paymentStream.join(stockStream)                      │  │ │
        │  │     .with(OrderManageService::confirm)                │  │ │
        │  │     .to("orders")  ← Publish final decision           │  │ │
        │  │                                                        │  │ │
        │  │  KTable<Long, Order> ordersTable                      │  │ │
        │  │     ← Materialized view from "orders" topic           │  │ │
        │  │     → Stored in "orders" state store (queryable)      │  │ │
        │  └───────────────────────────────────────────────────────┘  │ │
        └─────────────────────────────────────────────────────────────┘ │
                                                                          │
                 ┌────────────────────────────────────────────────────────┘
                 │
                 │ GET /orders → Query state store
                 │
           ┌─────▼─────────────────────┐
           │  ReadOnlyKeyValueStore    │
           │  (Kafka Streams)          │
           │  - Key: Order ID          │
           │  - Value: Order object    │
           └───────────────────────────┘
```

---

## Technology Stack

### Reference Repository Stack
| Component | Technology | Version |
|-----------|------------|---------|
| **Java** | OpenJDK | 21 |
| **Build Tool** | Maven | 3.x |
| **Framework** | Spring Boot | 4.1.0 (Spring 7.x) |
| **Messaging** | Apache Kafka | 3.x (via spring-kafka) |
| **Stream Processing** | Kafka Streams | 3.x |
| **Database** | H2 (in-memory) | Latest |
| **ORM** | Spring Data JPA | (Spring Boot managed) |
| **JSON** | Jackson | (Spring Boot managed) |
| **Logging** | SLF4J + Logback | (Spring Boot managed) |
| **Testing** | JUnit 5, Spring Kafka Test, Testcontainers | (Spring Boot managed) |
| **Containerization** | Docker Compose | 3.x |
| **Docs** | OpenAPI/Swagger | springdoc-openapi 3.0.3 |

### Current Project Stack (order-service)
| Component | Technology | Version | Notes |
|-----------|------------|---------|-------|
| **Java** | OpenJDK | 17 | Upgrade to 21 in Phase 7 |
| **Build Tool** | Maven | 3.x | |
| **Framework** | Spring Boot | 3.5.16 | Upgrade to 4.x in Phase 7 |
| **Messaging** | Apache Kafka | 3.3.2 (spring-kafka) | |
| **Validation** | Jakarta Validation | (Spring Boot managed) | |
| **Logging** | Lombok @Slf4j | (via Lombok) | |
| **Testing** | JUnit 5, Spring Kafka Test | (Spring Boot managed) | |

### Technology Gaps (To Implement)
- Kafka Streams (Phase 5)
- Spring Data JPA (Phase 6)
- H2 Database (Phase 6)
- Testcontainers (Phase 8)
- Docker Compose orchestration (Phase 8)

### Why These Differences?

#### Java 17 vs 21
- **Current:** Java 17 (LTS, stable for learning)
- **Reference:** Java 21 (latest LTS, virtual threads, pattern matching)
- **Migration:** Phase 7 (after core functionality works)

#### Spring Boot 3.5 vs 4.1
- **Current:** Spring Boot 3.5.16 (stable, widely documented)
- **Reference:** Spring Boot 4.1.0 (bleeding edge, requires Java 21)
- **Migration:** Phase 7 (after SAGA pattern works)

#### In-Memory Storage vs JPA
- **Current:** In-memory `Map<String, Order>` (Phase 1-2)
- **Reference:** H2 + Spring Data JPA (Phase 6)
- **Reason:** Learn REST/Kafka fundamentals before database complexity

---

## Phase-by-Phase Implementation Plan

### Phase 1: Foundation (Week 1-2) ✅ COMPLETED

**Duration:** 2 weeks  
**Status:** COMPLETED  
**Goal:** Build production-ready REST API with validation, DTOs, error handling

#### Deliverables
- [x] order-service scaffolding (Spring Boot 3.5.16)
- [x] Domain models: `Order`, `OrderItem`, `OrderStatus` enum
- [x] DTOs: `CreateOrderRequest`, `OrderResponse`, `OrderItemResponse`, `ErrorResponse`
- [x] REST Controller with endpoints:
  - `POST /api/orders` - Create order
  - `GET /api/orders/{id}` - Get order by ID
  - `GET /api/orders` - List all orders
- [x] In-memory storage (`Map<String, Order>`)
- [x] Input validation using Jakarta Validation (`@Valid`, `@NotNull`, `@Min`)
- [x] Exception handling: `GlobalExceptionHandler`, custom exceptions

#### Technologies Introduced
- Spring Boot Starter Web
- Jakarta Validation
- Lombok
- Spring Boot Actuator

#### Learning Objectives (LLD)
- **Dependency Injection:** Constructor injection in controllers/services
- **DTO Pattern:** Separation of API contracts from domain models
- **Single Responsibility:** Each class has one clear purpose
- **Exception Handling:** Centralized error responses

#### Testing Checklist
- [x] POST order with valid data → 201 Created
- [x] POST order with invalid data → 400 Bad Request
- [x] GET order by valid ID → 200 OK
- [x] GET order by invalid ID → 404 Not Found
- [x] GET all orders → 200 OK with list

---

### Phase 2: Kafka Producer (Week 3) 🔄 IN PROGRESS

**Duration:** 1 week  
**Status:** IN PROGRESS  
**Goal:** Publish order events to Kafka when orders are created

#### Deliverables
- [ ] Configure Kafka producer in `application.yml`
  - Bootstrap servers: `localhost:9092`
  - Key serializer: `LongSerializer` (Order ID)
  - Value serializer: `JsonSerializer` (Order object)
- [ ] Create `KafkaProducerConfig` bean (optional, can use application.yml)
- [ ] Inject `KafkaTemplate<Long, Order>` into `OrderService`
- [ ] Publish event after order creation:
  ```java
  kafkaTemplate.send("orders", order.getOrderId(), order);
  ```
- [ ] Add logging: `LOG.info("Sent: {}", order);`
- [ ] Set up local Kafka broker (Docker Compose or manual install)
- [ ] Test order creation → verify event in Kafka topic

#### Technologies Introduced
- Spring Kafka (Producer)
- KafkaTemplate
- Apache Kafka (single broker, Zookeeper)

#### Learning Objectives (LLD)
- **Template Pattern:** KafkaTemplate abstracts Kafka producer complexity
- **Serialization:** Java objects → JSON → Kafka messages
- **Asynchronous Communication:** Fire-and-forget vs. sync sends

#### Learning Objectives (HLD)
- **Event-Driven Architecture:** Services communicate via events, not direct calls
- **Publish-Subscribe Pattern:** One publisher, many subscribers

#### Testing Checklist
- [ ] Start Zookeeper and Kafka broker
- [ ] POST order → Event published to `orders` topic
- [ ] Verify event using Kafka console consumer:
  ```bash
  kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic orders --from-beginning --property print.key=true
  ```
- [ ] Verify event contains correct Order JSON
- [ ] Test with Kafka down → Handle connection errors gracefully

#### Error Handling Enhancements
- [ ] Add `KafkaProducerException` handler
- [ ] Retry logic for transient Kafka failures (Phase 8)
- [ ] Dead Letter Queue (DLQ) for failed messages (Phase 8)

---

### Phase 3: Payment Service (Week 4-5)

**Duration:** 2 weeks  
**Goal:** Build first Kafka consumer service with business logic

#### Week 4: Service Scaffolding & Database Setup

**Deliverables:**
- [ ] Create `payment-service` Maven module
  - Spring Boot Starter (no Web dependency)
  - Spring Kafka
  - Spring Data JPA
  - H2 Database
  - Lombok
- [ ] Configure `application.yml`:
  - Kafka consumer settings
  - JPA/Hibernate settings (ddl-auto: create-drop)
  - H2 console enabled
- [ ] Create `Customer` entity:
  ```java
  @Entity
  public class Customer {
      @Id @GeneratedValue
      private Long id;
      private String name;
      private Integer amountAvailable;  // Current balance
      private Integer amountReserved;   // Funds on hold
  }
  ```
- [ ] Create `CustomerRepository extends JpaRepository<Customer, Long>`
- [ ] Add `@PostConstruct` data initialization: 100 random customers (use Datafaker)
- [ ] Test: Start service → Verify customers in H2 console

#### Week 5: Kafka Consumer & Business Logic

**Deliverables:**
- [ ] Create `OrderManageService`:
  - `reserve(Order order)` method:
    - Fetch customer by `order.getCustomerId()`
    - Check balance: `order.getPrice() < customer.getAmountAvailable()`
    - If sufficient: Reserve funds, set `order.status = "ACCEPT"`
    - If insufficient: Set `order.status = "REJECT"`
    - Save customer
    - Publish response to `payment-orders` topic
- [ ] Add `@KafkaListener` in main application class:
  ```java
  @KafkaListener(id = "orders", topics = "orders", groupId = "payment")
  public void onEvent(Order order) {
      if (order.getStatus().equals("NEW"))
          orderManageService.reserve(order);
  }
  ```
- [ ] Configure Kafka producer in payment-service (for response events)
- [ ] Create shared `base-domain` module for `Order` class (used by all services)

#### Technologies Introduced
- Spring Kafka (@KafkaListener)
- Spring Data JPA
- H2 in-memory database
- Multi-module Maven project (base-domain)

#### Learning Objectives (LLD)
- **Repository Pattern:** Abstraction over data access
- **Entity-Repository-Service Layering:** Clear separation of concerns
- **JPA Entity Lifecycle:** Transient → Persistent → Detached states
- **Consumer Groups:** Kafka partitioning and parallel processing

#### Learning Objectives (HLD)
- **Microservices Communication:** Async event-driven messaging
- **Data Ownership:** payment-service owns Customer data
- **Eventual Consistency:** No synchronous database calls between services

#### Testing Checklist
- [ ] Start payment-service → Verify 100 customers created in H2
- [ ] POST order in order-service → Verify payment-service receives event
- [ ] Verify payment-service publishes `ACCEPT` or `REJECT` to `payment-orders`
- [ ] Test edge cases:
  - Customer not found (should throw exception)
  - Insufficient balance (should reject)
  - Sufficient balance (should accept and reserve)
- [ ] Verify database state: `amountAvailable` and `amountReserved` updated correctly

---

### Phase 4: Stock Service (Week 5-6)

**Duration:** 1 week  
**Goal:** Build second consumer service (parallel to payment-service)

#### Deliverables
- [ ] Create `stock-service` Maven module (similar to payment-service)
- [ ] Create `Product` entity:
  ```java
  @Entity
  public class Product {
      @Id @GeneratedValue
      private Long id;
      private String name;
      private Integer availableItems;  // Current stock
      private Integer reservedItems;   // Items on hold
  }
  ```
- [ ] Create `ProductRepository`
- [ ] Initialize 1000 random products with stock (10-1000 items)
- [ ] Create `OrderManageService`:
  - `reserve(Order order)` method:
    - Fetch product by `order.getProductId()`
    - Check stock: `order.getProductCount() < product.getAvailableItems()`
    - If sufficient: Reserve items, set `order.status = "ACCEPT"`
    - If insufficient: Set `order.status = "REJECT"`
    - Publish response to `stock-orders` topic
- [ ] Add `@KafkaListener(topics = "orders", groupId = "stock")`

#### Technologies Introduced
- Same as Phase 3 (JPA, H2, Kafka consumer)

#### Learning Objectives (LLD)
- **Code Reusability:** Similar structure to payment-service (template pattern)
- **Separation of Concerns:** Stock service only cares about inventory

#### Learning Objectives (HLD)
- **Parallel Processing:** payment-service and stock-service process orders independently
- **Loose Coupling:** Services don't know about each other (only Kafka topics)

#### Testing Checklist
- [ ] Start stock-service → Verify 1000 products in H2
- [ ] POST order → Verify stock-service receives event
- [ ] Verify stock-service publishes `ACCEPT` or `REJECT` to `stock-orders`
- [ ] Test edge cases:
  - Product not found
  - Insufficient stock
  - Sufficient stock
- [ ] Run all 3 services together:
  - order-service (port 8080)
  - payment-service (no port)
  - stock-service (no port)
- [ ] POST order → Verify both services publish responses

---

### Phase 5: SAGA Orchestration (Week 6-7)

**Duration:** 2 weeks  
**Goal:** Implement distributed transaction coordination using Kafka Streams

#### Week 6: Kafka Streams Setup

**Deliverables:**
- [ ] Add Kafka Streams dependency to order-service:
  ```xml
  <dependency>
      <groupId>org.apache.kafka</groupId>
      <artifactId>kafka-streams</artifactId>
  </dependency>
  ```
- [ ] Enable Kafka Streams in `OrderApp`:
  ```java
  @EnableKafkaStreams
  ```
- [ ] Configure Kafka Streams in `application.yml`:
  ```yaml
  spring.kafka.streams:
    properties:
      default.key.serde: org.apache.kafka.common.serialization.Serdes$LongSerde
      default.value.serde: org.springframework.kafka.support.serializer.JsonSerde
      spring.json.trusted.packages: "*"
    state-dir: /tmp/kafka-streams/
  ```
- [ ] Create Kafka topics programmatically:
  ```java
  @Bean
  public NewTopic orders() {
      return TopicBuilder.name("orders").partitions(3).compact().build();
  }
  @Bean
  public NewTopic paymentTopic() { ... }
  @Bean
  public NewTopic stockTopic() { ... }
  ```
- [ ] Create basic Kafka Streams topology (log events only):
  ```java
  @Bean
  public KStream<Long, Order> stream(StreamsBuilder builder) {
      KStream<Long, Order> stream = builder.stream("payment-orders");
      stream.peek((k, v) -> LOG.info("Payment: {}", v));
      return stream;
  }
  ```

#### Week 7: Stream Join & Decision Logic

**Deliverables:**
- [ ] Implement stream join in order-service:
  ```java
  @Bean
  public KStream<Long, Order> stream(StreamsBuilder builder) {
      JsonSerde<Order> orderSerde = new JsonSerde<>(Order.class);
      
      KStream<Long, Order> paymentStream = builder
          .stream("payment-orders", Consumed.with(Serdes.Long(), orderSerde));
      
      KStream<Long, Order> stockStream = builder
          .stream("stock-orders", Consumed.with(Serdes.Long(), orderSerde));
      
      paymentStream.join(
              stockStream,
              orderManageService::confirm,  // Join logic
              JoinWindows.of(Duration.ofSeconds(10)),  // Join window
              StreamJoined.with(Serdes.Long(), orderSerde, orderSerde))
          .peek((k, o) -> LOG.info("Output: {}", o))
          .to("orders");  // Publish final decision
      
      return paymentStream;
  }
  ```
- [ ] Implement `OrderManageService.confirm()` decision logic:
  ```java
  public Order confirm(Order orderPayment, Order orderStock) {
      Order finalOrder = new Order(orderPayment.getId(), ...);
      
      if (orderPayment.getStatus().equals("ACCEPT") &&
          orderStock.getStatus().equals("ACCEPT")) {
          finalOrder.setStatus("CONFIRMED");
      } else if (orderPayment.getStatus().equals("REJECT") &&
                 orderStock.getStatus().equals("REJECT")) {
          finalOrder.setStatus("REJECTED");
      } else {
          // One accepted, one rejected → Rollback
          String source = orderPayment.getStatus().equals("REJECT")
              ? "PAYMENT" : "STOCK";
          finalOrder.setStatus("ROLLBACK");
          finalOrder.setSource(source);  // Track which service needs compensation
      }
      return finalOrder;
  }
  ```
- [ ] Create state store for queryable orders:
  ```java
  @Bean
  public KTable<Long, Order> table(StreamsBuilder builder) {
      KeyValueBytesStoreSupplier store = Stores.persistentKeyValueStore("orders");
      KStream<Long, Order> stream = builder.stream("orders");
      return stream.toTable(Materialized.<Long, Order>as(store)
          .withKeySerde(Serdes.Long())
          .withValueSerde(orderSerde));
  }
  ```
- [ ] Update `GET /orders` to query state store:
  ```java
  ReadOnlyKeyValueStore<Long, Order> store = kafkaStreamsFactory
      .getKafkaStreams()
      .store(StoreQueryParameters.fromNameAndType("orders", 
             QueryableStoreTypes.keyValueStore()));
  KeyValueIterator<Long, Order> it = store.all();
  it.forEachRemaining(kv -> orders.add(kv.value));
  ```

#### Technologies Introduced
- **Kafka Streams API**
- **KStream** (event stream)
- **KTable** (changelog stream → materialized view)
- **Stream Joins** (KStream.join())
- **State Stores** (persistent key-value store)

#### Learning Objectives (LLD)
- **Functional Programming:** Streams, map, filter, peek operations
- **Stateful Stream Processing:** Maintaining state across events
- **Join Operations:** Inner join, time windows

#### Learning Objectives (HLD)
- **SAGA Pattern (Choreography):** Distributed transaction without central coordinator
- **Eventual Consistency:** Final order status converges after all events processed
- **Event Sourcing:** Order state derived from event stream
- **CQRS (Command Query Responsibility Segregation):** Write to Kafka, read from state store

#### Testing Checklist
- [ ] POST order → Verify payment and stock events published
- [ ] Verify order-service joins both events within 10 seconds
- [ ] Verify final order status:
  - Both ACCEPT → `CONFIRMED`
  - Both REJECT → `REJECTED`
  - One ACCEPT, one REJECT → `ROLLBACK`
- [ ] Verify final order published back to `orders` topic
- [ ] GET /orders → Verify order appears in state store with final status
- [ ] Test join window timeout (manually delay one service)

#### Known Edge Cases
- **Join Window Timeout:** If payment/stock responses arrive >10s apart, join fails
  - Solution: Increase window or implement retry logic
- **Out-of-Order Events:** Kafka guarantees order per partition only
  - Solution: Ensure all events for same order ID go to same partition (key by order ID)

---

### Phase 6: SAGA Compensation (Week 7-8)

**Duration:** 1 week  
**Goal:** Implement rollback logic in payment-service and stock-service

#### Deliverables
- [ ] Update payment-service `@KafkaListener` to handle final status:
  ```java
  @KafkaListener(id = "orders", topics = "orders", groupId = "payment")
  public void onEvent(Order order) {
      if (order.getStatus().equals("NEW"))
          orderManageService.reserve(order);
      else
          orderManageService.confirm(order);  // Handle CONFIRMED/ROLLBACK
  }
  ```
- [ ] Implement `OrderManageService.confirm()` in payment-service:
  ```java
  public void confirm(Order order) {
      Customer customer = repository.findById(order.getCustomerId()).orElseThrow();
      
      if (order.getStatus().equals("CONFIRMED")) {
          // Commit: Release reserved funds (transaction complete)
          customer.setAmountReserved(customer.getAmountReserved() - order.getPrice());
          repository.save(customer);
      } else if (order.getStatus().equals("ROLLBACK") &&
                 !order.getSource().equals("payment")) {
          // Rollback: Refund reserved funds (compensation)
          customer.setAmountReserved(customer.getAmountReserved() - order.getPrice());
          customer.setAmountAvailable(customer.getAmountAvailable() + order.getPrice());
          repository.save(customer);
      }
      // If source == "payment", do nothing (we rejected, nothing to rollback)
  }
  ```
- [ ] Implement same compensation logic in stock-service:
  ```java
  public void confirm(Order order) {
      Product product = repository.findById(order.getProductId()).orElseThrow();
      
      if (order.getStatus().equals("CONFIRMED")) {
          // Commit: Release reserved items
          product.setReservedItems(product.getReservedItems() - order.getProductCount());
      } else if (order.getStatus().equals("ROLLBACK") &&
                 !order.getSource().equals("stock")) {
          // Rollback: Restore reserved items
          product.setReservedItems(product.getReservedItems() - order.getProductCount());
          product.setAvailableItems(product.getAvailableItems() + order.getProductCount());
      }
      repository.save(product);
  }
  ```

#### Technologies Introduced
- SAGA compensation pattern
- Idempotency (Phase 8 enhancement)

#### Learning Objectives (LLD)
- **State Machine:** Order status transitions (NEW → ACCEPT → CONFIRMED/ROLLBACK)
- **Compensation Logic:** Reversing local transactions

#### Learning Objectives (HLD)
- **SAGA Compensation:** Undoing completed work when distributed transaction fails
- **Eventual Consistency:** System reaches consistent state after rollback
- **Atomicity Without ACID:** No two-phase commit, no distributed locks

#### Testing Checklist
- [ ] **Scenario 1: Happy Path (Both Accept)**
  - POST order with valid customer/product
  - Verify payment reserves funds
  - Verify stock reserves items
  - Verify order-service publishes `CONFIRMED`
  - Verify payment commits (amountReserved -= price)
  - Verify stock commits (reservedItems -= count)
  - Verify final order status: `CONFIRMED`
- [ ] **Scenario 2: Both Reject**
  - POST order with insufficient balance AND insufficient stock
  - Verify both services reject
  - Verify order-service publishes `REJECTED`
  - Verify no database changes (nothing reserved)
- [ ] **Scenario 3: Payment Accepts, Stock Rejects (Rollback Payment)**
  - POST order with valid balance but insufficient stock
  - Verify payment reserves funds
  - Verify stock rejects
  - Verify order-service publishes `ROLLBACK` with `source=STOCK`
  - Verify payment compensates: `amountAvailable` restored
  - Verify stock does nothing (it rejected, nothing to rollback)
- [ ] **Scenario 4: Payment Rejects, Stock Accepts (Rollback Stock)**
  - POST order with insufficient balance but valid stock
  - Verify payment rejects
  - Verify stock reserves items
  - Verify order-service publishes `ROLLBACK` with `source=PAYMENT`
  - Verify stock compensates: `availableItems` restored
  - Verify payment does nothing

#### Database Verification Queries
```sql
-- Check customer balance after each scenario
SELECT * FROM customer WHERE id = ?;

-- Check product stock after each scenario
SELECT * FROM product WHERE id = ?;

-- Verify no orphaned reservations
SELECT SUM(amount_reserved) FROM customer;  -- Should decrease after commits
SELECT SUM(reserved_items) FROM product;    -- Should decrease after commits
```

---

### Phase 7: Database Integration & Persistence (Week 8-9)

**Duration:** 2 weeks  
**Goal:** Replace in-memory storage with real database, add JPA to order-service

#### Week 8: Order Service Database Setup

**Deliverables:**
- [ ] Add dependencies to order-service:
  ```xml
  <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-data-jpa</artifactId>
  </dependency>
  <dependency>
      <groupId>com.h2database</groupId>
      <artifactId>h2</artifactId>
      <scope>runtime</scope>
  </dependency>
  ```
- [ ] Convert `Order` and `OrderItem` to JPA entities:
  ```java
  @Entity
  @Table(name = "orders")
  public class Order {
      @Id
      @GeneratedValue(strategy = GenerationType.UUID)
      private String orderId;
      
      private Long customerId;
      private Long productId;
      private BigDecimal totalAmount;
      
      @Enumerated(EnumType.STRING)
      private OrderStatus status;
      
      @OneToMany(cascade = CascadeType.ALL, orphanRemoval = true)
      @JoinColumn(name = "order_id")
      private List<OrderItem> items;
      
      private LocalDateTime createdAt;
  }
  ```
- [ ] Create `OrderRepository extends JpaRepository<Order, String>`
- [ ] Update `OrderService` to use repository instead of `Map<String, Order>`
- [ ] Configure H2 console in `application.yml`:
  ```yaml
  spring:
    datasource:
      url: jdbc:h2:mem:orderdb
      driver-class-name: org.h2.Driver
    h2:
      console.enabled: true
    jpa:
      hibernate:
        ddl-auto: create-drop
      show-sql: true
  ```

#### Week 9: Advanced JPA Features

**Deliverables:**
- [ ] Add JPA auditing:
  ```java
  @EntityListeners(AuditingEntityListener.class)
  public class Order {
      @CreatedDate
      private LocalDateTime createdAt;
      
      @LastModifiedDate
      private LocalDateTime updatedAt;
  }
  
  @EnableJpaAuditing  // In main class
  ```
- [ ] Add custom query methods:
  ```java
  public interface OrderRepository extends JpaRepository<Order, String> {
      List<Order> findByCustomerId(Long customerId);
      List<Order> findByStatus(OrderStatus status);
      List<Order> findByCreatedAtBetween(LocalDateTime start, LocalDateTime end);
  }
  ```
- [ ] Implement pagination:
  ```java
  @GetMapping
  public Page<OrderResponse> getAllOrders(
      @RequestParam(defaultValue = "0") int page,
      @RequestParam(defaultValue = "10") int size) {
      Pageable pageable = PageRequest.of(page, size, Sort.by("createdAt").descending());
      return orderService.getAllOrders(pageable);
  }
  ```
- [ ] Add database migrations (Flyway or Liquibase) - Optional

#### Technologies Introduced
- Spring Data JPA (CRUD operations)
- JPA Entity Lifecycle (@Entity, @Id, @GeneratedValue)
- JPA Relationships (@OneToMany, @ManyToOne)
- JPA Auditing (@CreatedDate, @LastModifiedDate)
- H2 embedded database
- JPQL (Java Persistence Query Language)

#### Learning Objectives (LLD)
- **Entity-Relationship Modeling:** Order ↔ OrderItem relationship
- **ORM Mapping:** Java objects ↔ Database tables
- **Lazy vs Eager Loading:** Fetch strategies
- **N+1 Query Problem:** Fetch joins to optimize queries
- **Transaction Management:** @Transactional boundaries

#### Learning Objectives (HLD)
- **Data Persistence:** Stateful services with durable storage
- **Database per Service Pattern:** Each microservice owns its database

#### Testing Checklist
- [ ] POST order → Verify saved in database (check H2 console)
- [ ] GET /orders → Verify pagination works
- [ ] GET /orders?page=0&size=5 → Returns first 5 orders
- [ ] Restart service → Verify orders persisted (if using file-based H2)
- [ ] Test custom queries: findByCustomerId, findByStatus

---

### Phase 8: API Gateway & Frontend APIs (Week 9-10)

**Duration:** 1 week  
**Goal:** Prepare backend APIs for frontend consumption

#### Deliverables
- [ ] Add CORS configuration:
  ```java
  @Configuration
  public class WebConfig implements WebMvcConfigurer {
      @Override
      public void addCorsMappings(CorsRegistry registry) {
          registry.addMapping("/api/**")
              .allowedOrigins("http://localhost:3000")  // React dev server
              .allowedMethods("GET", "POST", "PUT", "DELETE")
              .allowedHeaders("*");
      }
  }
  ```
- [ ] Add OpenAPI documentation:
  ```xml
  <dependency>
      <groupId>org.springdoc</groupId>
      <artifactId>springdoc-openapi-starter-webmvc-ui</artifactId>
      <version>2.3.0</version>
  </dependency>
  ```
  - Access: http://localhost:8080/swagger-ui.html
- [ ] Enhance REST APIs for frontend:
  - [ ] Add `POST /api/orders/generate` endpoint (for demo)
  - [ ] Add `GET /api/orders/status/{id}` - Poll order status
  - [ ] Add `GET /api/customers` - List customers (for dropdown)
  - [ ] Add `GET /api/products` - List products (for dropdown)
- [ ] Optional: Spring Cloud Gateway (if building multiple services)
  - Single entry point: http://localhost:8080
  - Route `/api/orders/**` → order-service:8081
  - Route `/api/payments/**` → payment-service:8082
  - Route `/api/stock/**` → stock-service:8083

#### Technologies Introduced
- Spring CORS
- OpenAPI/Swagger
- Spring Cloud Gateway (optional)

#### Learning Objectives (HLD)
- **API Gateway Pattern:** Single entry point for frontend
- **BFF (Backend for Frontend):** Tailoring APIs for frontend needs
- **API Versioning:** `/api/v1/orders` vs `/api/v2/orders`

#### Frontend API Requirements

The frontend will need these APIs:

##### 1. Create Order
```http
POST /api/orders
Content-Type: application/json

{
  "customerId": 1,
  "items": [
    {
      "productId": 101,
      "productName": "Laptop",
      "quantity": 2,
      "price": 1200.00
    }
  ]
}

Response: 201 Created
{
  "orderId": "abc123",
  "customerId": 1,
  "totalAmount": 2400.00,
  "status": "NEW",
  "items": [...],
  "createdAt": "2026-07-01T10:00:00Z"
}
```

##### 2. Get Order Status (Polling)
```http
GET /api/orders/status/abc123

Response: 200 OK
{
  "orderId": "abc123",
  "status": "CONFIRMED",  // or NEW, PENDING, REJECTED, ROLLBACK
  "updatedAt": "2026-07-01T10:00:05Z"
}
```

##### 3. List All Orders (Dashboard)
```http
GET /api/orders?page=0&size=10

Response: 200 OK
{
  "content": [ {...}, {...} ],
  "totalElements": 50,
  "totalPages": 5,
  "size": 10,
  "number": 0
}
```

##### 4. List Customers (Dropdown)
```http
GET /api/customers

Response: 200 OK
[
  { "id": 1, "name": "John Doe", "balance": 5000 },
  { "id": 2, "name": "Jane Smith", "balance": 8000 }
]
```

##### 5. List Products (Catalog)
```http
GET /api/products?search=laptop

Response: 200 OK
[
  { "id": 101, "name": "Laptop", "price": 1200.00, "stock": 50 },
  { "id": 102, "name": "Laptop Bag", "price": 50.00, "stock": 100 }
]
```

##### 6. WebSocket for Real-Time Updates (Optional - Phase 9)
```javascript
// Frontend connects to WebSocket
const ws = new WebSocket('ws://localhost:8080/ws/orders');

ws.onmessage = (event) => {
  const order = JSON.parse(event.data);
  console.log('Order updated:', order);
  // Update UI in real-time
};
```

---

### Phase 9: Advanced Features (Week 10-12)

**Duration:** 2-3 weeks  
**Goal:** Production-ready enhancements

#### Week 10: Observability & Monitoring

**Deliverables:**
- [ ] Add Spring Boot Actuator endpoints:
  - `/actuator/health` - Service health
  - `/actuator/metrics` - JVM, Kafka, DB metrics
  - `/actuator/prometheus` - Metrics for Prometheus
- [ ] Add distributed tracing (Spring Cloud Sleuth + Zipkin)
- [ ] Add structured logging (Logstash JSON format)
- [ ] Add Kafka metrics monitoring

#### Week 11: Resilience & Reliability

**Deliverables:**
- [ ] Add Kafka producer retry logic:
  ```yaml
  spring.kafka.producer:
    retries: 3
    acks: all  # Wait for all replicas
  ```
- [ ] Implement Dead Letter Queue (DLQ):
  - Failed messages → `orders-dlq` topic
  - Manual reprocessing endpoint
- [ ] Add idempotency handling:
  - Deduplicate events by order ID
  - Use Kafka Streams state store to track processed IDs
- [ ] Add circuit breaker (Resilience4j) for external calls
- [ ] Add rate limiting (bucket4j)

#### Week 12: Testing & Quality

**Deliverables:**
- [ ] Integration tests with Testcontainers:
  ```java
  @Testcontainers
  @SpringBootTest
  public class OrderIntegrationTest {
      @Container
      static KafkaContainer kafka = new KafkaContainer(
          DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));
      
      @DynamicPropertySource
      static void kafkaProperties(DynamicPropertyRegistry registry) {
          registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
      }
      
      @Test
      void testOrderCreation() { ... }
  }
  ```
- [ ] Contract testing (Spring Cloud Contract)
- [ ] Load testing (JMeter or Gatling)
- [ ] Chaos engineering (Chaos Monkey for Spring Boot)

#### Technologies Introduced
- Spring Boot Actuator
- Prometheus + Grafana
- Zipkin (distributed tracing)
- Testcontainers
- Resilience4j
- Chaos Monkey

#### Learning Objectives (HLD)
- **Observability:** Metrics, logs, traces
- **Fault Tolerance:** Retries, circuit breakers, fallbacks
- **Testing in Production:** Chaos engineering

---

### Phase 10: Containerization & Deployment (Week 12-14)

**Duration:** 2 weeks  
**Goal:** Deploy to production environment

#### Week 12: Docker & Docker Compose

**Deliverables:**
- [ ] Create Dockerfile for each service:
  ```dockerfile
  FROM eclipse-temurin:21-jdk-alpine
  WORKDIR /app
  COPY target/*.jar app.jar
  EXPOSE 8080
  ENTRYPOINT ["java", "-jar", "app.jar"]
  ```
- [ ] Build Docker images using Jib Maven plugin:
  ```bash
  mvn clean package -Pbuild-image
  ```
- [ ] Create `docker-compose.yml`:
  ```yaml
  version: '3.8'
  services:
    zookeeper:
      image: confluentinc/cp-zookeeper:7.5.0
      environment:
        ZOOKEEPER_CLIENT_PORT: 2181
    
    kafka:
      image: confluentinc/cp-kafka:7.5.0
      depends_on: [zookeeper]
      ports: ["9092:9092"]
      environment:
        KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
        KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
    
    order-service:
      image: order-service:1.0.0-SNAPSHOT
      depends_on: [kafka]
      ports: ["8080:8080"]
      environment:
        SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:9092
    
    payment-service:
      image: payment-service:1.0.0-SNAPSHOT
      depends_on: [kafka]
      environment:
        SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:9092
    
    stock-service:
      image: stock-service:1.0.0-SNAPSHOT
      depends_on: [kafka]
      environment:
        SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:9092
  ```
- [ ] Test: `docker-compose up` → All services running

#### Week 13: Kubernetes Deployment (Optional)

**Deliverables:**
- [ ] Create Kubernetes manifests:
  - Deployments (order-service, payment-service, stock-service)
  - Services (ClusterIP, LoadBalancer)
  - ConfigMaps (application.yml)
  - Secrets (Kafka credentials)
- [ ] Deploy to Minikube (local) or cloud (AWS EKS, GCP GKE)
- [ ] Add Horizontal Pod Autoscaler (HPA)
- [ ] Add health checks (liveness, readiness probes)

#### Technologies Introduced
- Docker
- Docker Compose
- Kubernetes (optional)
- Jib Maven plugin

---

## Design Patterns & Learning Path

### LLD (Low-Level Design) Patterns

#### Phase 1-2: Foundational Patterns
| Pattern | Where Used | Why |
|---------|-----------|-----|
| **Dependency Injection** | All services | Loose coupling, testability |
| **DTO (Data Transfer Object)** | REST API layer | Decouple API contracts from domain models |
| **Builder Pattern** | DTOs, domain models | Fluent object creation |
| **Repository Pattern** | Data access layer | Abstract persistence logic |
| **Service Layer** | Business logic | Separate concerns from controllers |
| **Exception Handling** | GlobalExceptionHandler | Centralized error responses |

#### Phase 3-6: Messaging Patterns
| Pattern | Where Used | Why |
|---------|-----------|-----|
| **Template Pattern** | KafkaTemplate | Abstract producer complexity |
| **Strategy Pattern** | Order status transitions | Different behavior per status |
| **State Machine** | Order lifecycle | Manage complex state transitions |
| **Factory Pattern** | Order creation | Encapsulate instantiation logic |

#### Phase 7-9: Persistence Patterns
| Pattern | Where Used | Why |
|---------|-----------|-----|
| **Entity-Repository-Service** | JPA layer | Clear separation of concerns |
| **Lazy Loading** | JPA relationships | Performance optimization |
| **Optimistic Locking** | JPA entities | Handle concurrent updates |

### HLD (High-Level Design) Patterns

#### Microservices Architecture Patterns
| Pattern | Implementation | Tradeoffs |
|---------|---------------|-----------|
| **Database per Service** | Each service has its own H2 database | **Pro:** Data autonomy, loose coupling<br>**Con:** No ACID across services |
| **Event-Driven Architecture** | Kafka topics for inter-service communication | **Pro:** Loose coupling, scalability<br>**Con:** Eventual consistency, complexity |
| **SAGA Pattern (Choreography)** | order-service orchestrates via Kafka Streams | **Pro:** No single point of failure<br>**Con:** Hard to debug, complex event flows |
| **CQRS** | Write to Kafka, read from state store | **Pro:** Scalable reads<br>**Con:** Eventual consistency |
| **Event Sourcing** | Order state derived from event log | **Pro:** Audit trail, time travel<br>**Con:** Complex queries |
| **API Gateway** | Single entry point for frontend | **Pro:** Simplified client, security<br>**Con:** Single point of failure |

#### Distributed Systems Challenges
| Challenge | How Reference Repo Handles It | Production Enhancements (Phase 9) |
|-----------|-------------------------------|-----------------------------------|
| **Eventual Consistency** | Accepts that order status converges after all events processed | Add polling endpoint for frontend |
| **Idempotency** | NOT IMPLEMENTED (duplicate events processed twice) | Add event deduplication using Kafka Streams state store |
| **Failure Handling** | Basic compensation logic (rollback) | Add Dead Letter Queue, retry logic, alerts |
| **Observability** | Console logging only | Add distributed tracing (Zipkin), metrics (Prometheus) |
| **Scalability** | 3 Kafka partitions per topic | Add more partitions, consumer instances (Kubernetes HPA) |

### SOLID Principles in Practice

#### Single Responsibility Principle (SRP)
- **OrderController:** Only handles HTTP requests/responses
- **OrderService:** Only handles business logic
- **OrderRepository:** Only handles database operations

#### Open/Closed Principle (OCP)
- Add new order statuses without modifying existing code (enum extensibility)
- Add new Kafka consumers without changing producers

#### Liskov Substitution Principle (LSP)
- All repositories implement `JpaRepository<T, ID>` interface
- Can swap H2 for PostgreSQL without code changes

#### Interface Segregation Principle (ISP)
- `OrderRepository` only exposes needed methods (findById, findAll, save)
- No bloated interfaces with unused methods

#### Dependency Inversion Principle (DIP)
- Services depend on `OrderRepository` interface, not concrete implementation
- High-level modules (services) don't depend on low-level modules (Kafka, JPA)

---

## Frontend Integration Plan

### When to Build Frontend
**After Phase 8 Complete** - Backend APIs are stable and documented.

### Frontend Architecture Options

#### Option 1: React SPA (Recommended)
**Tech Stack:**
- React 18 + TypeScript
- Axios (HTTP client)
- React Query (caching, polling)
- Tailwind CSS (styling)
- SockJS + STOMP (WebSocket for real-time updates)

**Pages:**
1. **Dashboard** - List all orders with status (GET /api/orders)
2. **Create Order** - Form to create new order (POST /api/orders)
3. **Order Details** - Show order status, items, customer (GET /api/orders/{id})
4. **Customer List** - Show all customers (GET /api/customers)
5. **Product Catalog** - Browse products (GET /api/products)

**Real-Time Updates:**
- Poll `GET /api/orders/status/{id}` every 2 seconds after order creation
- Display status transitions: NEW → PENDING → CONFIRMED/REJECTED/ROLLBACK
- Show toast notifications for status changes

#### Option 2: Next.js (SSR)
**When to Use:**
- Need SEO for product catalog
- Server-side rendering for faster initial load

#### Option 3: Vue.js or Angular
**When to Use:**
- Team expertise in these frameworks

### Frontend-Backend Integration

#### CORS Configuration (Already in Phase 8)
```java
@CrossOrigin(origins = "http://localhost:3000")
@RestController
@RequestMapping("/api/orders")
public class OrderController { ... }
```

#### Authentication & Authorization (Future Phase)
- Add Spring Security with JWT tokens
- Frontend sends `Authorization: Bearer <token>` header
- Backend validates JWT and checks roles

#### WebSocket for Real-Time Updates (Phase 9)
**Backend:**
```java
@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {
    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        registry.addEndpoint("/ws").setAllowedOrigins("http://localhost:3000").withSockJS();
    }
    
    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry) {
        registry.enableSimpleBroker("/topic");
        registry.setApplicationDestinationPrefixes("/app");
    }
}

// In OrderService
@Autowired
private SimpMessagingTemplate messagingTemplate;

public void notifyOrderUpdate(Order order) {
    messagingTemplate.convertAndSend("/topic/orders/" + order.getOrderId(), order);
}
```

**Frontend:**
```typescript
import SockJS from 'sockjs-client';
import { Client } from '@stomp/stompjs';

const client = new Client({
  webSocketFactory: () => new SockJS('http://localhost:8080/ws'),
  onConnect: () => {
    client.subscribe('/topic/orders/' + orderId, (message) => {
      const order = JSON.parse(message.body);
      setOrderStatus(order.status);  // Update UI
    });
  }
});
client.activate();
```

### API Contracts (OpenAPI)
After Phase 8, generate TypeScript types from OpenAPI spec:
```bash
npx openapi-typescript http://localhost:8080/v3/api-docs -o src/types/api.ts
```

---

## Testing Strategy

### Unit Testing (All Phases)
**Tools:** JUnit 5, Mockito, AssertJ

**What to Test:**
- Service methods with mocked repositories
- DTO validation rules
- Order status transition logic

**Example:**
```java
@Test
void testOrderCreation_ValidRequest() {
    // Given
    CreateOrderRequest request = new CreateOrderRequest(...);
    when(orderRepository.save(any())).thenReturn(savedOrder);
    
    // When
    Order result = orderService.createOrder(request);
    
    // Then
    assertThat(result.getStatus()).isEqualTo(OrderStatus.NEW);
    verify(orderRepository).save(any());
}
```

### Integration Testing (Phase 3+)
**Tools:** Spring Boot Test, Testcontainers, Spring Kafka Test

**What to Test:**
- End-to-end Kafka message flow
- Database persistence
- REST API endpoints

**Example:**
```java
@SpringBootTest
@Testcontainers
class OrderIntegrationTest {
    @Container
    static KafkaContainer kafka = new KafkaContainer(...);
    
    @Autowired
    private TestRestTemplate restTemplate;
    
    @Test
    void testOrderCreationPublishesKafkaEvent() {
        // When
        ResponseEntity<OrderResponse> response = restTemplate.postForEntity(
            "/api/orders", request, OrderResponse.class);
        
        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        // Verify Kafka event published (use Kafka consumer)
    }
}
```

### SAGA Testing (Phase 5-6)
**Scenarios to Test:**
1. **Happy Path:** Both payment and stock accept → CONFIRMED
2. **Both Reject:** Both services reject → REJECTED
3. **Payment Rollback:** Payment accepts, stock rejects → ROLLBACK
4. **Stock Rollback:** Payment rejects, stock accepts → ROLLBACK
5. **Timeout:** One service doesn't respond within join window
6. **Duplicate Events:** Same event processed twice (idempotency)

**Test Data Setup:**
```java
// Create customer with $100 balance
Customer customer = new Customer(1L, "John", 100, 0);
customerRepository.save(customer);

// Create product with 5 items
Product product = new Product(1L, "Laptop", 5, 0);
productRepository.save(product);

// Order costs $150 (should reject - insufficient balance)
Order order = new Order(1L, 1L, 1L, 1, 150);
```

### Performance Testing (Phase 9)
**Tools:** JMeter, Gatling

**Scenarios:**
- 100 concurrent order creations/sec
- Measure end-to-end latency (order creation → CONFIRMED status)
- Kafka consumer lag monitoring

---

## Success Criteria

### Phase 2 Success Criteria
- [ ] order-service publishes events to Kafka
- [ ] Events contain correct Order JSON
- [ ] Kafka broker is running and reachable
- [ ] Errors handled gracefully (Kafka down, serialization errors)

### Phase 3 Success Criteria
- [ ] payment-service consumes orders from Kafka
- [ ] Customer balance correctly reserved/committed/rolled back
- [ ] Responses published to `payment-orders` topic
- [ ] Database state consistent after each operation

### Phase 4 Success Criteria
- [ ] stock-service consumes orders from Kafka
- [ ] Product inventory correctly reserved/committed/rolled back
- [ ] Responses published to `stock-orders` topic
- [ ] Both payment and stock services run in parallel

### Phase 5 Success Criteria
- [ ] Kafka Streams joins payment and stock responses
- [ ] Correct final status: CONFIRMED, REJECTED, or ROLLBACK
- [ ] Final order persisted in state store
- [ ] GET /orders returns orders from state store

### Phase 6 Success Criteria (Critical!)
- [ ] **Scenario 1:** Both accept → Order CONFIRMED, funds/items committed
- [ ] **Scenario 2:** Both reject → Order REJECTED, nothing reserved
- [ ] **Scenario 3:** Payment accepts, stock rejects → Order ROLLBACK, payment compensated
- [ ] **Scenario 4:** Payment rejects, stock accepts → Order ROLLBACK, stock compensated
- [ ] Database verification: No orphaned reservations

### Phase 7 Success Criteria
- [ ] Orders persisted in H2 database
- [ ] Pagination works
- [ ] Custom queries work (findByCustomerId, findByStatus)
- [ ] Audit timestamps populated

### Phase 8 Success Criteria
- [ ] CORS enabled for frontend
- [ ] Swagger UI accessible
- [ ] All APIs documented in OpenAPI spec
- [ ] Frontend can call APIs without CORS errors

### Phase 9 Success Criteria
- [ ] /actuator/health returns healthy status
- [ ] Testcontainers tests pass
- [ ] DLQ captures failed messages
- [ ] Idempotency prevents duplicate processing

### Phase 10 Success Criteria
- [ ] Docker images built successfully
- [ ] docker-compose up starts all services
- [ ] Services communicate via Kafka in Docker network
- [ ] End-to-end test passes in Docker environment

---

## Timeline Summary

| Phase | Duration | Cumulative Weeks | Deliverable |
|-------|----------|------------------|-------------|
| Phase 1 | 2 weeks | Weeks 1-2 | REST API with validation ✅ |
| Phase 2 | 1 week | Week 3 | Kafka producer 🔄 |
| Phase 3 | 2 weeks | Weeks 4-5 | payment-service |
| Phase 4 | 1 week | Week 5-6 | stock-service |
| Phase 5 | 2 weeks | Weeks 6-7 | Kafka Streams join |
| Phase 6 | 1 week | Week 7-8 | SAGA compensation |
| Phase 7 | 2 weeks | Weeks 8-9 | JPA persistence |
| Phase 8 | 1 week | Week 9-10 | Frontend APIs |
| Phase 9 | 2-3 weeks | Weeks 10-12 | Production features |
| Phase 10 | 2 weeks | Weeks 12-14 | Docker/K8s |
| **Total** | **14 weeks** | | **Production-ready system** |

**Accelerated Path (Minimum Viable Product):**
- Skip Phase 7 (use in-memory storage)
- Skip Phase 9-10 (no Docker/K8s)
- **Total:** 8 weeks to working SAGA implementation

---

## Appendix: Reference Repository Analysis Summary

### Key Findings
1. **No API Gateway:** Each service exposes its own endpoints (or none for consumers)
2. **No Service Discovery:** Static Kafka broker configuration (localhost:9092)
3. **No Authentication/Authorization:** Open endpoints (add in Phase 9)
4. **H2 In-Memory Database:** Data lost on restart (replace with PostgreSQL for production)
5. **Single Kafka Broker:** No replication (use 3 brokers for production)
6. **Testcontainers for Testing:** Modern approach to integration tests
7. **Spring Boot 4.1 + Java 21:** Latest versions (use 3.5 + Java 17 for stability)

### What Reference Repo Does Well
- Clean SAGA choreography pattern
- Proper compensation logic
- Kafka Streams for stateful processing
- Good separation of concerns
- Testcontainers for reliable tests

### What Reference Repo Lacks (Add in Your Implementation)
- API Gateway (Spring Cloud Gateway)
- Distributed tracing (Zipkin)
- Metrics monitoring (Prometheus)
- Idempotency handling
- Dead Letter Queue
- Circuit breakers
- WebSocket for real-time updates
- Frontend integration

---

## Next Steps

### Immediate (Week 3)
1. Finish Phase 2: Kafka producer in order-service
2. Test with local Kafka broker
3. Verify events published to `orders` topic

### Short-Term (Weeks 4-6)
1. Build payment-service (Phase 3)
2. Build stock-service (Phase 4)
3. Test parallel event processing

### Medium-Term (Weeks 7-9)
1. Implement Kafka Streams join (Phase 5)
2. Add SAGA compensation (Phase 6)
3. Add JPA persistence (Phase 7)

### Long-Term (Weeks 10-14)
1. Prepare backend for frontend (Phase 8)
2. Add production features (Phase 9)
3. Containerize with Docker (Phase 10)
4. **Build frontend** (separate project)

---

## Questions & Decisions

### Architecture Decisions
- **SAGA Pattern:** Choreography (event-driven) vs Orchestration (central coordinator)?
  - **Decision:** Choreography (like reference repo) - more scalable, no SPOF
- **Database:** H2 (in-memory) vs PostgreSQL (production)?
  - **Decision:** H2 for learning, migrate to PostgreSQL in Phase 9
- **API Gateway:** Add or not?
  - **Decision:** Add in Phase 8 (simplifies frontend integration)

### Technology Choices
- **Java Version:** 17 (stable) vs 21 (latest)?
  - **Decision:** Java 17 now, upgrade to 21 in Phase 7
- **Spring Boot Version:** 3.5 (stable) vs 4.1 (latest)?
  - **Decision:** 3.5 now, upgrade to 4.1 in Phase 7
- **Frontend Framework:** React vs Vue vs Angular?
  - **Decision:** React (most popular, best ecosystem)

---

## Resources

### Reference Repository
- GitHub: https://github.com/piomin/sample-spring-kafka-microservices
- Author Blog: https://piotrminkowski.com

### Official Documentation
- Spring Kafka: https://docs.spring.io/spring-kafka/reference/
- Kafka Streams: https://kafka.apache.org/documentation/streams/
- Spring Data JPA: https://docs.spring.io/spring-data/jpa/reference/

### Learning Resources
- "Building Microservices" by Sam Newman
- "Designing Data-Intensive Applications" by Martin Kleppmann
- "Microservices Patterns" by Chris Richardson

---

**Document Version:** 1.0  
**Last Updated:** July 2026  
**Status:** Living document - update after each phase completion  
**Owner:** Hitesh Kumar
