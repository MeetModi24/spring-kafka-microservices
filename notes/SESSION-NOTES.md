# Session Notes - Spring Kafka Microservices Project

> **Last Updated:** July 4, 2026  
> **Current Phase:** Phase 2 Complete → Ready for Phase 3

---

## 📊 Project Status Overview

### Completed Phases

#### ✅ Phase 1: Foundation (Week 1-2)
**Duration:** 2 weeks  
**Status:** COMPLETE

**What We Built:**
- order-service with REST API (POST, GET endpoints)
- Domain models: Order, OrderItem, OrderStatus
- DTOs: CreateOrderRequest, OrderResponse, ErrorResponse
- Bean validation with Jakarta Validation
- In-memory storage (ConcurrentHashMap)

**Key Files:**
- `/order-service/src/main/java/com/example/orderservice/controller/OrderController.java`
- `/order-service/src/main/java/com/example/orderservice/service/OrderService.java`
- `/order-service/src/main/java/com/example/orderservice/model/Order.java`

**Learning Outcomes:**
- Constructor injection and dependency injection
- DTO pattern for API/domain separation
- REST controller patterns
- Stream-based transformations

---

#### ✅ Phase 2: Error Handling + Kafka Producer (Week 3)
**Duration:** 1 week  
**Status:** COMPLETE (July 1, 2026)

**What We Built:**
1. **Custom Exceptions:**
   - `OrderNotFoundException.java` (404)
   - `InvalidOrderException.java` (400)

2. **Global Exception Handler:**
   - `GlobalExceptionHandler.java` with @RestControllerAdvice
   - Handles 404, 400, 500 with consistent ErrorResponse format

3. **Kafka Producer:**
   - `OrderEventProducer.java` - Publishes OrderCreatedEvent
   - `OrderCreatedEvent.java` - Event DTO with nested OrderItemEvent
   - Asynchronous publishing with CompletableFuture callbacks
   - Success/failure logging

4. **Kafka Configuration:**
   - `application.yml` updated with producer settings
   - acks=all, retries=3, idempotence enabled
   - JsonSerializer with type mapping

5. **Documentation Created:**
   - 8 comprehensive Kafka documentation files (4,939 lines total)
   - Added deep dive on producer internals to `03-producers-consumers.md`

**Key Files:**
- `/order-service/src/main/java/com/example/orderservice/exception/GlobalExceptionHandler.java`
- `/order-service/src/main/java/com/example/orderservice/event/OrderCreatedEvent.java`
- `/order-service/src/main/java/com/example/orderservice/service/OrderEventProducer.java`
- `/docs/03-kafka/` (8 documentation files)

**Learning Outcomes:**
- @RestControllerAdvice for AOP-based exception handling
- KafkaTemplate for message publishing
- Key-based partitioning for ordering guarantees
- Async vs sync vs fire-and-forget sending
- Producer internals: RecordAccumulator, Sender Thread, batching

**Testing:**
- Created `test-kafka-integration.sh` for automated testing
- All 6 test scenarios passing (order creation, errors, partitioning)
- Events verified in Kafka UI at http://localhost:8080

---

### 🎯 Current Position: Ready for Phase 3

**Next Task:** Build payment-service (Kafka Consumer)  
**Task File:** `/tasks/03-build-payment-service-consumer.md`

---

## 🏗️ Architecture Summary

### Current System (After Phase 2)

```
Client (Postman/Browser)
    │
    │ REST API (HTTP)
    ↓
┌─────────────────────────┐
│   order-service :8081   │
│   ┌─────────────────┐   │
│   │ OrderController │   │
│   └────────┬────────┘   │
│            ↓             │
│   ┌─────────────────┐   │
│   │  OrderService   │   │
│   └────────┬────────┘   │
│            ↓             │
│   ┌─────────────────────┐
│   │ OrderEventProducer  │
│   └────────┬────────────┘
└────────────┼─────────────┘
             │
             │ Publish OrderCreatedEvent
             ↓
    ┌────────────────┐
    │ Kafka Broker   │
    │ Topic:         │
    │ order-events   │
    └────────────────┘
```

### Target Architecture (Phase 3+)

```
order-service :8081
    │
    ↓ Publish OrderCreatedEvent
┌───────────────────┐
│   Kafka Broker    │
│   ┌─────────────┐ │
│   │order-events │ │
│   └─────┬───────┘ │
│         │         │
│   ┌─────▼──────────┐
│   │payment-events  │
│   └────────────────┘
└─────┬──────▲────────┘
      │      │
      │      └── Publish PaymentProcessedEvent
      ↓
payment-service :8082
    ├─ @KafkaListener (order-events)
    ├─ Validate balance (JPA + H2)
    ├─ Reserve funds
    └─ Publish response
```

---

## 💡 Key Technical Concepts Learned

### 1. Kafka Producer Deep Dive

**Two-Thread Architecture:**
- **Application threads:** Only write to RecordAccumulator buffer (fast, non-blocking)
- **Sender thread:** Background I/O thread handles network communication

**Message Flow:**
1. `send()` → Serialize → Choose partition (hash(key) % partitions)
2. Add to RecordAccumulator buffer
3. Return CompletableFuture immediately (application thread continues)
4. Sender thread batches messages (batch-size: 16KB, linger-ms: 10ms)
5. Send ProduceRequest over TCP
6. Broker writes to partition log + replicates (if acks=all)
7. Sender thread receives ACK → Completes CompletableFuture → Triggers callbacks

**Performance Impact:**
- Batching: 100x throughput improvement (1000 requests → 10 batches)
- Async: Application never blocks on network I/O
- Sync (.get()): 20x slower - blocks application thread

**Configuration:**
```yaml
batch-size: 16384      # 16KB batch
linger-ms: 10          # Max wait time to fill batch
buffer-memory: 33554432 # 32MB RecordAccumulator buffer
acks: all              # Wait for all replicas
retries: 3             # Retry failed sends
enable.idempotence: true # Exactly-once per partition
```

---

### 2. Event-Driven Architecture Patterns

**Why Event DTOs ≠ Domain Models:**
- Domain model (Order) has internal fields not for publishing
- Event schema is a contract with consumers (must be stable)
- Allows domain model to evolve without breaking consumers

**Key Design:**
- **Key = orderId:** All events for same order → same partition → ordering guaranteed
- **Value = event JSON:** Actual message payload
- **Topic naming:** `order-events`, `payment-events` (plural, domain-based)

---

### 3. Exception Handling Patterns

**@RestControllerAdvice (AOP):**
- Intercepts exceptions globally
- Separates cross-cutting concern (error handling) from business logic
- Single place to define HTTP status codes

**Exception Hierarchy:**
```
RuntimeException
├─ OrderNotFoundException (404)
├─ InvalidOrderException (400)
└─ Exception (500 - catch-all)
```

**ErrorResponse Structure:**
```json
{
  "timestamp": "2026-07-01T...",
  "status": 404,
  "error": "Not Found",
  "message": "Order not found: invalid-id",
  "path": "/api/orders/invalid-id",
  "details": null
}
```

---

## 🔧 Development Environment

### Ports
- **8080:** Kafka UI (http://localhost:8080)
- **8081:** order-service REST API
- **8082:** payment-service (future - no REST, only Kafka)
- **9092:** Kafka broker
- **2181:** Zookeeper

### Docker Services
```bash
docker-compose up -d  # Start Kafka + Zookeeper + Kafka UI
docker-compose ps     # Check status
docker-compose logs -f kafka  # View Kafka logs
docker-compose down   # Stop all services
```

### Maven Commands
```bash
mvn clean compile     # Compile
mvn spring-boot:run   # Run service
mvn test             # Run tests (future)
```

---

## 📝 Code Patterns & Conventions

### 1. Dependency Injection
**Always use constructor injection:**
```java
@Service
public class OrderService {
    private final OrderEventProducer eventProducer;
    
    public OrderService(OrderEventProducer eventProducer) {
        this.eventProducer = eventProducer;
    }
}
```

**Why?**
- Immutable dependencies (final fields)
- Testability (can mock in tests)
- Fails fast (missing dependency = compilation error)

---

### 2. Logging Pattern
**Use SLF4J with Lombok @Slf4j:**
```java
@Slf4j
@Service
public class OrderEventProducer {
    public void publish(OrderCreatedEvent event) {
        log.info("Publishing: orderId={}", event.getOrderId());
        // Business logic...
        log.info("Success: orderId={}, offset={}", event.getOrderId(), offset);
    }
}
```

**Log Levels:**
- `DEBUG`: Detailed flow (deserialization, partition selection)
- `INFO`: Important events (message sent, order created)
- `WARN`: Recoverable issues (customer not found)
- `ERROR`: Failures requiring attention (Kafka down, DB error)

---

### 3. DTO vs Domain Model
**Domain Model (Internal):**
```java
@Entity
public class Order {
    private String orderId;
    private String customerId;
    private List<OrderItem> items;
    private OrderStatus status;  // Internal state
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;  // Internal tracking
    
    public boolean isValid() { ... }  // Business logic
}
```

**Event DTO (External Contract):**
```java
public class OrderCreatedEvent {
    private String orderId;
    private String customerId;
    private List<OrderItemEvent> items;
    private BigDecimal totalAmount;
    private LocalDateTime createdAt;
    // NO status, NO updatedAt, NO business methods
}
```

---

### 4. Async Callback Pattern
**Recommended for production:**
```java
CompletableFuture<SendResult> future = kafkaTemplate.send(topic, key, value);

future.whenComplete((result, ex) -> {
    if (ex == null) {
        log.info("Success: offset={}", result.getRecordMetadata().offset());
    } else {
        log.error("Failed: {}", ex.getMessage());
        // Store in DB for retry, send to DLQ, alert ops
    }
});

// Application continues immediately (non-blocking)
```

---

## 🐛 Common Issues & Solutions

### Issue 1: "Connection refused to localhost:9092"
**Cause:** Kafka not running  
**Solution:**
```bash
docker-compose up -d
sleep 30  # Wait for Kafka to initialize
```

---

### Issue 2: Topic not created
**Cause:** Auto-creation disabled (rare)  
**Solution:**
```bash
docker exec -it kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic order-events \
  --partitions 3 \
  --replication-factor 1
```

---

### Issue 3: JsonSerializer fails with "Type not mapped"
**Cause:** Missing type mapping in application.yml  
**Solution:**
```yaml
spring:
  kafka:
    producer:
      properties:
        spring.json.type.mapping: orderCreated:com.example.orderservice.event.OrderCreatedEvent
```

---

### Issue 4: CompletableFuture never completes
**Cause:** Kafka broker down or network issue  
**Solution:**
- Check `docker-compose ps` - is Kafka running?
- Check logs: `docker-compose logs kafka`
- Increase timeout:
  ```yaml
  spring.kafka.producer.properties:
    request.timeout.ms: 30000
    delivery.timeout.ms: 120000
  ```

---

## 📚 Documentation Structure

```
/docs
├── 01-fundamentals/
│   └── java-core-concepts.md (800+ lines)
├── 02-spring-boot/
│   └── error-handling-deep-dive.md (600+ lines)
├── 03-kafka/
│   ├── 01-kafka-fundamentals.md (554 lines)
│   ├── 02-kafka-configuration.md (1,049 lines)
│   ├── 03-producers-consumers.md (849 lines + deep dive)
│   ├── 05-error-handling.md (678 lines)
│   ├── 07-event-driven-patterns.md (675 lines)
│   ├── GETTING-STARTED.md (458 lines)
│   ├── KAFKA-QUICK-REFERENCE.md (529 lines)
│   └── README.md (147 lines)
├── ARCHITECTURE-OVERVIEW.md (28KB)
├── PROJECT-PLAN.md (66KB)
└── LEARNING-GUIDE.md (15KB)

/tasks
├── 01-implement-order-service.md
├── 02-add-error-handling-kafka-producer.md
└── 03-build-payment-service-consumer.md (NEW)

/notes
└── SESSION-NOTES.md (THIS FILE)
```

**Total Documentation:** ~6,000+ lines

---

## 🎯 Next Steps Roadmap

### Phase 3: Payment Service (Week 4-5) - NEXT
**Goal:** Build Kafka consumer with JPA persistence

**Week 1: Scaffolding**
- [ ] Create payment-service Maven module
- [ ] Configure Kafka consumer + H2 database
- [ ] Create Customer entity with balance management
- [ ] Initialize test data (10 customers)
- [ ] Test database connection

**Week 2: Business Logic**
- [ ] Implement payment validation service
- [ ] Add @KafkaListener for order-events
- [ ] Reserve funds logic (SAGA phase 1)
- [ ] Publish PaymentProcessedEvent (ACCEPT/REJECT)
- [ ] End-to-end testing

**Deliverables:**
- payment-service consuming order-events
- Balance validation with H2 database
- Response events published to payment-events topic

**Task File:** `/tasks/03-build-payment-service-consumer.md`

---

### Phase 4: SAGA Orchestration (Week 6-7)
**Goal:** Complete distributed transaction loop

**order-service additions:**
- Consume payment-events
- Store order state (in-memory or Kafka state store)
- Determine final status: CONFIRMED / REJECTED / ROLLBACK
- Publish final decision to order-events

**payment-service additions:**
- Listen for final decision events
- Confirm reservations (deduct from amountReserved)
- Rollback reservations (return to amountAvailable)

---

### Phase 5: Stock Service (Week 8-9)
**Goal:** Add second participant service

**Build stock-service:**
- Similar structure to payment-service
- Product entity with availableItems / reservedItems
- Validates stock availability
- Publishes stock-events (ACCEPT/REJECT)

**order-service orchestration:**
- Join payment-events + stock-events (Kafka Streams)
- Both ACCEPT → CONFIRMED
- One REJECT → ROLLBACK

---

### Phase 6-8: Advanced Features
- Kafka Streams for stateful processing
- Idempotency checks (prevent duplicate processing)
- Dead Letter Queue (DLQ) for failed messages
- Retry with exponential backoff
- Monitoring with actuator metrics
- Integration tests

---

## 🔗 Quick Reference Links

### Local URLs
- **Kafka UI:** http://localhost:8080
- **order-service API:** http://localhost:8081/api/orders
- **order-service health:** http://localhost:8081/actuator/health
- **H2 Console (future):** http://localhost:8082/h2-console

### Reference Repository
- **GitHub:** https://github.com/piomin/sample-spring-kafka-microservices
- **Blog:** https://piotrminkowski.com/2023/11/03/kafka-streams-for-microservices-part-1/

### Official Docs
- **Spring Kafka:** https://spring.io/projects/spring-kafka
- **Apache Kafka:** https://kafka.apache.org/documentation/
- **Spring Data JPA:** https://spring.io/projects/spring-data-jpa

---

## 💭 Personal Learning Notes

### What Clicked
1. **RecordAccumulator insight:** Understanding that `send()` returns immediately because messages go to a buffer, not network, was the key to understanding Kafka's high throughput.

2. **Event DTOs separate from domain:** Initially seemed redundant, but now clear why event schema stability matters for backward compatibility.

3. **AOP for exceptions:** @RestControllerAdvice is elegant - keeps business logic clean while centralizing error handling.

4. **CompletableFuture pattern:** Non-blocking async is crucial for microservices scalability.

### What to Review
1. Kafka Streams API (Phase 4)
2. JPA relationships and transactions
3. Idempotency patterns (storing processed IDs)
4. Dead Letter Queue setup

### Interview Prep Topics
- "Explain how Kafka achieves high throughput" → RecordAccumulator + batching
- "Describe SAGA pattern implementation" → Reserve/Confirm/Compensate phases
- "Why separate event DTOs from domain models?" → Contract stability
- "How does @KafkaListener work internally?" → Polling, deserialization, offset commit

---

## ✅ Milestones Achieved

- [x] **June 26:** Project kickoff, order-service scaffolding
- [x] **June 27-29:** Domain models, DTOs, REST endpoints
- [x] **June 30:** Error handling, custom exceptions
- [x] **July 1:** Kafka producer, event publishing, comprehensive documentation
- [x] **July 4:** Producer internals deep dive, Task 03 created
- [ ] **July 5-12:** Phase 3 - Build payment-service
- [ ] **July 13-20:** Phase 4 - SAGA orchestration
- [ ] **July 21+:** Advanced features

---

## 🙏 Acknowledgments

**Learning Resources:**
- Reference repo by Piotr Minkowski (piomin/sample-spring-kafka-microservices)
- Spring Kafka documentation
- Kafka: The Definitive Guide (book concepts)

**AI Assistance:**
- Claude Code for implementation guidance
- Documentation generation workflow
- Architecture pattern recommendations

---

**Last Session:** July 4, 2026  
**Next Session Goal:** Start Phase 3 - Create payment-service project structure  
**Status:** Ready to build! 🚀
