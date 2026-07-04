# Alignment with Reference Repository

> **Reference:** [piomin/sample-spring-kafka-microservices](https://github.com/piomin/sample-spring-kafka-microservices)  
> **Status:** Phase 2 Complete → Phase 3 Ready  
> **Last Updated:** July 4, 2026

---

## 📊 Implementation Progress

### Reference Architecture Services

| Service | Port | Status | Our Implementation |
|---------|------|--------|-------------------|
| **order-service** | 8080 | 🟡 Partial | ✅ REST API + Kafka Producer (Port 8081) |
| **payment-service** | Auto | ⏳ Next | 📋 Task created, ready to build |
| **stock-service** | Auto | ⏳ Future | 📅 Phase 5 (Week 8-9) |

---

## 🔍 Feature Comparison

### order-service

| Feature | Reference Repo | Our Implementation | Status |
|---------|---------------|-------------------|--------|
| **REST API** | ✅ POST /orders, GET /orders | ✅ POST /api/orders, GET /api/orders/{id}, GET /api/orders | ✅ Complete |
| **Kafka Producer** | ✅ Publish to "orders" topic | ✅ Publish to "order-events" topic | ✅ Complete |
| **Event Model** | Order (status=NEW/CONFIRMED/REJECTED) | OrderCreatedEvent | ✅ Complete |
| **Serialization** | JsonSerializer | JsonSerializer | ✅ Complete |
| **Error Handling** | Basic | @RestControllerAdvice with custom exceptions | ✅ Enhanced |
| **Kafka Consumer** | ✅ @KafkaListener for payment/stock responses | ⏳ Not yet | 📋 Phase 4 |
| **Kafka Streams** | ✅ Join payment + stock streams | ⏳ Not yet | 📋 Phase 4 |
| **State Store** | ✅ KTable for queryable orders | ⏳ Not yet | 📋 Phase 4 |
| **SAGA Orchestration** | ✅ Determine CONFIRMED/ROLLBACK | ⏳ Not yet | 📋 Phase 4 |

**Key Differences:**
- **Topic naming:** We use `order-events` instead of `orders` (more descriptive)
- **Port:** 8081 instead of 8080 (avoid conflict with Kafka UI)
- **Error handling:** More comprehensive with GlobalExceptionHandler
- **Documentation:** Extensive docs added (6,000+ lines)

---

### payment-service

| Feature | Reference Repo | Our Implementation | Status |
|---------|---------------|-------------------|--------|
| **Kafka Consumer** | ✅ Listen to "orders" topic | 📋 Planned: Listen to "order-events" | ⏳ Task created |
| **JPA / H2 Database** | ✅ Customer entity with balance | 📋 Planned: Same structure | ⏳ Task created |
| **Balance Validation** | ✅ Check amountAvailable | 📋 Planned: Same logic | ⏳ Task created |
| **Reserve Funds** | ✅ amountAvailable → amountReserved | 📋 Planned: Same | ⏳ Task created |
| **Kafka Producer** | ✅ Publish to "payment-orders" | 📋 Planned: Publish to "payment-events" | ⏳ Task created |
| **Response Event** | Order with status=ACCEPT/REJECT | PaymentProcessedEvent | 📋 Planned |
| **SAGA Confirm** | ✅ Deduct from amountReserved | ⏳ Not yet | 📋 Phase 4 |
| **SAGA Rollback** | ✅ Return to amountAvailable | ⏳ Not yet | 📋 Phase 4 |
| **Idempotency** | ❌ Not implemented | 📋 Planned: Phase 6 | 📅 Future |

**Key Differences:**
- **Topic naming:** `payment-events` instead of `payment-orders` (consistency)
- **Event model:** Separate `PaymentProcessedEvent` DTO (cleaner than reusing Order)
- **Test data:** DataInitializer with 10 customers (reference uses Datafaker with 100)

---

### stock-service

| Feature | Reference Repo | Our Implementation | Status |
|---------|---------------|-------------------|--------|
| **Entire Service** | ✅ Complete | ⏳ Not started | 📅 Phase 5 |

---

## 🏗️ Architecture Alignment

### Current Architecture (Our Implementation)

```
┌─────────────────────────────────────────────────┐
│ Phase 2 Complete (July 1, 2026)                │
└─────────────────────────────────────────────────┘

Client → order-service :8081 (REST API)
            │
            │ Publish OrderCreatedEvent
            ↓
        Kafka Broker
            │
            │ Topic: order-events
            ↓
        (No consumer yet)
```

### Target Architecture (Reference Repo)

```
┌─────────────────────────────────────────────────┐
│ Full SAGA Implementation                        │
└─────────────────────────────────────────────────┘

Client → order-service :8080
            │
            │ Publish Order (status=NEW)
            ↓
        ┌───────────┐
        │   Kafka   │
        │  "orders" │
        └─────┬─────┘
              │
    ┌─────────┴─────────┐
    ↓                   ↓
payment-service    stock-service
    │                   │
    │ Publish to        │ Publish to
    │ "payment-orders"  │ "stock-orders"
    └─────────┬─────────┘
              │
              ↓
        order-service
        (Kafka Streams)
              │
              │ Join streams → Determine status
              │ Publish to "orders" (CONFIRMED/ROLLBACK)
              ↓
    ┌─────────┴─────────┐
    ↓                   ↓
payment-service    stock-service
(Confirm/Rollback) (Confirm/Rollback)
```

### Our Next Steps (Phase 3-4)

```
┌─────────────────────────────────────────────────┐
│ After Phase 3 (Week 4-5)                        │
└─────────────────────────────────────────────────┘

Client → order-service :8081
            │
            │ Publish OrderCreatedEvent
            ↓
        ┌──────────────┐
        │    Kafka     │
        │ order-events │
        └──────┬───────┘
               │
               ↓
        payment-service :8082
               │
               │ Validate balance (H2 DB)
               │ Reserve funds
               │
               │ Publish PaymentProcessedEvent
               ↓
        ┌──────────────┐
        │    Kafka     │
        │payment-events│
        └──────────────┘

(order-service doesn't consume yet - Phase 4 will add this)
```

---

## 📋 SAGA Pattern Comparison

### Reference Implementation

**Phase 1: Reserve**
```
1. order-service publishes Order (status=NEW)
2. payment-service reserves funds → publishes Order (status=ACCEPT/REJECT)
3. stock-service reserves items → publishes Order (status=ACCEPT/REJECT)
```

**Phase 2: Decision (Kafka Streams)**
```
4. order-service joins payment + stock streams
5. Apply logic:
   - Both ACCEPT → status=CONFIRMED
   - Both REJECT → status=REJECTED
   - One ACCEPT, one REJECT → status=ROLLBACK (source=rejecting service)
6. Publish final decision to "orders" topic
```

**Phase 3: Commit/Compensate**
```
7. payment-service receives final decision:
   - CONFIRMED → Confirm (deduct from amountReserved)
   - ROLLBACK (source != "payment") → Rollback (return to amountAvailable)
8. stock-service receives final decision:
   - CONFIRMED → Confirm (deduct from reservedItems)
   - ROLLBACK (source != "stock") → Rollback (return to availableItems)
```

### Our Implementation Plan

**Phase 3 (Current Task): Reserve Phase**
```
1. order-service publishes OrderCreatedEvent
2. payment-service reserves funds → publishes PaymentProcessedEvent (ACCEPT/REJECT)
```

**Phase 4: Decision Phase**
```
3. order-service consumes PaymentProcessedEvent
4. Store order state (in-memory Map or Kafka state store)
5. Determine status:
   - ACCEPT → status=CONFIRMED
   - REJECT → status=REJECTED
6. Publish final decision event
```

**Phase 4: Commit/Compensate Phase**
```
7. payment-service receives final decision:
   - CONFIRMED → confirm() method
   - REJECTED → rollback() method
```

**Phase 5: Add Stock Service**
```
8. Build stock-service (similar to payment-service)
9. order-service joins payment + stock responses
10. Three-way decision logic (both ACCEPT → CONFIRMED)
```

---

## 🔧 Technical Differences

### Topic Naming Convention

| Purpose | Reference Repo | Our Implementation | Reasoning |
|---------|---------------|-------------------|-----------|
| Order events | `orders` | `order-events` | More descriptive |
| Payment responses | `payment-orders` | `payment-events` | Consistency |
| Stock responses | `stock-orders` | `stock-events` | Consistency |

**Rationale:** Adding `-events` suffix makes it clear these are event streams, not entity collections.

---

### Event Models

**Reference Repo:**
- Reuses `Order` entity for all events
- Uses `status` field to indicate event type (NEW, ACCEPT, REJECT, CONFIRMED)
- Single class for everything

**Our Implementation:**
- Separate event classes: `OrderCreatedEvent`, `PaymentProcessedEvent`
- Explicit event types (better type safety)
- Cleaner separation of concerns

**Trade-off:**
- Reference: Less code, but mixed concerns (domain + events)
- Ours: More code, but cleaner architecture

---

### Database Strategy

**Reference Repo:**
- No database in order-service (Kafka Streams state store only)
- H2 in payment-service and stock-service

**Our Implementation:**
- Same strategy (in-memory for order-service, H2 for payment-service)
- Will add Kafka Streams state store in Phase 4

---

## ✅ What We've Done Better

1. **Comprehensive Documentation**
   - 6,000+ lines of docs vs. minimal README in reference
   - Deep dives on producer internals, SAGA pattern, error handling
   - Learning-focused approach

2. **Error Handling**
   - GlobalExceptionHandler with custom exceptions
   - Consistent ErrorResponse format
   - Reference has basic error handling

3. **Code Comments & JavaDoc**
   - Extensive inline documentation explaining WHY
   - Reference has minimal comments

4. **Testing Infrastructure**
   - `test-kafka-integration.sh` automated test script
   - Reference relies on manual Postman testing

5. **Logging**
   - Comprehensive logging at all stages
   - Success/failure callbacks with metadata (offset, partition)

---

## 📚 What We Can Learn from Reference

1. **Kafka Streams Usage**
   - Reference uses KStream for joining payment + stock responses
   - We'll implement this in Phase 4

2. **State Store Queries**
   - Reference exposes state store via REST API (`GET /orders`)
   - Allows querying Kafka state store like a database

3. **Datafaker for Test Data**
   - Reference uses Datafaker library for realistic test data
   - We use manual test data (simpler for learning)

4. **Join Windows**
   - Reference uses `JoinWindows.of(Duration.ofSeconds(10))`
   - Important for handling late-arriving messages

---

## 🎯 Roadmap to Full Alignment

### Phase 3: Payment Service (Week 4-5) - NEXT
**Goal:** Match payment-service feature set (Reserve phase only)

**Deliverables:**
- [x] Task document created (`/tasks/03-build-payment-service-consumer.md`)
- [ ] Create payment-service project structure
- [ ] Customer entity with reserve/confirm/rollback methods
- [ ] @KafkaListener consuming order-events
- [ ] PaymentService with balance validation
- [ ] Publish PaymentProcessedEvent (ACCEPT/REJECT)
- [ ] End-to-end testing

**Alignment:** 60% of reference payment-service features

---

### Phase 4: SAGA Orchestration (Week 6-7)
**Goal:** Match order-service orchestration (Kafka Streams)

**Deliverables:**
- [ ] order-service consumes payment-events
- [ ] Store order state (in-memory or Kafka state store)
- [ ] Determine final status (CONFIRMED/REJECTED)
- [ ] Publish final decision
- [ ] payment-service implements confirm/rollback

**Alignment:** 80% of reference order-service features (no stock-service yet)

---

### Phase 5: Stock Service (Week 8-9)
**Goal:** Add second participant service

**Deliverables:**
- [ ] Create stock-service (mirror payment-service structure)
- [ ] Product entity with availableItems/reservedItems
- [ ] Stock validation logic
- [ ] Publish stock-events
- [ ] order-service joins payment + stock streams
- [ ] Three-way decision logic

**Alignment:** 95% of reference architecture

---

### Phase 6-8: Advanced Features
**Goal:** Production-readiness features

**Deliverables:**
- [ ] Idempotency (store processed order IDs)
- [ ] Dead Letter Queue (DLQ)
- [ ] Retry with exponential backoff
- [ ] Monitoring with actuator metrics
- [ ] Integration tests
- [ ] Docker Compose for all services

**Alignment:** 100% + enhancements

---

## 📊 Current Completion Metrics

| Metric | Progress | Details |
|--------|----------|---------|
| **Services Built** | 1/3 (33%) | order-service complete, payment-service next |
| **SAGA Phases** | 0/3 (0%) | Reserve/Decision/Commit phases pending |
| **Kafka Patterns** | 1/2 (50%) | Producer ✅, Consumer ⏳ |
| **Core Features** | 40% | REST API ✅, Events ✅, Orchestration ⏳ |
| **Documentation** | 120% | Far exceeds reference repo |

---

## 🎓 Learning Alignment

### What We're Learning (Aligned with Reference)

✅ **From Reference Repo:**
- Event-driven architecture
- SAGA pattern for distributed transactions
- Kafka producer/consumer patterns
- Spring Kafka integration
- JPA with H2 database
- Microservices coordination

✅ **Additional Learning (Our Enhancements):**
- Deep dive into Kafka internals (RecordAccumulator, Sender Thread)
- Exception handling best practices
- DTO vs domain model separation
- Async programming with CompletableFuture
- Documentation as code

---

## 🔗 Quick Links

### Reference Repository
- **GitHub:** https://github.com/piomin/sample-spring-kafka-microservices
- **Blog Series:**
  - Part 1: https://piotrminkowski.com/2023/11/03/kafka-streams-for-microservices-part-1/
  - Part 2: SAGA pattern implementation

### Our Documentation
- **Project Plan:** `/docs/PROJECT-PLAN.md`
- **Architecture Overview:** `/docs/ARCHITECTURE-OVERVIEW.md`
- **Task 03 (Next):** `/tasks/03-build-payment-service-consumer.md`
- **Session Notes:** `/notes/SESSION-NOTES.md`

---

## ✅ Validation Checklist

Before moving to Phase 3, verify:
- [x] order-service compiles and runs
- [x] Kafka is running (docker-compose up -d)
- [x] Can create orders via POST /api/orders
- [x] OrderCreatedEvent visible in Kafka UI (order-events topic)
- [x] Logs show successful message publishing
- [x] Task 03 document created and reviewed
- [x] Notes document created for reference
- [x] Alignment document created (THIS FILE)

**Status:** ✅ Ready to start Phase 3

---

## 🚀 Next Session Action Items

1. **Read Task 03:** `/tasks/03-build-payment-service-consumer.md`
2. **Create payment-service directory structure**
3. **Set up pom.xml with Kafka + JPA dependencies**
4. **Configure application.yml (Kafka consumer + H2)**
5. **Create Customer entity**

**Estimated Time:** 2-3 hours for initial setup

---

**Last Updated:** July 4, 2026  
**Next Milestone:** Phase 3 Week 1 - Payment Service Scaffolding  
**Current Alignment:** 40% → Target: 100% by Week 9
