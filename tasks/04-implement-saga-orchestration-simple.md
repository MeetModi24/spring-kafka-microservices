# Task 04: Implement SAGA Orchestration (Phase 4 - Simple Pattern)

> **Current Status:** ✅ FILES ALREADY CREATED (July 5th)  
> **Phase:** 4 of 6  
> **Duration:** 1-2 days (testing & verification only)  
> **Prerequisites:** ✅ Phase 3 complete (payment-service with SAGA reserve/confirm/rollback)

---

## 🎯 Phase 4 Goals

Complete the **SAGA orchestration pattern** by implementing order-service orchestration that:
1. Consumes `PaymentProcessedEvent` from Kafka `payment-events` topic
2. Makes final decision (CONFIRMED or REJECTED) based on payment status
3. Updates order status in existing `OrderService.orderStore`
4. Publishes `FinalDecisionEvent` to `order-events` topic
5. Triggers payment-service to confirm (deduct) or reject (no-op) the reservation

**Pattern:** Simple 2-service orchestration (order + payment)  
**State:** Uses existing `ConcurrentHashMap` in OrderService

This aligns with the reference architecture: [piomin/sample-spring-kafka-microservices](https://github.com/piomin/sample-spring-kafka-microservices) but uses the **simple pattern** (Phase 4). Phase 5 will migrate to **Kafka Streams** for 3-service orchestration.

---

## 📊 Architecture Overview

### Current Flow (After Phase 4)

```
Client → POST /api/orders
    ↓
order-service (REST API)
    ↓ Publishes OrderCreatedEvent
Kafka topic: order-events
    ↓ Consumes
payment-service
    ↓ Reserve funds (amountAvailable → amountReserved)
    ↓ Publishes PaymentProcessedEvent (ACCEPT/REJECT)
Kafka topic: payment-events
    ↓ Consumes (NEW in Phase 4!)
order-service (PaymentEventConsumer)
    ↓
OrderOrchestrationService
    ↓ Decision: ACCEPT → CONFIRMED, REJECT → REJECTED
    ↓ Updates Order.status in orderStore
    ↓ Publishes FinalDecisionEvent
Kafka topic: order-events
    ↓ Consumes (NEW in Phase 4!)
payment-service (DecisionEventConsumer)
    ↓ CONFIRMED → confirm() (amountReserved → 0, permanently deducted)
    ↓ REJECTED → no-op (already rejected, nothing reserved)
✅ SAGA Complete
```

### Services State

| Service | Phase 3 Status | Phase 4 Changes |
|---------|---------------|-----------------|
| **payment-service** | ✅ Complete (reserve/confirm/rollback implemented) | ✅ No changes needed |
| **order-service** | ⚠️ Missing orchestration | ✅ Add consumer + orchestration |

---

## 📁 Files Already Created (July 5th)

### ✅ order-service - Already Complete

These files were created on July 5th, 2026 and are ready to use:

1. **`consumer/PaymentEventConsumer.java`** ✅
   - @KafkaListener on "payment-events" topic
   - Delegates to OrderOrchestrationService
   - Error handling with exception re-throw

2. **`service/OrderOrchestrationService.java`** ✅
   - Injects existing OrderService (uses orderStore HashMap)
   - Simple decision logic: ACCEPT → CONFIRMED, REJECT → REJECTED
   - Idempotency tracking with Set<String>
   - Publishes FinalDecisionEvent to "order-events"

3. **`event/PaymentProcessedEvent.java`** ✅
   - DTO copied from payment-service
   - PaymentStatus enum (ACCEPT, REJECT)
   - Lombok @Builder and @Data

4. **`event/FinalDecisionEvent.java`** ✅
   - DTO for orchestrator decision
   - DecisionStatus enum (CONFIRMED, REJECTED)
   - Matches payment-service DTO structure

### ⚠️ Needs Verification

**`application.yml`** - May need updates for:
- Kafka consumer configuration (payment-events topic)
- Kafka producer type mapping (FinalDecisionEvent)
- KafkaListenerContainerFactory bean

---

## 📋 Implementation Plan

### Week 1: Verification & Testing (Days 1-2)

#### Step 1: Verify Existing Files (30 mins)

**Check all files exist:**

```bash
cd /Users/mhiteshkumar/spring-kafka-microservices/order-service

# Verify consumer
ls src/main/java/com/example/orderservice/consumer/PaymentEventConsumer.java

# Verify orchestration service
ls src/main/java/com/example/orderservice/service/OrderOrchestrationService.java

# Verify DTOs
ls src/main/java/com/example/orderservice/event/PaymentProcessedEvent.java
ls src/main/java/com/example/orderservice/event/FinalDecisionEvent.java
```

**Expected:** All 4 files should exist from July 5th work.

---

#### Step 2: Review application.yml Configuration (15 mins)

**File:** `order-service/src/main/resources/application.yml`

**Check consumer configuration exists:**

```yaml
spring:
  kafka:
    consumer:
      group-id: order-service-group
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "*"
        spring.json.type.mapping: paymentProcessed:com.example.orderservice.event.PaymentProcessedEvent
```

**Check producer configuration exists:**

```yaml
spring:
  kafka:
    producer:
      properties:
        spring.json.type.mapping: orderCreated:com.example.orderservice.event.OrderCreatedEvent,finalDecision:com.example.orderservice.event.FinalDecisionEvent
```

**If missing:** Add these configurations. See [Configuration Template](#configuration-template) below.

---

#### Step 3: Verify KafkaTemplate Bean (15 mins)

**Check if config/KafkaConfig.java exists:**

```bash
ls src/main/java/com/example/orderservice/config/KafkaConfig.java
```

**What's needed:**

A `KafkaTemplate<String, FinalDecisionEvent>` bean for publishing decisions.

**Option 1 (Simplest):** If order-service already has a generic KafkaTemplate, Spring will auto-configure it for FinalDecisionEvent.

**Option 2 (Explicit):** Create a dedicated bean:

```java
@Configuration
public class KafkaConfig {
    
    @Bean
    public KafkaTemplate<String, FinalDecisionEvent> decisionKafkaTemplate(
        ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
```

**Test:** Run `mvn clean compile` - if it compiles, the bean is properly configured.

---

#### Step 4: Compile and Test (30 mins)

**Compile both services:**

```bash
# payment-service
cd /Users/mhiteshkumar/spring-kafka-microservices/payment-service
mvn clean compile

# Expected: BUILD SUCCESS

# order-service
cd /Users/mhiteshkumar/spring-kafka-microservices/order-service
mvn clean compile

# Expected: BUILD SUCCESS
```

**If compilation fails:**
- Check that PaymentProcessedEvent.PaymentStatus enum is imported correctly
- Check that FinalDecisionEvent.DecisionStatus enum is imported correctly
- Verify Lombok annotations are processed (see Phase 3 fix for maven-compiler-plugin)

---

### Week 1: End-to-End Testing (Days 3-5)

#### Step 5: Start All Services (15 mins)

**Terminal 1: Start Kafka**

```bash
cd /Users/mhiteshkumar/spring-kafka-microservices
docker-compose up -d

# Verify running
docker-compose ps
# Expected: kafka, zookeeper, kafka-ui all "Up"
```

**Terminal 2: Start payment-service**

```bash
cd payment-service
mvn spring-boot:run

# Watch for:
# - "Started PaymentServiceApplication"
# - "Subscribed to topic(s): order-events"
# - "Customer initialization: 10 customers created"
```

**Terminal 3: Start order-service**

```bash
cd order-service
mvn spring-boot:run

# Watch for:
# - "Started OrderServiceApplication"
# - "Subscribed to topic(s): payment-events" ← NEW in Phase 4!
```

---

#### Step 6: Test Happy Path (30 mins)

**Scenario:** Order with sufficient balance → CONFIRMED

**1. Create order for CUST-1 (has $10,000 available)**

```bash
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-1",
    "items": [{
      "productId": "PROD-001",
      "productName": "Laptop",
      "quantity": 1,
      "price": 999.99
    }]
  }'
```

**Expected Response:**

```json
{
  "orderId": "550e8400-e29b-41d4-a716-446655440000",
  "customerId": "CUST-1",
  "items": [...],
  "totalAmount": 999.99,
  "status": "PENDING",
  "createdAt": "2026-07-06T10:30:00"
}
```

**2. Watch logs:**

**payment-service logs:**

```
[1] Received OrderCreatedEvent: orderId=550e8400-..., customerId=CUST-1, amount=$999.99
[2] Customer CUST-1 | Available: $10000.00 | Required: $999.99
[3] Payment ACCEPTED for order: 550e8400-... | Reserved: $999.99
[4] Published PaymentProcessedEvent: ACCEPT for order 550e8400-...
```

**order-service logs:**

```
[5] Received PaymentProcessedEvent: orderId=550e8400-..., status=ACCEPT
[6] Orchestrating decision for order: 550e8400-...
[7] Payment ACCEPTED → Order CONFIRMED: 550e8400-...
[8] Published FinalDecisionEvent: orderId=550e8400-..., status=CONFIRMED, offset=42
[9] Orchestration complete: orderId=550e8400-..., status=CONFIRMED
```

**payment-service logs (decision handling):**

```
[10] Received FinalDecisionEvent: orderId=550e8400-..., status=CONFIRMED, customerId=CUST-1
[11] Order CONFIRMED by orchestrator - committing reservation for order: 550e8400-...
[12] Confirming payment for order: 550e8400-... | Customer: CUST-1 | Reserved balance: $999.99
[13] Payment CONFIRMED for order: 550e8400-... | Deducted: $999.99
[14] Successfully processed final decision for order: 550e8400-...
```

**3. Verify database:**

```bash
# Open H2 Console
open http://localhost:8082/h2-console

# JDBC URL: jdbc:h2:mem:paymentdb
# Username: sa
# Password: (empty)

# Query:
SELECT * FROM customers WHERE customer_id = 'CUST-1';

# Expected:
# amount_available: 9000 (10000 - 1000)
# amount_reserved: 0 (was 1000, now confirmed and deducted)
```

**4. Verify Kafka events:**

```bash
open http://localhost:8080

# Topic: order-events
# Should see 2 messages for this order:
# - OrderCreatedEvent (status=NEW)
# - FinalDecisionEvent (status=CONFIRMED)

# Topic: payment-events
# Should see 1 message:
# - PaymentProcessedEvent (status=ACCEPT)
```

✅ **Success!** Happy path works end-to-end.

---

#### Step 7: Test Rejection Path (30 mins)

**Scenario:** Order with insufficient balance → REJECTED

**1. Create order for CUST-9 (has $500 available)**

```bash
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-9",
    "items": [{
      "productId": "PROD-002",
      "productName": "iPhone",
      "quantity": 1,
      "price": 1200.00
    }]
  }'
```

**2. Watch logs:**

**payment-service logs:**

```
[1] Received OrderCreatedEvent: orderId=660e8400-..., customerId=CUST-9, amount=$1200.00
[2] Customer CUST-9 | Available: $500.00 | Required: $1200.00
[3] Payment REJECTED for order: 660e8400-... | Insufficient balance
[4] Published PaymentProcessedEvent: REJECT for order 660e8400-... | Reason: Insufficient balance
```

**order-service logs:**

```
[5] Received PaymentProcessedEvent: orderId=660e8400-..., status=REJECT
[6] Orchestrating decision for order: 660e8400-...
[7] Payment REJECTED → Order REJECTED: 660e8400-... | Reason: Insufficient balance
[8] Published FinalDecisionEvent: orderId=660e8400-..., status=REJECTED, offset=43
[9] Orchestration complete: orderId=660e8400-..., status=REJECTED
```

**payment-service logs (decision handling):**

```
[10] Received FinalDecisionEvent: orderId=660e8400-..., status=REJECTED, customerId=CUST-9
[11] Order REJECTED by orchestrator - rolling back reservation for order: 660e8400-...
[12] Rolling back payment for order: 660e8400-... | Customer: CUST-9 | Reserved balance: $0.00
[13] No reserved amount to rollback for order: 660e8400-... | Possible data inconsistency
```

**Note:** Log [13] is expected - payment was never reserved (rejected immediately), so rollback has nothing to do.

**3. Verify database:**

```bash
# H2 Console query:
SELECT * FROM customers WHERE customer_id = 'CUST-9';

# Expected:
# amount_available: 500 (unchanged)
# amount_reserved: 0 (nothing was reserved)
```

✅ **Success!** Rejection path works correctly.

---

#### Step 8: Test Idempotency (30 mins)

**Scenario:** Simulate duplicate messages

**1. Restart payment-service mid-processing**

```bash
# Terminal 2: Stop payment-service (Ctrl+C)
# Wait 5 seconds
# Restart: mvn spring-boot:run
```

**2. Create order:**

```bash
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-2",
    "items": [{
      "productId": "PROD-003",
      "productName": "Keyboard",
      "quantity": 1,
      "price": 50.00
    }]
  }'
```

**3. Watch for duplicate detection:**

**order-service logs:**

```
[1] Orchestrating decision for order: 770e8400-...
[2] Order 770e8400-... already processed, skipping
```

**payment-service logs:**

```
[1] Handling CONFIRM decision for order: 770e8400-...
[2] Decision already processed (duplicate message): 770e8400-...
```

✅ **Success!** Idempotency prevents duplicate processing.

---

### Week 2: Additional Testing Scenarios

#### Step 9: Test Customer Not Found (15 mins)

```bash
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-999",
    "items": [{
      "productId": "PROD-004",
      "productName": "Mouse",
      "quantity": 1,
      "price": 25.00
    }]
  }'
```

**Expected:**
- payment-service: "Customer not found: CUST-999"
- payment-service: Published PaymentProcessedEvent: REJECT | Reason: Customer not found
- order-service: Order REJECTED
- No balance changes

---

#### Step 10: Test Multiple Concurrent Orders (30 mins)

**Create 5 orders in quick succession:**

```bash
for i in {1..5}; do
  curl -X POST http://localhost:8081/api/orders \
    -H "Content-Type: application/json" \
    -d "{
      \"customerId\": \"CUST-1\",
      \"items\": [{
        \"productId\": \"PROD-00$i\",
        \"productName\": \"Product $i\",
        \"quantity\": 1,
        \"price\": 100.00
      }]
    }" &
done
wait
```

**Verify:**
- All 5 orders processed
- CUST-1 balance: 10000 - (5 × 100) = 9500
- No race conditions
- All events in Kafka UI

---

## 🔍 Troubleshooting

### Issue 1: PaymentEventConsumer Not Receiving Messages

**Symptom:** order-service logs show no "Received PaymentProcessedEvent"

**Debug:**

```bash
# Check Kafka UI
open http://localhost:8080

# Topics → payment-events → Consumer Groups
# Look for: order-service-group
# Check lag: Should be 0 if messages are consumed
```

**Fix:**
- Verify topic name: Must be exactly "payment-events"
- Check consumer group-id in application.yml
- Restart order-service

---

### Issue 2: Deserialization Error

**Symptom:** `Cannot deserialize value of type PaymentProcessedEvent`

**Fix:**

Update `application.yml`:

```yaml
spring:
  kafka:
    consumer:
      properties:
        spring.json.trusted.packages: "*"
        spring.json.type.mapping: paymentProcessed:com.example.orderservice.event.PaymentProcessedEvent
```

**Note:** Type mapping must match exactly what payment-service publishes.

---

### Issue 3: KafkaTemplate Not Found

**Symptom:** `No qualifying bean of type 'KafkaTemplate<String, FinalDecisionEvent>'`

**Fix:**

Create explicit bean in KafkaConfig:

```java
@Bean
public KafkaTemplate<String, FinalDecisionEvent> decisionKafkaTemplate(
    ProducerFactory<String, Object> producerFactory) {
    return new KafkaTemplate<>(producerFactory);
}
```

---

### Issue 4: Order Not Found Exception

**Symptom:** `OrderNotFoundException` when PaymentEventConsumer tries to get order

**Root Cause:** Order was created in OrderService but somehow missing from orderStore

**Debug:**

```bash
# Add debug log in OrderService.createOrder():
log.debug("Stored order in orderStore: orderId={}, mapSize={}", 
    order.getOrderId(), orderStore.size());
```

**Fix:**
- Verify OrderService.createOrder() puts order in map BEFORE publishing event
- Check that orderId matches between event and map lookup

---

### Issue 5: Payment Confirmed Twice

**Symptom:** Customer balance goes negative

**Root Cause:** Idempotency not working - duplicate FinalDecisionEvent processed

**Fix:**
- Verify `processedDecisions` Set is used in both orchestrationService and paymentService
- Check that orderId is added to Set BEFORE processing
- Use ConcurrentHashMap.newKeySet() for thread safety

---

## 📝 Configuration Template

If `application.yml` is missing consumer/producer config, use this:

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    
    # Consumer configuration (NEW in Phase 4)
    consumer:
      group-id: order-service-group
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "*"
        spring.json.type.mapping: paymentProcessed:com.example.orderservice.event.PaymentProcessedEvent
    
    # Producer configuration (UPDATED in Phase 4)
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      acks: all
      retries: 3
      properties:
        enable.idempotence: true
        spring.json.type.mapping: orderCreated:com.example.orderservice.event.OrderCreatedEvent,finalDecision:com.example.orderservice.event.FinalDecisionEvent
```

---

## ✅ Completion Checklist

### Code Verification
- [ ] PaymentEventConsumer.java exists and compiles
- [ ] OrderOrchestrationService.java exists and compiles
- [ ] PaymentProcessedEvent.java exists (DTO copied from payment-service)
- [ ] FinalDecisionEvent.java exists (DTO matches payment-service)
- [ ] application.yml has consumer config
- [ ] application.yml has producer type mapping for FinalDecisionEvent
- [ ] KafkaTemplate<String, FinalDecisionEvent> bean exists
- [ ] `mvn clean compile` succeeds for both services

### Testing
- [ ] Happy path: Order → ACCEPT → CONFIRMED → Balance deducted
- [ ] Rejection path: Order → REJECT → REJECTED → Balance unchanged
- [ ] Customer balance in H2 database is correct
- [ ] All events visible in Kafka UI (order-events and payment-events)
- [ ] Idempotency works (duplicate messages ignored)
- [ ] Concurrent orders processed correctly
- [ ] Customer not found handled gracefully

### Documentation
- [ ] Logs show complete event flow
- [ ] No errors or warnings in logs (except expected validation failures)
- [ ] H2 console accessible at http://localhost:8082/h2-console
- [ ] Kafka UI accessible at http://localhost:8080

---

## 🎓 What You Learned

### Concepts
1. **SAGA Orchestration** - Coordinating distributed transactions via events
2. **Idempotency** - Preventing duplicate processing with Set<String>
3. **Two-Phase Commit Pattern** - Reserve → Confirm/Rollback
4. **Event-Driven Architecture** - Services communicate via Kafka events
5. **State Management** - Using existing HashMap instead of separate state store

### Patterns
1. **@KafkaListener** - Consuming events from Kafka
2. **KafkaTemplate** - Publishing events to Kafka
3. **Orchestration Service** - Central decision-making service
4. **Enum-Based Routing** - ACCEPT → CONFIRMED, REJECT → REJECTED
5. **Error Handling** - Exception re-throw prevents offset commit

### Skills
1. Configuring Kafka consumers and producers
2. JSON deserialization with type mapping
3. Testing distributed systems end-to-end
4. Debugging with Kafka UI and H2 console
5. Verifying idempotency and concurrent processing

---

## 🚀 Next Phase: Phase 5 (Stock Service + Kafka Streams)

After completing Phase 4, you'll have a working 2-service SAGA. Phase 5 will:

1. **Add stock-service** (third participant)
2. **Migrate to Kafka Streams** (replace @KafkaListener with stream joins)
3. **Implement 3-way decision logic:**
   - Both payment AND stock ACCEPT → CONFIRMED
   - Both REJECT → REJECTED
   - One ACCEPT, one REJECT → ROLLBACK (with source tracking)
4. **Use RocksDB state store** (replace ConcurrentHashMap)

**Pattern Evolution:**

| Phase | Services | Pattern | State |
|-------|----------|---------|-------|
| Phase 4 | order + payment | Simple @KafkaListener | HashMap |
| Phase 5 | order + payment + stock | Kafka Streams joins | RocksDB |

---

## 📚 Reference

- **Reference Repository:** https://github.com/piomin/sample-spring-kafka-microservices
- **Phase 3 Task:** `/tasks/03-build-payment-service-consumer.md`
- **Order Model:** `/order-service/src/main/java/com/example/orderservice/model/Order.java`
- **Customer Model:** `/payment-service/src/main/java/com/example/paymentservice/model/Customer.java`
- **Kafka UI:** http://localhost:8080
- **H2 Console:** http://localhost:8082/h2-console

---

**Status:** ✅ **READY TO TEST** (Code already exists from July 5th)  
**Next Action:** Follow Step 1 (Verify existing files) and Step 4 (Compile and test)  
**Estimated Time:** 1-2 days for complete testing and verification
