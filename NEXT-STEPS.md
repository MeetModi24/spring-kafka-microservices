# Next Steps - Quick Reference

> **Status:** Phase 2 Complete ✅ → Phase 3 Ready 🎯  
> **Date:** July 4, 2026

---

## 🎉 What We Just Completed

### Phase 2: Error Handling + Kafka Producer ✅

**Delivered:**
1. ✅ Custom exceptions (OrderNotFoundException, InvalidOrderException)
2. ✅ Global exception handler with @RestControllerAdvice
3. ✅ Kafka producer publishing OrderCreatedEvent
4. ✅ OrderEventProducer service with async callbacks
5. ✅ Comprehensive Kafka documentation (8 files, 4,939 lines)
6. ✅ Deep dive on producer internals added to docs
7. ✅ Task 03 created for Phase 3
8. ✅ Session notes and alignment documents created

**Files Created/Modified:**
- `order-service/src/main/java/com/example/orderservice/exception/` (3 files)
- `order-service/src/main/java/com/example/orderservice/event/OrderCreatedEvent.java`
- `order-service/src/main/java/com/example/orderservice/service/OrderEventProducer.java`
- `docs/03-kafka/` (8 documentation files)
- `tasks/03-build-payment-service-consumer.md`
- `notes/SESSION-NOTES.md`
- `ALIGNMENT-WITH-REFERENCE.md`

---

## 🚀 Your Next Task: Phase 3

### Task #8: Build payment-service (Kafka Consumer)

**Goal:** Create second microservice that consumes order events and validates payments

**Duration:** 2-3 weeks (Week 4-5)

**Detailed Instructions:** 
📄 `/tasks/03-build-payment-service-consumer.md`

---

## 📋 Quick Start Guide for Phase 3

### Step 1: Create Project Structure (30 mins)

```bash
cd /Users/mhiteshkumar/spring-kafka-microservices
mkdir payment-service
cd payment-service
mkdir -p src/main/java/com/example/paymentservice
mkdir -p src/main/resources
```

### Step 2: Create pom.xml (15 mins)

Copy from `/tasks/03-build-payment-service-consumer.md` → Step 1

**Key dependencies:**
- spring-kafka (Consumer)
- spring-boot-starter-data-jpa
- h2 database
- lombok
- **NO spring-boot-starter-web** (Kafka-only service)

### Step 3: Create application.yml (15 mins)

Copy from Task 03 → Step 2

**Key config:**
- Kafka consumer: group-id, deserializers
- Kafka producer: for response events
- JPA: ddl-auto=create-drop, show-sql=true
- H2: console enabled at /h2-console

### Step 4: Create Customer Entity (30 mins)

```java
@Entity
@Table(name = "customers")
public class Customer {
    @Id @GeneratedValue
    private Long id;
    private String customerId;
    private String name;
    private Integer amountAvailable;
    private Integer amountReserved;
    
    public boolean reserve(Integer amount) { ... }
    public void confirm(Integer amount) { ... }
    public void rollback(Integer amount) { ... }
}
```

### Step 5: Test Database (15 mins)

```bash
mvn spring-boot:run

# Access H2 console
open http://localhost:8082/h2-console
# JDBC URL: jdbc:h2:mem:paymentdb
# Username: sa
# Password: (empty)
```

---

## 🎯 Week 1 Goals (Database Setup)

- [ ] payment-service project created
- [ ] pom.xml with correct dependencies
- [ ] application.yml configured
- [ ] Customer entity created
- [ ] CustomerRepository interface created
- [ ] DataInitializer creates 10 test customers
- [ ] Can view customers in H2 console
- [ ] Application starts without errors

**Target:** Friday, July 11, 2026

---

## 🎯 Week 2 Goals (Kafka Integration)

- [ ] OrderCreatedEvent DTO created (match order-service)
- [ ] PaymentProcessedEvent DTO created
- [ ] PaymentService with payment validation logic
- [ ] OrderEventConsumer with @KafkaListener
- [ ] End-to-end test: create order → payment validates → event published
- [ ] Verify balance updates in H2
- [ ] Verify PaymentProcessedEvent in Kafka UI

**Target:** Friday, July 18, 2026

---

## 📖 Resources Available

### Documentation
1. **Task 03 (Detailed Instructions):**  
   `/tasks/03-build-payment-service-consumer.md`

2. **Kafka Fundamentals:**  
   `/docs/03-kafka/01-kafka-fundamentals.md`

3. **Kafka Consumers:**  
   `/docs/03-kafka/03-producers-consumers.md`

4. **Project Plan:**  
   `/docs/PROJECT-PLAN.md` (Phase 3 section)

5. **Session Notes:**  
   `/notes/SESSION-NOTES.md`

6. **Alignment with Reference:**  
   `/ALIGNMENT-WITH-REFERENCE.md`

### Reference Repository
- **GitHub:** https://github.com/piomin/sample-spring-kafka-microservices
- **Focus on:** `/payment-service` directory

---

## 🧪 Testing Strategy

### Test 1: Database Setup
```bash
# Start payment-service
mvn spring-boot:run

# Check logs for:
# "Created 10 test customers"
# "Customer CUST-1 | Balance: $2500.00"

# Verify in H2 console
SELECT * FROM customers;
```

### Test 2: Kafka Consumer
```bash
# Terminal 1: order-service
cd order-service && mvn spring-boot:run

# Terminal 2: payment-service  
cd payment-service && mvn spring-boot:run

# Terminal 3: Create order
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-1",
    "items": [{"productId": "P1", "productName": "Laptop", "quantity": 1, "price": 999.99}]
  }'

# Check payment-service logs:
# "Received OrderCreatedEvent: orderId=..."
# "Payment ACCEPTED for order: ..."
```

### Test 3: Balance Update
```bash
# H2 console: Check customer balance BEFORE and AFTER
SELECT * FROM customers WHERE customer_id = 'CUST-1';

# Expected:
# BEFORE: amountAvailable = 2500, amountReserved = 0
# AFTER:  amountAvailable = 1500, amountReserved = 99999 (999.99 in cents)
```

### Test 4: Kafka Event
```bash
# Kafka UI: http://localhost:8080
# Navigate to: Topics → payment-events → Messages

# Expected PaymentProcessedEvent:
{
  "orderId": "...",
  "customerId": "CUST-1",
  "amount": 999.99,
  "status": "ACCEPT",
  "processedAt": "2026-07-..."
}
```

---

## ⚠️ Common Issues & Solutions

### Issue 1: "ClassNotFoundException: OrderCreatedEvent"
**Cause:** DTO structure doesn't match between services  
**Solution:** Ensure OrderCreatedEvent in payment-service matches order-service exactly

### Issue 2: "Could not extract ResultSet"
**Cause:** JPA entity mapping issue  
**Solution:** Check @Entity, @Table, @Column annotations on Customer class

### Issue 3: Consumer not receiving messages
**Cause:** Wrong topic name or consumer group misconfigured  
**Solution:** 
- Check topic name: "order-events" (not "orders")
- Check consumer group-id in application.yml
- Verify Kafka is running: `docker-compose ps`

### Issue 4: "Cannot deserialize value"
**Cause:** JsonDeserializer type mapping missing  
**Solution:** Add to application.yml:
```yaml
spring.json.type.mapping: orderCreated:com.example.paymentservice.event.OrderCreatedEvent
```

---

## 📊 Success Criteria for Phase 3

### Must Have ✅
- [ ] payment-service compiles and runs
- [ ] Consumes OrderCreatedEvent from Kafka
- [ ] Validates balance against H2 database
- [ ] Reserves funds (updates Customer entity)
- [ ] Publishes PaymentProcessedEvent (ACCEPT/REJECT)
- [ ] All logs show successful processing

### Nice to Have 🎯
- [ ] Handle unknown customer gracefully
- [ ] Handle insufficient balance correctly
- [ ] Comprehensive logging at each step
- [ ] Test script for automated testing

### Out of Scope (Phase 4) 🔮
- Confirm/rollback logic (SAGA completion)
- Idempotency checks
- Dead Letter Queue (DLQ)
- Error retry logic

---

## 🗺️ Phase Progression

```
Phase 1 ✅ → Phase 2 ✅ → Phase 3 🎯 → Phase 4 → Phase 5 → Phase 6

Current Progress: 40% complete
Next Milestone: 60% (after Phase 3)
```

---

## 🤝 Getting Help

**Stuck on something?**

1. **Check Task 03:** Detailed step-by-step instructions
2. **Check Session Notes:** `/notes/SESSION-NOTES.md` for context
3. **Check Reference Repo:** See how they implemented it
4. **Check Kafka Docs:** `/docs/03-kafka/` for concepts

**Common questions already answered:**
- "How does @KafkaListener work?" → `/docs/03-kafka/03-producers-consumers.md`
- "What is SAGA pattern?" → `/docs/PROJECT-PLAN.md`
- "How to debug Kafka issues?" → `/docs/03-kafka/05-error-handling.md`

---

## ✅ Pre-Phase 3 Checklist

Before starting, verify:
- [x] Phase 2 complete (order-service with Kafka producer)
- [x] Kafka running (`docker-compose ps`)
- [x] Can create orders successfully
- [x] Events visible in Kafka UI (order-events topic)
- [x] Task 03 document read and understood
- [ ] Ready to code!

---

## 🎯 Your Action Items for Next Session

1. **Review Task 03:** Read `/tasks/03-build-payment-service-consumer.md` (15 mins)
2. **Create project structure:** payment-service directory + basic files (30 mins)
3. **Set up pom.xml:** Add dependencies (15 mins)
4. **Configure application.yml:** Kafka + JPA + H2 (20 mins)
5. **Create Customer entity:** Domain model with balance logic (30 mins)
6. **Test database:** Verify H2 console works (15 mins)

**Total Time:** ~2 hours for Week 1 Day 1

---

**Good luck with Phase 3! You've got comprehensive documentation to guide you every step of the way.** 🚀

---

**Quick Commands:**

```bash
# Start Kafka
docker-compose up -d

# Start order-service
cd order-service && mvn spring-boot:run

# (Future) Start payment-service
cd payment-service && mvn spring-boot:run

# Check Kafka UI
open http://localhost:8080

# Check H2 console
open http://localhost:8082/h2-console
```
