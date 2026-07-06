# 🚀 Start Here - Spring Kafka Microservices

> **Last Updated:** July 6, 2026  
> **Current Phase:** ✅ Phase 4 Complete (Testing)  
> **Status:** Ready to test SAGA orchestration

---

## 📊 Project Status

```
Phase 1 ████████████████████ 100% ✅ order-service REST API
Phase 2 ████████████████████ 100% ✅ Kafka Producer + Error Handling
Phase 3 ████████████████████ 100% ✅ payment-service Consumer + SAGA
Phase 4 ████████████████████ 100% ✅ SAGA Orchestration (Code Complete)
Phase 5 ░░░░░░░░░░░░░░░░░░░░   0% ⏳ stock-service + Kafka Streams
Phase 6 ░░░░░░░░░░░░░░░░░░░░   0% ⏳ Advanced Features

Overall: ████████████░░░░░░░░ 80% Complete
```

---

## 🎯 What This Project Does

Learn **event-driven microservices** with Kafka by building a distributed order processing system with SAGA orchestration pattern.

**Current Architecture (Phase 4):**
- **order-service**: REST API + Kafka producer + SAGA orchestrator
- **payment-service**: Kafka consumer + payment validation + two-phase commit
- **Kafka**: Event streaming platform (order-events, payment-events topics)
- **Pattern**: Distributed transaction coordination without 2PC

---

## 🏃 Quick Start

### Prerequisites
- Java 17 or higher
- Docker Desktop (for Kafka)
- Maven 3.8+

### 1. Start Kafka

```bash
docker-compose up -d

# Verify running
docker ps
# Expected: kafka, zookeeper, kafka-ui all "Up"
```

### 2. Start Services

**Terminal 1: payment-service**
```bash
cd payment-service
mvn spring-boot:run

# Wait for: "Started PaymentServiceApplication"
```

**Terminal 2: order-service**
```bash
cd order-service
mvn spring-boot:run

# Wait for: "Started OrderServiceApplication"
```

### 3. Create Test Order

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

### 4. Verify

- **Kafka UI**: http://localhost:8080 (see events)
- **H2 Console**: http://localhost:8082/h2-console (check balances)
  - JDBC URL: `jdbc:h2:mem:paymentdb`
  - Username: `sa`, Password: (empty)

**Expected Flow:**
1. Order created (status: PENDING)
2. Payment reserved ($999.99 from CUST-1)
3. Order confirmed (status: CONFIRMED)
4. Payment deducted (balance updated)

---

## 📁 Project Structure

```
spring-kafka-microservices/
├── README.md                   # Project overview
├── START-HERE.md              # ← You are here
├── docker-compose.yml         # Kafka infrastructure
│
├── docs/                      # Learning materials
│   ├── PROJECT-PLAN.md       # Full roadmap (6 phases)
│   ├── ARCHITECTURE-OVERVIEW.md
│   ├── LEARNING-GUIDE.md
│   └── 03-kafka/             # Kafka fundamentals (8 guides)
│
├── tasks/                     # Step-by-step guides
│   ├── 01-implement-order-service.md
│   ├── 02-add-error-handling-kafka-producer.md
│   ├── 03-build-payment-service-consumer.md
│   └── 04-implement-saga-orchestration-simple.md
│
├── order-service/            # REST API + Kafka producer + Orchestrator
│   ├── src/main/java/com/example/orderservice/
│   │   ├── controller/       # OrderController (REST endpoints)
│   │   ├── service/          # OrderService, OrderOrchestrationService
│   │   ├── consumer/         # PaymentEventConsumer
│   │   ├── event/            # Kafka event DTOs
│   │   └── model/            # Order, OrderItem domain models
│   └── pom.xml
│
└── payment-service/          # Kafka consumer + Payment validation
    ├── src/main/java/com/example/paymentservice/
    │   ├── consumer/         # OrderEventConsumer, DecisionEventConsumer
    │   ├── service/          # PaymentService (reserve/confirm/rollback)
    │   ├── model/            # Customer entity
    │   ├── repository/       # CustomerRepository (JPA)
    │   └── event/            # Kafka event DTOs
    └── pom.xml
```

---

## 📚 Documentation Guide

### Getting Started
1. **Start Here** ← You are here
2. **[README.md](README.md)** - Project overview
3. **[docs/PROJECT-PLAN.md](docs/PROJECT-PLAN.md)** - Full 6-phase roadmap

### Phase-by-Phase Learning
- **Phase 1**: [tasks/01-implement-order-service.md](tasks/01-implement-order-service.md) - REST API basics
- **Phase 2**: [tasks/02-add-error-handling-kafka-producer.md](tasks/02-add-error-handling-kafka-producer.md) - Kafka producer
- **Phase 3**: [tasks/03-build-payment-service-consumer.md](tasks/03-build-payment-service-consumer.md) - Kafka consumer
- **Phase 4**: [tasks/04-implement-saga-orchestration-simple.md](tasks/04-implement-saga-orchestration-simple.md) - SAGA orchestration

### Deep Dives
- **Kafka Fundamentals**: [docs/03-kafka/](docs/03-kafka/) - 8 comprehensive guides
- **Architecture**: [docs/ARCHITECTURE-OVERVIEW.md](docs/ARCHITECTURE-OVERVIEW.md)
- **Learning Path**: [docs/LEARNING-GUIDE.md](docs/LEARNING-GUIDE.md)

---

## 🔥 Current Phase: Phase 4 Testing

### What's Complete ✅
- ✅ order-service orchestration (PaymentEventConsumer, OrderOrchestrationService)
- ✅ payment-service SAGA handlers (DecisionEventConsumer, confirm/rollback)
- ✅ All DTOs and events (PaymentProcessedEvent, FinalDecisionEvent)
- ✅ Kafka configuration (2 topics: order-events, payment-events)
- ✅ Compilation successful

### What to Test 🧪
Follow **[tasks/04-implement-saga-orchestration-simple.md](tasks/04-implement-saga-orchestration-simple.md)** for:
1. Happy path: Order → Payment ACCEPT → CONFIRMED
2. Rejection path: Order → Payment REJECT → REJECTED
3. Idempotency: Duplicate messages handled correctly
4. Database verification: Customer balances updated
5. Kafka events: All events visible in Kafka UI

**Testing Time:** 2-3 hours

---

## 🎓 What You'll Learn

### Completed (Phases 1-4)
- ✅ Spring Boot REST APIs
- ✅ Kafka producers and consumers
- ✅ Event-driven architecture
- ✅ SAGA orchestration pattern
- ✅ Two-phase commit (reserve/confirm/rollback)
- ✅ Idempotency in distributed systems
- ✅ JSON serialization with type mapping

### Coming Next (Phase 5-6)
- ⏳ Kafka Streams (stateful processing)
- ⏳ Stream joins (multi-service coordination)
- ⏳ RocksDB state stores
- ⏳ Three-way SAGA decision logic
- ⏳ Rollback with source tracking

---

## 🛠️ Useful Commands

### Maven
```bash
# Compile
mvn clean compile

# Run service
mvn spring-boot:run

# Run tests
mvn test

# Package JAR
mvn clean package
```

### Docker
```bash
# Start Kafka
docker-compose up -d

# Stop Kafka
docker-compose down

# View logs
docker-compose logs -f kafka

# Remove volumes (clean slate)
docker-compose down -v
```

### Testing
```bash
# Create order
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d @test-order.json

# Get all orders
curl http://localhost:8081/api/orders

# Get specific order
curl http://localhost:8081/api/orders/{orderId}
```

---

## 🐛 Troubleshooting

### Services Won't Start
1. Check Kafka is running: `docker ps`
2. Check port availability: `lsof -i :8081` (order-service), `lsof -i :8082` (H2 console)
3. Check logs: Look for "Started *Application" message

### Orders Not Processing
1. Open Kafka UI: http://localhost:8080
2. Check topics exist: `order-events`, `payment-events`
3. Check consumer groups: `order-service-group`, `payment-service-group`, `payment-decision-group`
4. Check logs for errors in both services

### Database Issues
1. Open H2 Console: http://localhost:8082/h2-console
2. JDBC URL: `jdbc:h2:mem:paymentdb`
3. Run: `SELECT * FROM customers;`
4. Verify 10 test customers exist (CUST-1 to CUST-10)

**More troubleshooting**: See [tasks/04-implement-saga-orchestration-simple.md](tasks/04-implement-saga-orchestration-simple.md) - Troubleshooting section

---

## 📖 Key Concepts

### SAGA Pattern
Distributed transaction pattern that coordinates multiple microservices without distributed locks:
- **Reserve**: Tentatively lock resources
- **Confirm**: Commit reserved resources
- **Rollback**: Compensate by releasing reserved resources

### Two-Phase Accounting
Customer balance tracking with separate pools:
- **amountAvailable**: Funds available for new orders
- **amountReserved**: Funds locked for pending orders
- **Flow**: available → reserved → deducted (confirm) OR available (rollback)

### Event-Driven Architecture
Services communicate via Kafka events:
- **OrderCreatedEvent**: order-service → payment-service
- **PaymentProcessedEvent**: payment-service → order-service
- **FinalDecisionEvent**: order-service → payment-service

### Idempotency
Preventing duplicate processing:
- Track processed message IDs in `Set<String>`
- Skip duplicate messages
- Critical for at-least-once delivery

---

## 🚀 Next Steps

### For Learning
1. **Test Phase 4**: Follow the testing guide in Task 04
2. **Explore Kafka UI**: See events flow in real-time
3. **Experiment**: Try different scenarios (insufficient balance, unknown customer)
4. **Read Code**: Understand OrderOrchestrationService logic

### For Development
1. **Phase 5**: Add stock-service (third participant)
2. **Migrate to Kafka Streams**: Replace @KafkaListener with stream joins
3. **Implement Rollback**: Handle partial success scenarios
4. **Add Monitoring**: Prometheus metrics, distributed tracing

---

## 📚 Reference

- **Reference Repository**: https://github.com/piomin/sample-spring-kafka-microservices
- **Apache Kafka Docs**: https://kafka.apache.org/documentation/
- **Spring Kafka Docs**: https://docs.spring.io/spring-kafka/reference/
- **SAGA Pattern**: https://microservices.io/patterns/data/saga.html

---

## 💬 Need Help?

1. **Check the task guide**: Each phase has detailed step-by-step instructions
2. **Read troubleshooting section**: Common issues documented
3. **Check logs**: Services print detailed debug logs
4. **Verify with tools**: Kafka UI, H2 Console

---

**Ready to test?** → Open [tasks/04-implement-saga-orchestration-simple.md](tasks/04-implement-saga-orchestration-simple.md) and start at Step 4!

**Want to understand architecture?** → Read [docs/ARCHITECTURE-OVERVIEW.md](docs/ARCHITECTURE-OVERVIEW.md)

**Need Kafka fundamentals?** → Check [docs/03-kafka/](docs/03-kafka/)
