# Task 03: Build Payment Service (Kafka Consumer)

> **Current Status:** 🎯 READY TO START  
> **Phase:** 3 of 6  
> **Duration:** 2-3 weeks  
> **Prerequisites:** ✅ Phase 2 complete (order-service with Kafka producer)

---

## 🎯 Phase 3 Goals

Build the **payment-service** microservice that:
1. Consumes `OrderCreatedEvent` from Kafka `order-events` topic
2. Validates payment by checking customer balance (H2 database)
3. Reserves funds if balance is sufficient
4. Publishes response event: `PaymentProcessedEvent` with status `ACCEPT` or `REJECT`
5. Implements the **SAGA participant pattern**

This aligns with the reference architecture: [piomin/sample-spring-kafka-microservices](https://github.com/piomin/sample-spring-kafka-microservices)

---

## 📋 Implementation Plan

### Week 1: Service Scaffolding & Database Setup

#### Step 1: Create Payment Service Project Structure (1 hour)

**Create new Spring Boot module:**

```bash
cd /Users/mhiteshkumar/spring-kafka-microservices
mkdir payment-service
cd payment-service
```

**Create `pom.xml`:**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>3.5.16</version>
        <relativePath/>
    </parent>

    <groupId>com.example</groupId>
    <artifactId>payment-service</artifactId>
    <version>1.0.0</version>
    <name>payment-service</name>
    <description>Payment validation microservice with Kafka consumer</description>

    <properties>
        <java.version>21</java.version>
    </properties>

    <dependencies>
        <!-- Spring Kafka (Consumer) -->
        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka</artifactId>
        </dependency>

        <!-- Spring Data JPA -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-data-jpa</artifactId>
        </dependency>

        <!-- H2 Database (in-memory) -->
        <dependency>
            <groupId>com.h2database</groupId>
            <artifactId>h2</artifactId>
            <scope>runtime</scope>
        </dependency>

        <!-- Lombok -->
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <optional>true</optional>
        </dependency>

        <!-- NO spring-boot-starter-web - This is a Kafka-only service -->
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>
```

**Key difference from order-service:** NO `spring-boot-starter-web` dependency - this service doesn't expose REST APIs, only consumes Kafka events.

---

#### Step 2: Create Application Configuration (30 mins)

**Create:** `payment-service/src/main/resources/application.yml`

```yaml
spring:
  application:
    name: payment-service

  # Kafka Consumer Configuration
  kafka:
    bootstrap-servers: localhost:9092
    
    consumer:
      # Consumer group ID - multiple instances share load
      group-id: payment-service-group
      
      # Start from earliest offset if no commit exists
      auto-offset-reset: earliest
      
      # Deserializers
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      
      # JsonDeserializer properties
      properties:
        spring.json.trusted.packages: "*"
        spring.json.type.mapping: orderCreated:com.example.paymentservice.event.OrderCreatedEvent
    
    # Producer Configuration (for response events)
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      acks: all
      retries: 3
      properties:
        enable.idempotence: true
        spring.json.type.mapping: paymentProcessed:com.example.paymentservice.event.PaymentProcessedEvent

  # JPA / Hibernate Configuration
  jpa:
    hibernate:
      ddl-auto: create-drop  # Recreate schema on startup (dev only)
    show-sql: true  # Log SQL statements
    properties:
      hibernate:
        format_sql: true

  # H2 Database Configuration
  datasource:
    url: jdbc:h2:mem:paymentdb
    driver-class-name: org.h2.Driver
    username: sa
    password:
  
  h2:
    console:
      enabled: true
      path: /h2-console  # Access at http://localhost:8082/h2-console

# Server Configuration (optional - only if you want actuator endpoints)
server:
  port: 8082  # Different from order-service (8081)

# Logging
logging:
  level:
    com.example.paymentservice: DEBUG
    org.springframework.kafka: INFO
    org.hibernate.SQL: DEBUG
    org.hibernate.type.descriptor.sql.BasicBinder: TRACE

# Actuator (optional - for health checks)
management:
  endpoints:
    web:
      exposure:
        include: health,info
```

**Key Configuration Points:**
- **Consumer group:** Enables load balancing across multiple payment-service instances
- **auto-offset-reset: earliest:** Processes all messages from topic start (useful for testing)
- **trusted.packages: "*":** Allows deserializing any package (dev only; restrict in production)
- **ddl-auto: create-drop:** Recreates DB schema on every restart (dev convenience)

---

#### Step 3: Create Domain Model (Customer Entity) (30 mins)

**Create:** `payment-service/src/main/java/com/example/paymentservice/model/Customer.java`

```java
package com.example.paymentservice.model;

import jakarta.persistence.*;
import lombok.*;

/**
 * Customer entity representing a customer account with balance management
 * 
 * BUSINESS LOGIC:
 * - amountAvailable: Current spendable balance
 * - amountReserved: Funds on hold (pending orders)
 * 
 * SAGA PATTERN:
 * - reserve(): Moves funds from available → reserved (tentative)
 * - confirm(): Deducts from reserved (commit)
 * - rollback(): Returns from reserved → available (compensate)
 */
@Entity
@Table(name = "customers")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class Customer {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(nullable = false)
    private String customerId;  // Business ID (e.g., "CUST-123")
    
    @Column(nullable = false)
    private String name;
    
    /**
     * Available balance (can be spent immediately)
     */
    @Column(nullable = false)
    private Integer amountAvailable = 0;
    
    /**
     * Reserved balance (pending order confirmation)
     */
    @Column(nullable = false)
    private Integer amountReserved = 0;
    
    /**
     * Reserve funds for an order (SAGA Reserve phase)
     * 
     * @param amount Amount to reserve
     * @return true if successful, false if insufficient balance
     */
    public boolean reserve(Integer amount) {
        if (amountAvailable >= amount) {
            amountAvailable -= amount;
            amountReserved += amount;
            return true;
        }
        return false;
    }
    
    /**
     * Confirm reservation - deduct from reserved (SAGA Confirm phase)
     * 
     * @param amount Amount to confirm
     */
    public void confirm(Integer amount) {
        if (amountReserved >= amount) {
            amountReserved -= amount;
        }
    }
    
    /**
     * Rollback reservation - return to available (SAGA Compensate phase)
     * 
     * @param amount Amount to rollback
     */
    public void rollback(Integer amount) {
        amountReserved -= amount;
        amountAvailable += amount;
    }
}
```

**Design Notes:**
- **Two-phase accounting:** `reserve()` → `confirm()` implements SAGA pattern
- **Compensating transaction:** `rollback()` undoes reserve if order fails
- **Business methods:** Encapsulate balance logic in domain model (not service layer)

---

#### Step 4: Create Repository Layer (15 mins)

**Create:** `payment-service/src/main/java/com/example/paymentservice/repository/CustomerRepository.java`

```java
package com.example.paymentservice.repository;

import com.example.paymentservice.model.Customer;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * JPA Repository for Customer entity
 * 
 * DESIGN PATTERN: Repository Pattern
 * - Abstracts data access logic
 * - Provides CRUD operations out-of-the-box
 * - Supports custom query methods
 */
@Repository
public interface CustomerRepository extends JpaRepository<Customer, Long> {
    
    /**
     * Find customer by business ID (e.g., "CUST-123")
     * 
     * Spring Data JPA auto-generates implementation from method name:
     * - "findBy" → SELECT query
     * - "CustomerId" → WHERE customer_id = ?
     * 
     * @param customerId Business customer ID
     * @return Optional<Customer> (empty if not found)
     */
    Optional<Customer> findByCustomerId(String customerId);
}
```

**Spring Data Magic:** Method name `findByCustomerId` is automatically implemented as:
```sql
SELECT * FROM customers WHERE customer_id = ?
```

---

#### Step 5: Initialize Test Data (30 mins)

**Create:** `payment-service/src/main/java/com/example/paymentservice/config/DataInitializer.java`

```java
package com.example.paymentservice.config;

import com.example.paymentservice.model.Customer;
import com.example.paymentservice.repository.CustomerRepository;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Initializes database with test customers on application startup
 * 
 * @PostConstruct runs after all beans are initialized
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class DataInitializer {
    
    private final CustomerRepository customerRepository;
    private final Random random = new Random();
    
    @PostConstruct
    public void init() {
        log.info("Initializing test customers...");
        
        List<Customer> customers = new ArrayList<>();
        
        // Create 10 test customers
        for (int i = 1; i <= 10; i++) {
            Customer customer = new Customer();
            customer.setCustomerId("CUST-" + i);
            customer.setName("Customer " + i);
            
            // Random balance between 1000 and 5000
            customer.setAmountAvailable(1000 + random.nextInt(4000));
            customer.setAmountReserved(0);
            
            customers.add(customer);
        }
        
        customerRepository.saveAll(customers);
        
        log.info("Created {} test customers", customers.size());
        customers.forEach(c -> log.info("  - {} | Balance: ${}", 
            c.getCustomerId(), c.getAmountAvailable()));
    }
}
```

**Testing:** Start payment-service and verify customers exist:
```bash
# Start service
mvn spring-boot:run

# Access H2 console
open http://localhost:8082/h2-console

# JDBC URL: jdbc:h2:mem:paymentdb
# Username: sa
# Password: (leave empty)

# Query:
SELECT * FROM customers;
```

---

### Week 2: Kafka Consumer & Business Logic

#### Step 6: Create Event DTOs (45 mins)

**Create:** `payment-service/src/main/java/com/example/paymentservice/event/OrderCreatedEvent.java`

```java
package com.example.paymentservice.event;

import lombok.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Event received from order-service via Kafka
 * 
 * IMPORTANT: Must match order-service's OrderCreatedEvent structure exactly
 * for JSON deserialization to work
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderCreatedEvent {
    
    private String orderId;
    private String customerId;
    private List<OrderItemEvent> items;
    private BigDecimal totalAmount;
    private LocalDateTime createdAt;
    
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OrderItemEvent {
        private String productId;
        private String productName;
        private int quantity;
        private BigDecimal price;
    }
}
```

**Create:** `payment-service/src/main/java/com/example/paymentservice/event/PaymentProcessedEvent.java`

```java
package com.example.paymentservice.event;

import lombok.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Response event published after payment validation
 * 
 * Consumed by order-service to determine final order status
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PaymentProcessedEvent {
    
    private String orderId;
    private String customerId;
    private BigDecimal amount;
    
    /**
     * Payment status: ACCEPT or REJECT
     */
    private PaymentStatus status;
    
    /**
     * Reason for rejection (if status=REJECT)
     */
    private String reason;
    
    private LocalDateTime processedAt;
    
    public enum PaymentStatus {
        ACCEPT,   // Sufficient balance, funds reserved
        REJECT    // Insufficient balance
    }
}
```

---

#### Step 7: Implement Business Logic Service (1 hour)

**Create:** `payment-service/src/main/java/com/example/paymentservice/service/PaymentService.java`

```java
package com.example.paymentservice.service;

import com.example.paymentservice.event.OrderCreatedEvent;
import com.example.paymentservice.event.PaymentProcessedEvent;
import com.example.paymentservice.model.Customer;
import com.example.paymentservice.repository.CustomerRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Optional;

/**
 * Payment validation business logic
 * 
 * SAGA PATTERN: Participant Service
 * - Receives order event
 * - Validates payment (reserve funds)
 * - Publishes response (ACCEPT/REJECT)
 * - Later receives final decision (confirm/rollback)
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentService {
    
    private static final String PAYMENT_EVENTS_TOPIC = "payment-events";
    
    private final CustomerRepository customerRepository;
    private final KafkaTemplate<String, PaymentProcessedEvent> kafkaTemplate;
    
    /**
     * Process order payment validation (SAGA Reserve phase)
     * 
     * @param event OrderCreatedEvent from order-service
     */
    @Transactional
    public void processOrderPayment(OrderCreatedEvent event) {
        log.info("Processing payment for order: {}", event.getOrderId());
        
        // Find customer
        Optional<Customer> customerOpt = customerRepository.findByCustomerId(event.getCustomerId());
        
        if (customerOpt.isEmpty()) {
            log.warn("Customer not found: {}", event.getCustomerId());
            publishRejection(event, "Customer not found");
            return;
        }
        
        Customer customer = customerOpt.get();
        Integer amountCents = event.getTotalAmount().multiply(new java.math.BigDecimal("100")).intValue();
        
        log.info("Customer {} | Available: ${} | Required: ${}", 
            customer.getCustomerId(), 
            customer.getAmountAvailable() / 100.0,
            amountCents / 100.0);
        
        // Attempt to reserve funds
        boolean reserved = customer.reserve(amountCents);
        
        if (reserved) {
            // Save updated balance
            customerRepository.save(customer);
            
            log.info("Payment ACCEPTED for order: {} | Reserved: ${}", 
                event.getOrderId(), amountCents / 100.0);
            
            publishAcceptance(event, amountCents);
        } else {
            log.warn("Payment REJECTED for order: {} | Insufficient balance", 
                event.getOrderId());
            
            publishRejection(event, "Insufficient balance");
        }
    }
    
    /**
     * Publish payment acceptance event
     */
    private void publishAcceptance(OrderCreatedEvent event, Integer amountCents) {
        PaymentProcessedEvent paymentEvent = PaymentProcessedEvent.builder()
            .orderId(event.getOrderId())
            .customerId(event.getCustomerId())
            .amount(event.getTotalAmount())
            .status(PaymentProcessedEvent.PaymentStatus.ACCEPT)
            .processedAt(LocalDateTime.now())
            .build();
        
        kafkaTemplate.send(PAYMENT_EVENTS_TOPIC, event.getOrderId(), paymentEvent);
        log.info("Published PaymentProcessedEvent: ACCEPT for order {}", event.getOrderId());
    }
    
    /**
     * Publish payment rejection event
     */
    private void publishRejection(OrderCreatedEvent event, String reason) {
        PaymentProcessedEvent paymentEvent = PaymentProcessedEvent.builder()
            .orderId(event.getOrderId())
            .customerId(event.getCustomerId())
            .amount(event.getTotalAmount())
            .status(PaymentProcessedEvent.PaymentStatus.REJECT)
            .reason(reason)
            .processedAt(LocalDateTime.now())
            .build();
        
        kafkaTemplate.send(PAYMENT_EVENTS_TOPIC, event.getOrderId(), paymentEvent);
        log.info("Published PaymentProcessedEvent: REJECT for order {} | Reason: {}", 
            event.getOrderId(), reason);
    }
}
```

**Key Design Points:**
- `@Transactional`: Ensures database changes rollback on error
- **Reserve pattern:** Funds moved to `amountReserved` (not deducted yet)
- **Response event:** Published immediately after validation
- **Currency handling:** Stores cents (Integer) to avoid floating-point issues

---

#### Step 8: Implement Kafka Consumer (30 mins)

**Create:** `payment-service/src/main/java/com/example/paymentservice/consumer/OrderEventConsumer.java`

```java
package com.example.paymentservice.consumer;

import com.example.paymentservice.event.OrderCreatedEvent;
import com.example.paymentservice.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer that listens to order-events topic
 * 
 * @KafkaListener automatically:
 * - Connects to Kafka broker
 * - Subscribes to topic
 * - Polls for messages
 * - Deserializes JSON → OrderCreatedEvent
 * - Calls this method for each message
 * - Commits offset after successful processing
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class OrderEventConsumer {
    
    private final PaymentService paymentService;
    
    /**
     * Consume OrderCreatedEvent from order-service
     * 
     * @param event Deserialized order event
     */
    @KafkaListener(
        topics = "order-events",
        groupId = "payment-service-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeOrderEvent(OrderCreatedEvent event) {
        log.info("Received OrderCreatedEvent: orderId={}, customerId={}, amount=${}", 
            event.getOrderId(), 
            event.getCustomerId(), 
            event.getTotalAmount());
        
        try {
            // Process payment validation
            paymentService.processOrderPayment(event);
            
            log.info("Successfully processed order: {}", event.getOrderId());
            
        } catch (Exception e) {
            log.error("Failed to process order: {}", event.getOrderId(), e);
            
            // In production:
            // - Retry with exponential backoff
            // - Send to DLQ after max retries
            // - Alert operations team
            throw e;  // Causes Kafka to NOT commit offset (message will be retried)
        }
    }
}
```

**How @KafkaListener Works:**
1. Spring scans for `@KafkaListener` methods on startup
2. Creates Kafka consumer with config from `application.yml`
3. Subscribes to `order-events` topic
4. Polls Kafka broker continuously
5. Deserializes message → `OrderCreatedEvent` object
6. Calls `consumeOrderEvent()` method
7. Commits offset if method succeeds
8. Retries if method throws exception (offset NOT committed)

---

#### Step 9: Create Main Application Class (15 mins)

**Create:** `payment-service/src/main/java/com/example/paymentservice/PaymentServiceApplication.java`

```java
package com.example.paymentservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Payment Service - Kafka Consumer Microservice
 * 
 * NO @EnableWebMvc or REST controllers
 * This is a pure Kafka consumer service with JPA persistence
 * 
 * On startup:
 * 1. Connects to H2 database (jdbc:h2:mem:paymentdb)
 * 2. Creates schema (customers table)
 * 3. Initializes 10 test customers (@PostConstruct in DataInitializer)
 * 4. Connects to Kafka broker (localhost:9092)
 * 5. Subscribes to "order-events" topic
 * 6. Waits for messages...
 */
@SpringBootApplication
public class PaymentServiceApplication {
    
    public static void main(String[] args) {
        SpringApplication.run(PaymentServiceApplication.class, args);
    }
}
```

---

## 🧪 Testing Steps

### Test 1: Start Services

```bash
# Terminal 1: Start Kafka (if not running)
cd /Users/mhiteshkumar/spring-kafka-microservices
docker-compose up -d

# Terminal 2: Start order-service
cd order-service
mvn spring-boot:run

# Terminal 3: Start payment-service
cd payment-service
mvn spring-boot:run

# Wait for logs:
# payment-service: "Created 10 test customers"
# payment-service: "Started PaymentServiceApplication in X seconds"
```

### Test 2: Create Order (Happy Path)

```bash
# Create order with valid customer
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-1",
    "items": [
      {
        "productId": "PROD-001",
        "productName": "Laptop",
        "quantity": 1,
        "price": 999.99
      }
    ]
  }'
```

**Expected Logs (order-service):**
```
Publishing OrderCreatedEvent: orderId=abc-123
Message sent successfully: orderId=abc-123, offset=0
```

**Expected Logs (payment-service):**
```
Received OrderCreatedEvent: orderId=abc-123, customerId=CUST-1, amount=$999.99
Processing payment for order: abc-123
Customer CUST-1 | Available: $2500.00 | Required: $999.99
Payment ACCEPTED for order: abc-123 | Reserved: $999.99
Published PaymentProcessedEvent: ACCEPT for order abc-123
```

### Test 3: Verify in H2 Database

```bash
# Access H2 console
open http://localhost:8082/h2-console

# Query customer balance
SELECT * FROM customers WHERE customer_id = 'CUST-1';

# Expected:
# AMOUNT_AVAILABLE: 1500 (was 2500, now 2500 - 999.99 = 1500.01)
# AMOUNT_RESERVED: 99999 (999.99 in cents)
```

### Test 4: Verify Event in Kafka UI

```bash
open http://localhost:8080

# Navigate to Topics → "payment-events" → Messages
# Should see PaymentProcessedEvent JSON:
{
  "orderId": "abc-123",
  "customerId": "CUST-1",
  "amount": 999.99,
  "status": "ACCEPT",
  "processedAt": "2026-07-04T..."
}
```

### Test 5: Insufficient Balance (Rejection)

```bash
# Create expensive order
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-1",
    "items": [
      {
        "productId": "PROD-002",
        "productName": "Server",
        "quantity": 1,
        "price": 9999.99
      }
    ]
  }'
```

**Expected Logs (payment-service):**
```
Received OrderCreatedEvent: orderId=xyz-456, customerId=CUST-1, amount=$9999.99
Payment REJECTED for order: xyz-456 | Insufficient balance
Published PaymentProcessedEvent: REJECT for order xyz-456 | Reason: Insufficient balance
```

### Test 6: Unknown Customer

```bash
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-999",
    "items": [{"productId": "P1", "productName": "Item", "quantity": 1, "price": 10.00}]
  }'
```

**Expected:** Payment REJECTED with reason "Customer not found"

---

## 📚 Learning Outcomes

### Concepts Mastered

**1. Kafka Consumer**
- `@KafkaListener` annotation
- Consumer groups for load balancing
- Offset management (auto-commit vs. manual)
- Deserialization (JSON → Java objects)
- Error handling and retries

**2. Spring Data JPA**
- Entity mapping (`@Entity`, `@Table`, `@Column`)
- Repository pattern (`JpaRepository`)
- Derived query methods (`findByCustomerId`)
- `@Transactional` for ACID guarantees
- H2 in-memory database

**3. SAGA Pattern (Phase 1: Reserve)**
- **Tentative operation:** Reserve funds (not commit yet)
- **Publish response:** Let orchestrator know result
- **Wait for decision:** Final commit/rollback comes later (Phase 4)

**4. Event-Driven Architecture**
- **Loose coupling:** payment-service doesn't call order-service directly
- **Asynchronous:** Processing happens in background
- **Scalability:** Can run multiple payment-service instances

### Design Patterns Used

1. **Repository Pattern:** `CustomerRepository` abstracts data access
2. **Service Layer Pattern:** `PaymentService` contains business logic
3. **Event-Driven:** Services communicate via Kafka events
4. **Two-Phase Commit (SAGA):** Reserve → Confirm/Rollback
5. **Dependency Injection:** Constructor injection throughout

---

## 🚧 Known Issues & Future Enhancements

### Phase 3 Limitations
- **No CONFIRM/ROLLBACK handling yet:** Currently only implements RESERVE phase
- **No idempotency:** Processing same event twice will double-reserve funds
- **No DLQ:** Failed messages block consumer (no dead-letter queue yet)
- **No retry logic:** Single failure causes permanent reprocessing
- **Synchronous processing:** One message at a time (no batch processing)

### Phase 4 Preview (SAGA Completion)
After Phase 3, we'll add:
1. Listen for final decision events from order-service
2. Implement `confirm()` (commit reserved funds)
3. Implement `rollback()` (return reserved funds)
4. Add idempotency checks (store processed order IDs)
5. Add error handling with DLQ

---

## ✅ Completion Checklist

- [ ] payment-service project created with correct dependencies
- [ ] `application.yml` configured (Kafka consumer + producer, JPA, H2)
- [ ] `Customer` entity created with reserve/confirm/rollback methods
- [ ] `CustomerRepository` interface created
- [ ] `DataInitializer` creates 10 test customers on startup
- [ ] `OrderCreatedEvent` DTO matches order-service structure
- [ ] `PaymentProcessedEvent` DTO created
- [ ] `PaymentService` implements payment validation logic
- [ ] `OrderEventConsumer` with `@KafkaListener` implemented
- [ ] Application compiles successfully (`mvn clean compile`)
- [ ] Both services start without errors
- [ ] Order creation triggers payment validation
- [ ] Payment acceptance/rejection visible in logs
- [ ] Customer balance updated in H2 database
- [ ] `PaymentProcessedEvent` published to Kafka

---

## 🎯 Next Steps (Phase 4)

After completing Phase 3, you'll have:
- ✅ order-service (producer)
- ✅ payment-service (consumer + producer)
- ✅ One-way event flow: order → payment → response

**Phase 4 Goals:**
1. **SAGA orchestration in order-service**
   - Consume payment-events
   - Store order state
   - Determine final status (CONFIRMED/REJECTED)
   - Publish final decision back to order-events

2. **SAGA completion in payment-service**
   - Listen for final decision events
   - Confirm or rollback reservations

This closes the SAGA loop and implements the full distributed transaction pattern.

---

## 📖 Reference Materials

- **Project Plan:** `/docs/PROJECT-PLAN.md` (Phase 3 section)
- **Architecture Overview:** `/docs/ARCHITECTURE-OVERVIEW.md`
- **Kafka Fundamentals:** `/docs/03-kafka/` (all files)
- **Reference Repo:** [piomin/sample-spring-kafka-microservices](https://github.com/piomin/sample-spring-kafka-microservices)

---

**Ready to start Phase 3? Follow the steps above and let me know if you hit any issues!** 🚀
