# Task 05: Implement Kafka Streams Orchestration with Stock Service

> **Goal:** Migrate from simple @KafkaListener orchestration to Kafka Streams with KStream-KStream joins  
> **Prerequisite:** Phase 4 complete and tested  
> **Estimated Time:** 2-3 weeks  
> **Difficulty:** Advanced

---

## Overview

### What You'll Build

Transform the 2-service choreography pattern into a 3-service Kafka Streams orchestration:

**Current (Phase 4):**
```
Order → Payment → Decision (if/else)
```

**Target (Phase 5):**
```
Order → Payment + Stock → Stream Join → Decision (3-way matrix)
         ↓         ↓
    KStream    KStream  → Join Window (10s) → KTable (RocksDB)
```

### Key Differences

| Aspect | Phase 4 | Phase 5 |
|--------|---------|---------|
| **Services** | 2 (order, payment) | 3 (order, payment, stock) |
| **Orchestration** | @KafkaListener + if/else | Kafka Streams join topology |
| **State** | ConcurrentHashMap | RocksDB-backed KTable |
| **Decision Logic** | 2-way (ACCEPT/REJECT) | 3-way (CONFIRMED/ROLLBACK/REJECTED) |
| **Coordination** | Sequential messages | Windowed stream join (10s) |
| **Compensation** | Simple (no partial success) | ROLLBACK with source tracking |

---

## Architecture Note: Multi-Item vs Single-Item Pattern

**This implementation handles multi-item orders** (realistic e-commerce scenario):
- Orders can have multiple products with different quantities
- Stock service reserves ALL items atomically (all or nothing)
- Payment service charges total amount (sum of all items)
- Join operates at ORDER level (not per-item)

**Reference repo uses single-item pattern** (simplified for Kafka Streams demo):
- One product per order (not a list)
- Simpler SAGA logic (no aggregation needed)
- Focus on Kafka Streams features, not business complexity

**Why we chose multi-item:**
- ✅ More realistic (real orders have multiple items)
- ✅ Shows atomic transaction handling (partial failure → full rollback)
- ✅ Better learning (handling aggregation in distributed systems)
- ✅ Builds on Phase 4 (already has List<OrderItem> structure)

See `PHASE-5-ARCHITECTURE-COMPARISON.md` for detailed comparison with reference repo.

---

## Part 1: Add Stock Service (Days 1-3)

### 1.1 Create stock-service Module

**Directory structure:**
```
stock-service/
├── src/main/java/com/example/stockservice/
│   ├── StockServiceApplication.java
│   ├── consumer/OrderEventConsumer.java
│   ├── service/StockService.java
│   ├── model/Product.java
│   ├── repository/ProductRepository.java
│   ├── event/StockProcessedEvent.java
│   └── config/DataInitializer.java
├── src/main/resources/
│   └── application.yml
└── pom.xml
```

### 1.2 Create pom.xml

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
    <artifactId>stock-service</artifactId>
    <version>1.0.0</version>
    <name>stock-service</name>
    <description>Stock/Inventory management microservice</description>

    <properties>
        <java.version>21</java.version>
    </properties>

    <dependencies>
        <!-- Spring Kafka -->
        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka</artifactId>
        </dependency>

        <!-- Spring Data JPA -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-data-jpa</artifactId>
        </dependency>

        <!-- H2 Database -->
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

        <!-- Jackson -->
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <configuration>
                    <source>21</source>
                    <target>21</target>
                    <annotationProcessorPaths>
                        <path>
                            <groupId>org.projectlombok</groupId>
                            <artifactId>lombok</artifactId>
                            <version>1.18.46</version>
                        </path>
                    </annotationProcessorPaths>
                </configuration>
            </plugin>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>
```

### 1.3 Create Product Entity

**File:** `stock-service/src/main/java/com/example/stockservice/model/Product.java`

```java
package com.example.stockservice.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.math.BigDecimal;

@Entity
@Table(name = "products")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Product {
    
    @Id
    private String productId;
    
    private String productName;
    
    private Integer availableItems;  // Available for new orders
    
    private Integer reservedItems;   // Reserved for pending orders
    
    private BigDecimal price;
    
    /**
     * Reserve stock for an order.
     * 
     * @param quantity Quantity to reserve
     * @return true if reservation successful, false if insufficient stock
     */
    public boolean reserve(int quantity) {
        if (availableItems >= quantity) {
            availableItems -= quantity;
            reservedItems += quantity;
            return true;
        }
        return false;
    }
    
    /**
     * Confirm reservation (commit the reservation).
     * Moves items from reserved to permanently deducted.
     */
    public void confirm(int quantity) {
        if (reservedItems >= quantity) {
            reservedItems -= quantity;
            // Items are now sold (not returned to available)
        }
    }
    
    /**
     * Rollback reservation (compensating transaction).
     * Returns reserved items back to available pool.
     */
    public void rollback(int quantity) {
        if (reservedItems >= quantity) {
            reservedItems -= quantity;
            availableItems += quantity;
        }
    }
}
```

### 1.4 Create Repository

**File:** `stock-service/src/main/java/com/example/stockservice/repository/ProductRepository.java`

```java
package com.example.stockservice.repository;

import com.example.stockservice.model.Product;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProductRepository extends JpaRepository<Product, String> {
}
```

### 1.5 Create StockProcessedEvent

**File:** `stock-service/src/main/java/com/example/stockservice/event/StockProcessedEvent.java`

```java
package com.example.stockservice.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class StockProcessedEvent {
    private String orderId;
    private String customerId;
    private List<StockItemResult> items;  // Results for ALL items
    private StockStatus status;
    private String reason;  // Rejection reason (if status = REJECT)
    
    public enum StockStatus {
        ACCEPT,   // All items reserved successfully
        REJECT    // At least one item unavailable
    }
    
    /**
     * Inner class for individual item results.
     * Useful for tracking which specific items failed.
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class StockItemResult {
        private String productId;
        private String productName;
        private int quantity;
        private boolean available;  // Was this specific item available?
    }
}
```

### 1.6 Create StockService

**File:** `stock-service/src/main/java/com/example/stockservice/service/StockService.java`

```java
package com.example.stockservice.service;

import com.example.stockservice.event.OrderCreatedEvent;
import com.example.stockservice.event.StockProcessedEvent;
import com.example.stockservice.event.StockProcessedEvent.StockItemResult;
import com.example.stockservice.event.StockProcessedEvent.StockStatus;
import com.example.stockservice.model.Product;
import com.example.stockservice.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
@RequiredArgsConstructor
public class StockService {
    
    private final ProductRepository productRepository;
    
    // Idempotency tracking
    private final Set<String> processedReservations = ConcurrentHashMap.newKeySet();
    private final Set<String> processedDecisions = ConcurrentHashMap.newKeySet();
    
    // Cache last response for idempotency
    private final Map<String, StockProcessedEvent> responseCache = new ConcurrentHashMap<>();
    
    /**
     * Reserve stock for ALL items in an order (Phase 1 of SAGA).
     * 
     * ATOMIC OPERATION:
     * - Either ALL items are reserved, or NONE are reserved
     * - If any item fails, rollback all previously reserved items in this order
     * 
     * This ensures consistency: we never end up with partial reservations.
     */
    @Transactional
    public StockProcessedEvent processOrderStock(OrderCreatedEvent event) {
        String orderId = event.getOrderId();
        
        // Idempotency check
        if (processedReservations.contains(orderId)) {
            log.warn("Duplicate stock reservation request for order: {}", orderId);
            return responseCache.getOrDefault(orderId, buildDefaultResponse(orderId));
        }
        
        log.info("Processing stock for order: {} | Items: {}", orderId, event.getItems().size());
        
        List<StockItemResult> itemResults = new ArrayList<>();
        List<Product> reservedProducts = new ArrayList<>();  // Track for rollback
        boolean allAvailable = true;
        StringBuilder failureReason = new StringBuilder();
        
        // Try to reserve ALL items
        for (OrderCreatedEvent.OrderItemEvent item : event.getItems()) {
            Product product = productRepository.findById(item.getProductId()).orElse(null);
            
            if (product == null) {
                log.warn("Product not found: {}", item.getProductId());
                allAvailable = false;
                failureReason.append("Product ").append(item.getProductId()).append(" not found; ");
                
                itemResults.add(StockItemResult.builder()
                    .productId(item.getProductId())
                    .productName(item.getProductName())
                    .quantity(item.getQuantity())
                    .available(false)
                    .build());
                break;  // Stop processing, will rollback
            }
            
            log.info("Product {} | Available: {} | Reserved: {} | Required: {}", 
                     item.getProductId(), product.getAvailableItems(), 
                     product.getReservedItems(), item.getQuantity());
            
            boolean reserved = product.reserve(item.getQuantity());
            
            if (!reserved) {
                log.warn("Insufficient stock for product: {} | Available: {} | Required: {}", 
                         item.getProductId(), product.getAvailableItems(), item.getQuantity());
                allAvailable = false;
                failureReason.append("Product ").append(item.getProductId())
                             .append(" insufficient (available: ").append(product.getAvailableItems())
                             .append(", required: ").append(item.getQuantity()).append("); ");
                
                itemResults.add(StockItemResult.builder()
                    .productId(item.getProductId())
                    .productName(item.getProductName())
                    .quantity(item.getQuantity())
                    .available(false)
                    .build());
                break;  // Stop processing, will rollback
            }
            
            // Successfully reserved this item
            reservedProducts.add(product);
            itemResults.add(StockItemResult.builder()
                .productId(item.getProductId())
                .productName(item.getProductName())
                .quantity(item.getQuantity())
                .available(true)
                .build());
            
            log.info("Reserved {} units of product {}", item.getQuantity(), item.getProductId());
        }
        
        StockProcessedEvent response;
        
        if (allAvailable) {
            // All items reserved successfully - commit to database
            productRepository.saveAll(reservedProducts);
            productRepository.flush();
            processedReservations.add(orderId);
            
            log.info("Stock RESERVED for order: {} | All {} items available", 
                     orderId, event.getItems().size());
            
            response = StockProcessedEvent.builder()
                .orderId(orderId)
                .customerId(event.getCustomerId())
                .items(itemResults)
                .status(StockStatus.ACCEPT)
                .build();
            
        } else {
            // At least one item failed - rollback ALL reserved items
            log.warn("Stock REJECTED for order: {} | Reason: {}", orderId, failureReason);
            
            for (Product product : reservedProducts) {
                // Find the quantity we reserved for this product
                int quantityToRollback = event.getItems().stream()
                    .filter(item -> item.getProductId().equals(product.getProductId()))
                    .findFirst()
                    .map(OrderCreatedEvent.OrderItemEvent::getQuantity)
                    .orElse(0);
                
                product.rollback(quantityToRollback);
                log.info("Rolled back {} units of product {}", quantityToRollback, product.getProductId());
            }
            
            productRepository.saveAll(reservedProducts);
            productRepository.flush();
            processedReservations.add(orderId);
            
            response = StockProcessedEvent.builder()
                .orderId(orderId)
                .customerId(event.getCustomerId())
                .items(itemResults)
                .status(StockStatus.REJECT)
                .reason(failureReason.toString().trim())
                .build();
        }
        
        // Cache response for idempotency
        responseCache.put(orderId, response);
        return response;
    }
    
    /**
     * Confirm stock reservation for ALL items (Phase 2 of SAGA - CONFIRMED path).
     * Moves items from reserved to permanently deducted.
     */
    @Transactional
    public void handleConfirm(OrderCreatedEvent event) {
        String orderId = event.getOrderId();
        
        if (processedDecisions.contains(orderId)) {
            log.warn("Duplicate confirm for order: {}", orderId);
            return;
        }
        
        log.info("Received CONFIRM decision for order: {} | Items: {}", 
                 orderId, event.getItems().size());
        
        List<Product> productsToUpdate = new ArrayList<>();
        
        for (OrderCreatedEvent.OrderItemEvent item : event.getItems()) {
            Product product = productRepository.findById(item.getProductId()).orElse(null);
            if (product != null) {
                product.confirm(item.getQuantity());
                productsToUpdate.add(product);
                
                log.info("Confirmed {} units of product {} | Reserved now: {}", 
                         item.getQuantity(), item.getProductId(), product.getReservedItems());
            }
        }
        
        productRepository.saveAll(productsToUpdate);
        productRepository.flush();
        processedDecisions.add(orderId);
        
        log.info("Stock CONFIRMED for order: {} | All {} items deducted", 
                 orderId, event.getItems().size());
    }
    
    /**
     * Rollback stock reservation for ALL items (Phase 2 of SAGA - REJECTED/ROLLBACK path).
     * Returns items from reserved back to available pool.
     */
    @Transactional
    public void handleRollback(OrderCreatedEvent event) {
        String orderId = event.getOrderId();
        
        if (processedDecisions.contains(orderId)) {
            log.warn("Duplicate rollback for order: {}", orderId);
            return;
        }
        
        log.info("Received ROLLBACK decision for order: {} | Items: {}", 
                 orderId, event.getItems().size());
        
        List<Product> productsToUpdate = new ArrayList<>();
        
        for (OrderCreatedEvent.OrderItemEvent item : event.getItems()) {
            Product product = productRepository.findById(item.getProductId()).orElse(null);
            if (product != null) {
                product.rollback(item.getQuantity());
                productsToUpdate.add(product);
                
                log.info("Rolled back {} units of product {} | Available now: {}", 
                         item.getQuantity(), item.getProductId(), product.getAvailableItems());
            }
        }
        
        productRepository.saveAll(productsToUpdate);
        productRepository.flush();
        processedDecisions.add(orderId);
        
        log.info("Stock ROLLED BACK for order: {} | All {} items returned to inventory", 
                 orderId, event.getItems().size());
    }
    
    private StockProcessedEvent buildDefaultResponse(String orderId) {
        return StockProcessedEvent.builder()
            .orderId(orderId)
            .status(StockStatus.ACCEPT)
            .items(Collections.emptyList())
            .build();
    }
}
```

### 1.7 Create OrderEventConsumer

**File:** `stock-service/src/main/java/com/example/stockservice/consumer/OrderEventConsumer.java`

```java
package com.example.stockservice.consumer;

import com.example.stockservice.event.OrderCreatedEvent;
import com.example.stockservice.event.FinalDecisionEvent;
import com.example.stockservice.event.StockProcessedEvent;
import com.example.stockservice.service.StockService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import static com.example.stockservice.event.FinalDecisionEvent.DecisionStatus.*;

@Component
@Slf4j
@RequiredArgsConstructor
public class OrderEventConsumer {
    
    private final StockService stockService;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    
    private static final String STOCK_EVENTS_TOPIC = "stock-events";
    
    /**
     * Consumer 1: Listen for new orders (reserve stock for ALL items).
     * 
     * ATOMIC RESERVATION:
     * - Passes entire event to service layer
     * - Service handles atomic reservation (all or nothing)
     */
    @KafkaListener(topics = "order-events", groupId = "stock-service-group")
    public void consumeOrderEvent(@Payload OrderCreatedEvent event,
                                  @Header(KafkaHeaders.RECEIVED_KEY) String key) {
        
        log.info("Received OrderCreatedEvent: orderId={}, customerId={}, items={}", 
                 event.getOrderId(), event.getCustomerId(), event.getItems().size());
        
        // Log all items for visibility
        event.getItems().forEach(item -> 
            log.info("  - Product: {} | Quantity: {} | Price: {}", 
                     item.getProductId(), item.getQuantity(), item.getPrice()));
        
        // Process ALL items atomically
        StockProcessedEvent stockEvent = stockService.processOrderStock(event);
        
        // Publish stock response
        kafkaTemplate.send(STOCK_EVENTS_TOPIC, event.getOrderId(), stockEvent);
        log.info("Published StockProcessedEvent: orderId={}, status={}, items={}", 
                 event.getOrderId(), stockEvent.getStatus(), stockEvent.getItems().size());
    }
    
    /**
     * Consumer 2: Listen for final decisions (confirm/rollback ALL items).
     * 
     * COMPENSATION HANDLING:
     * - CONFIRMED: Deduct all reserved items permanently
     * - ROLLBACK: Return all reserved items to available pool
     * - REJECTED: No-op (nothing was reserved)
     */
    @KafkaListener(topics = "order-events", groupId = "stock-decision-group")
    public void consumeDecisionEvent(@Payload FinalDecisionEvent event,
                                     @Header(KafkaHeaders.RECEIVED_KEY) String key) {
        
        log.info("Received FinalDecisionEvent: orderId={}, status={}, items={}", 
                 event.getOrderId(), event.getStatus(), event.getItems().size());
        
        // Convert FinalDecisionEvent back to OrderCreatedEvent structure for processing
        // (Both have same fields: orderId, customerId, items)
        OrderCreatedEvent orderEvent = convertToOrderCreatedEvent(event);
        
        if (event.getStatus() == CONFIRMED) {
            stockService.handleConfirm(orderEvent);
        } else if (event.getStatus() == ROLLBACK) {
            // Only rollback if stock was the successful service
            // (payment failed, so we need to return reserved stock)
            if (!"STOCK".equals(event.getSource())) {
                log.info("ROLLBACK triggered by payment failure - returning stock to inventory");
                stockService.handleRollback(orderEvent);
            } else {
                log.info("ROLLBACK triggered by stock failure - no compensation needed");
            }
        } else if (event.getStatus() == REJECTED) {
            // Both services rejected - nothing to rollback
            log.info("Order REJECTED - no stock compensation needed");
        }
    }
    
    /**
     * Helper to convert FinalDecisionEvent to OrderCreatedEvent structure.
     * Both events have the same core fields (orderId, customerId, items).
     */
    private OrderCreatedEvent convertToOrderCreatedEvent(FinalDecisionEvent event) {
        OrderCreatedEvent orderEvent = new OrderCreatedEvent();
        orderEvent.setOrderId(event.getOrderId());
        orderEvent.setCustomerId(event.getCustomerId());
        
        // Convert item DTOs
        List<OrderCreatedEvent.OrderItemEvent> items = event.getItems().stream()
            .map(item -> new OrderCreatedEvent.OrderItemEvent(
                item.getProductId(),
                item.getProductName(),
                item.getQuantity(),
                item.getPrice()
            ))
            .collect(java.util.stream.Collectors.toList());
        
        orderEvent.setItems(items);
        orderEvent.setTotalAmount(event.getItems().stream()
            .map(item -> item.getPrice().multiply(java.math.BigDecimal.valueOf(item.getQuantity())))
            .reduce(java.math.BigDecimal.ZERO, java.math.BigDecimal::add));
        
        return orderEvent;
    }
}
```

### 1.8 Create Events (Copy from order-service)

Copy these files from order-service to stock-service:
- `OrderCreatedEvent.java`
- `FinalDecisionEvent.java`

### 1.9 Create DataInitializer

**File:** `stock-service/src/main/java/com/example/stockservice/config/DataInitializer.java`

```java
package com.example.stockservice.config;

import com.example.stockservice.model.Product;
import com.example.stockservice.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.List;

@Component
@Slf4j
@RequiredArgsConstructor
public class DataInitializer implements CommandLineRunner {
    
    private final ProductRepository productRepository;
    
    @Override
    public void run(String... args) {
        log.info("Initializing test product data...");
        
        List<Product> products = List.of(
            new Product("PROD-001", "Laptop", 50, 0, new BigDecimal("999.99")),
            new Product("PROD-002", "Mouse", 200, 0, new BigDecimal("29.99")),
            new Product("PROD-003", "Keyboard", 150, 0, new BigDecimal("79.99")),
            new Product("PROD-004", "Monitor", 30, 0, new BigDecimal("299.99")),
            new Product("PROD-005", "Headphones", 100, 0, new BigDecimal("149.99")),
            new Product("PROD-006", "Webcam", 75, 0, new BigDecimal("89.99")),
            new Product("PROD-007", "USB-C Hub", 120, 0, new BigDecimal("49.99")),
            new Product("PROD-008", "External SSD", 60, 0, new BigDecimal("129.99")),
            new Product("PROD-009", "Desk Lamp", 40, 0, new BigDecimal("39.99")),
            new Product("PROD-010", "Chair", 20, 0, new BigDecimal("399.99"))
        );
        
        productRepository.saveAll(products);
        
        log.info("Initialized {} products with stock", products.size());
    }
}
```

### 1.10 Create application.yml

**File:** `stock-service/src/main/resources/application.yml`

```yaml
spring:
  application:
    name: stock-service
  
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: stock-service-group
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "*"
        spring.json.type.mapping: >
          OrderCreatedEvent:com.example.stockservice.event.OrderCreatedEvent,
          FinalDecisionEvent:com.example.stockservice.event.FinalDecisionEvent
    
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      properties:
        spring.json.type.mapping: >
          StockProcessedEvent:com.example.stockservice.event.StockProcessedEvent
  
  datasource:
    url: jdbc:h2:mem:stockdb
    driver-class-name: org.h2.Driver
    username: sa
    password:
  
  h2:
    console:
      enabled: true
      path: /h2-console
  
  jpa:
    database-platform: org.hibernate.dialect.H2Dialect
    hibernate:
      ddl-auto: create-drop
    show-sql: true
    properties:
      hibernate:
        format_sql: true

logging:
  level:
    com.example.stockservice: DEBUG
    org.springframework.kafka: INFO
```

### 1.11 Create Main Application

**File:** `stock-service/src/main/java/com/example/stockservice/StockServiceApplication.java`

```java
package com.example.stockservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class StockServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(StockServiceApplication.class, args);
    }
}
```

### 1.12 Test Stock Service Standalone

```bash
# Terminal 1: Kafka
docker-compose up -d

# Terminal 2: Stock service
cd stock-service
mvn spring-boot:run

# Wait for: "Started StockServiceApplication"
# Check H2 console: http://localhost:8083/h2-console
# JDBC URL: jdbc:h2:mem:stockdb
# Verify 10 products exist
```

---

## Part 2: Migrate order-service to Kafka Streams (Days 4-8)

### 2.1 Add Kafka Streams Dependencies

**File:** `order-service/pom.xml`

Add to `<dependencies>`:

```xml
<!-- Kafka Streams -->
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-streams</artifactId>
</dependency>

<!-- Kafka Streams Test Utils (for testing) -->
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-streams-test-utils</artifactId>
    <scope>test</scope>
</dependency>
```

### 2.2 Update application.yml

**File:** `order-service/src/main/resources/application.yml`

Add Kafka Streams configuration:

```yaml
spring:
  application:
    name: order-service
  
  kafka:
    bootstrap-servers: localhost:9092
    
    # Kafka Streams Configuration (NEW)
    streams:
      application-id: order-service-streams
      state-dir: /tmp/kafka-streams/order-service
      properties:
        default.key.serde: org.apache.kafka.common.serialization.Serdes$StringSerde
        default.value.serde: org.springframework.kafka.support.serializer.JsonSerde
        spring.json.trusted.packages: "*"
        num.stream.threads: 2
        commit.interval.ms: 1000
        spring.json.type.mapping: >
          PaymentProcessedEvent:com.example.orderservice.event.PaymentProcessedEvent,
          StockProcessedEvent:com.example.orderservice.event.StockProcessedEvent,
          Order:com.example.orderservice.model.Order
    
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      properties:
        spring.json.type.mapping: >
          OrderCreatedEvent:com.example.orderservice.event.OrderCreatedEvent,
          FinalDecisionEvent:com.example.orderservice.event.FinalDecisionEvent
```

### 2.3 Enable Kafka Streams

**File:** `order-service/src/main/java/com/example/orderservice/OrderServiceApplication.java`

```java
package com.example.orderservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;  // NEW

@SpringBootApplication
@EnableKafka
@EnableKafkaStreams  // NEW: Enable Kafka Streams
public class OrderServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}
```

### 2.4 Create StockProcessedEvent

**File:** `order-service/src/main/java/com/example/orderservice/event/StockProcessedEvent.java`

```java
package com.example.orderservice.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class StockProcessedEvent {
    private String orderId;
    private String customerId;
    private List<StockItemResult> items;  // Results for ALL items
    private StockStatus status;
    private String reason;
    
    public enum StockStatus {
        ACCEPT,   // All items available
        REJECT    // At least one item unavailable
    }
    
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class StockItemResult {
        private String productId;
        private String productName;
        private int quantity;
        private boolean available;
    }
}
```

### 2.5 Create Kafka Streams Topology

**File:** `order-service/src/main/java/com/example/orderservice/streams/OrderStreamsTopology.java`

```java
package com.example.orderservice.streams;

import com.example.orderservice.event.FinalDecisionEvent;
import com.example.orderservice.event.FinalDecisionEvent.DecisionStatus;
import com.example.orderservice.event.PaymentProcessedEvent;
import com.example.orderservice.event.PaymentProcessedEvent.PaymentStatus;
import com.example.orderservice.event.StockProcessedEvent;
import com.example.orderservice.event.StockProcessedEvent.StockStatus;
import com.example.orderservice.model.Order;
import com.example.orderservice.model.OrderStatus;
import com.example.orderservice.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;

@Configuration
@Slf4j
@RequiredArgsConstructor
public class OrderStreamsTopology {
    
    private final OrderService orderService;
    
    /**
     * Kafka Streams topology that joins payment and stock responses.
     * 
     * This replaces the old PaymentEventConsumer + OrderOrchestrationService pattern.
     */
    @Bean
    public KStream<String, FinalDecisionEvent> orderSagaStream(StreamsBuilder builder) {
        
        // Configure serdes
        JsonSerde<PaymentProcessedEvent> paymentSerde = new JsonSerde<>(PaymentProcessedEvent.class);
        JsonSerde<StockProcessedEvent> stockSerde = new JsonSerde<>(StockProcessedEvent.class);
        JsonSerde<FinalDecisionEvent> decisionSerde = new JsonSerde<>(FinalDecisionEvent.class);
        
        // Stream 1: Payment responses
        KStream<String, PaymentProcessedEvent> paymentStream = builder
            .stream("payment-events", Consumed.with(Serdes.String(), paymentSerde))
            .peek((key, value) -> log.info("Received payment event: orderId={}, status={}", 
                                           key, value.getStatus()));
        
        // Stream 2: Stock responses
        KStream<String, StockProcessedEvent> stockStream = builder
            .stream("stock-events", Consumed.with(Serdes.String(), stockSerde))
            .peek((key, value) -> log.info("Received stock event: orderId={}, status={}", 
                                           key, value.getStatus()));
        
        // Join payment + stock within 10-second window
        KStream<String, FinalDecisionEvent> joinedStream = paymentStream.join(
            stockStream,
            this::makeDecision,  // Join function (3-way logic)
            JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(10)),  // Time window
            StreamJoined.with(
                Serdes.String(),
                paymentSerde,
                stockSerde
            )
        );
        
        // Publish final decision
        joinedStream
            .peek((key, decision) -> {
                log.info("Final decision for order {}: status={}, source={}", 
                         key, decision.getStatus(), decision.getSource());
                
                // Update order state
                Order order = orderService.getOrderById(key);
                if (order != null) {
                    order.setStatus(mapToOrderStatus(decision.getStatus()));
                    orderService.updateOrder(order);
                }
            })
            .to("order-events", Produced.with(Serdes.String(), decisionSerde));
        
        return joinedStream;
    }
    
    /**
     * 3-way decision logic (THE HEART OF SAGA ORCHESTRATION).
     * 
     * Decision Matrix:
     * ┌─────────┬────────┬──────────────┐
     * │ Payment │ Stock  │ Decision     │
     * ├─────────┼────────┼──────────────┤
     * │ ACCEPT  │ ACCEPT │ CONFIRMED    │
     * │ REJECT  │ REJECT │ REJECTED     │
     * │ ACCEPT  │ REJECT │ ROLLBACK (payment) │
     * │ REJECT  │ ACCEPT │ ROLLBACK (stock)   │
     * └─────────┴────────┴──────────────┘
     * 
     * MULTI-ITEM HANDLING:
     * - Both events contain results for ALL items in the order
     * - Decision is made at ORDER level (not per-item)
     * - If ANY item fails, entire order fails
     */
    private FinalDecisionEvent makeDecision(PaymentProcessedEvent payment, 
                                           StockProcessedEvent stock) {
        
        boolean paymentAccepted = payment.getStatus() == PaymentStatus.ACCEPT;
        boolean stockAccepted = stock.getStatus() == StockStatus.ACCEPT;
        
        log.info("Making decision for order {}: payment={}, stock={} | Items: {}", 
                 payment.getOrderId(), payment.getStatus(), stock.getStatus(),
                 payment.getItems().size());
        
        // Log detailed item status for debugging
        log.debug("Payment details: customerId={}, totalCharged={}", 
                  payment.getCustomerId(), payment.getAmountCharged());
        log.debug("Stock details: {} items processed", stock.getItems().size());
        
        // Case 1: Both accepted → CONFIRMED
        if (paymentAccepted && stockAccepted) {
            log.info("Order {} CONFIRMED: payment OK, stock OK", payment.getOrderId());
            
            return FinalDecisionEvent.builder()
                .orderId(payment.getOrderId())
                .customerId(payment.getCustomerId())
                .items(payment.getItems())  // Use items from payment event
                .status(DecisionStatus.CONFIRMED)
                .source(null)  // No source needed for success
                .build();
        }
        
        // Case 2: Both rejected → REJECTED (nothing to rollback)
        if (!paymentAccepted && !stockAccepted) {
            log.warn("Order {} REJECTED: both payment and stock failed", payment.getOrderId());
            
            String combinedReason = String.format("Payment: %s; Stock: %s", 
                                                  payment.getReason(), stock.getReason());
            
            return FinalDecisionEvent.builder()
                .orderId(payment.getOrderId())
                .customerId(payment.getCustomerId())
                .items(payment.getItems())
                .status(DecisionStatus.REJECTED)
                .reason(combinedReason)
                .source(null)
                .build();
        }
        
        // Case 3 & 4: One accepted, one rejected → ROLLBACK
        // Track which service caused the failure (needs compensation)
        String failureSource = !paymentAccepted ? "PAYMENT" : "STOCK";
        String reason = !paymentAccepted ? payment.getReason() : stock.getReason();
        String successfulService = !paymentAccepted ? "stock" : "payment";
        
        log.warn("Order {} ROLLBACK: {} failed, {} succeeded - compensating {}", 
                 payment.getOrderId(), failureSource.toLowerCase(), 
                 successfulService, successfulService);
        
        return FinalDecisionEvent.builder()
            .orderId(payment.getOrderId())
            .customerId(payment.getCustomerId())
            .items(payment.getItems())
            .status(DecisionStatus.ROLLBACK)
            .reason(reason)
            .source(failureSource)  // CRITICAL: Tells services who to rollback
            .build();
    }
    
    private OrderStatus mapToOrderStatus(DecisionStatus decisionStatus) {
        return switch (decisionStatus) {
            case CONFIRMED -> OrderStatus.CONFIRMED;
            case REJECTED -> OrderStatus.REJECTED;
            case ROLLBACK -> OrderStatus.CANCELLED;  // New status for rollback
        };
    }
}
```

### 2.6 Update FinalDecisionEvent

**File:** `order-service/src/main/java/com/example/orderservice/event/FinalDecisionEvent.java`

Add `source` field:

```java
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FinalDecisionEvent {
    private String orderId;
    private String customerId;
    private List<OrderItemDTO> items;
    private DecisionStatus status;
    private String reason;
    private String source;  // NEW: "PAYMENT" or "STOCK" (for ROLLBACK tracking)
    
    public enum DecisionStatus {
        CONFIRMED,
        REJECTED,
        ROLLBACK  // NEW: Partial success → compensate
    }
}
```

### 2.7 Update OrderStatus Enum

**File:** `order-service/src/main/java/com/example/orderservice/model/OrderStatus.java`

```java
public enum OrderStatus {
    PENDING,
    CONFIRMED,
    REJECTED,
    CANCELLED  // NEW: For rollback scenarios
}
```

### 2.8 Delete Old Orchestration Files

**Remove these files** (replaced by Kafka Streams):
- `order-service/src/main/java/com/example/orderservice/consumer/PaymentEventConsumer.java`
- `order-service/src/main/java/com/example/orderservice/service/OrderOrchestrationService.java`

### 2.9 Update payment-service DecisionEventConsumer

Handle new ROLLBACK status with proper multi-item support:

**File:** `payment-service/src/main/java/com/example/paymentservice/consumer/DecisionEventConsumer.java`

```java
@KafkaListener(topics = "order-events", groupId = "payment-decision-group")
public void consumeDecisionEvent(@Payload FinalDecisionEvent event) {
    
    log.info("Received FinalDecisionEvent: orderId={}, status={}, source={}, items={}", 
             event.getOrderId(), event.getStatus(), event.getSource(), event.getItems().size());
    
    if (event.getStatus() == CONFIRMED) {
        // Payment was successful, stock was successful → Commit payment
        log.info("Order CONFIRMED - committing payment for {} items", event.getItems().size());
        paymentService.handleConfirm(event);
        
    } else if (event.getStatus() == ROLLBACK) {
        // One service succeeded, one failed → Compensate the successful service
        // NEW: Only rollback if payment was the one that succeeded
        // (stock failed, so we need to compensate payment)
        if ("STOCK".equals(event.getSource())) {
            log.info("ROLLBACK triggered by stock failure - compensating payment for {} items", 
                     event.getItems().size());
            paymentService.handleRollback(event);
        } else {
            log.info("ROLLBACK triggered by payment failure - no compensation needed " +
                     "(payment never succeeded)");
        }
        
    } else if (event.getStatus() == REJECTED) {
        // Both failed - nothing to rollback
        log.info("Order REJECTED - no payment compensation needed (both services failed)");
    }
}
```

**Note:** Ensure `paymentService.handleConfirm()` and `handleRollback()` work with the total amount:

```java
// In PaymentService.java
@Transactional
public void handleConfirm(FinalDecisionEvent event) {
    if (processedDecisions.contains(event.getOrderId())) {
        log.warn("Duplicate confirm for order: {}", event.getOrderId());
        return;
    }
    
    // Calculate total from all items
    BigDecimal totalAmount = event.getItems().stream()
        .map(item -> item.getPrice().multiply(BigDecimal.valueOf(item.getQuantity())))
        .reduce(BigDecimal.ZERO, BigDecimal::add);
    
    Customer customer = customerRepository.findById(event.getCustomerId()).orElse(null);
    if (customer != null) {
        customer.confirm(totalAmount);  // Move from reserved to deducted
        customerRepository.save(customer);
        processedDecisions.add(event.getOrderId());
        
        log.info("Payment CONFIRMED for order: {} | Total: {} | Items: {}", 
                 event.getOrderId(), totalAmount, event.getItems().size());
    }
}

@Transactional
public void handleRollback(FinalDecisionEvent event) {
    if (processedDecisions.contains(event.getOrderId())) {
        log.warn("Duplicate rollback for order: {}", event.getOrderId());
        return;
    }
    
    // Calculate total from all items
    BigDecimal totalAmount = event.getItems().stream()
        .map(item -> item.getPrice().multiply(BigDecimal.valueOf(item.getQuantity())))
        .reduce(BigDecimal.ZERO, BigDecimal::add);
    
    Customer customer = customerRepository.findById(event.getCustomerId()).orElse(null);
    if (customer != null) {
        customer.rollback(totalAmount);  // Return from reserved to available
        customerRepository.save(customer);
        processedDecisions.add(event.getOrderId());
        
        log.info("Payment ROLLED BACK for order: {} | Total: {} | Items: {}", 
                 event.getOrderId(), totalAmount, event.getItems().size());
    }
}
```

### 2.10 Update stock-service DecisionEventConsumer

Already covered in Section 1.7 (OrderEventConsumer handles both OrderCreatedEvent and FinalDecisionEvent with proper multi-item support).

---

## Part 3: Add KTable State Store (Days 9-10)

### 3.1 Create KTable for Order State

**File:** `order-service/src/main/java/com/example/orderservice/streams/OrderStateStore.java`

```java
package com.example.orderservice.streams;

import com.example.orderservice.event.FinalDecisionEvent;
import com.example.orderservice.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

@Configuration
@Slf4j
public class OrderStateStore {
    
    /**
     * KTable backed by RocksDB for queryable order state.
     */
    @Bean
    public KTable<String, FinalDecisionEvent> orderStateTable(StreamsBuilder builder) {
        
        // Create persistent state store (RocksDB)
        KeyValueBytesStoreSupplier storeSupplier = 
            Stores.persistentKeyValueStore("order-state-store");
        
        JsonSerde<FinalDecisionEvent> decisionSerde = new JsonSerde<>(FinalDecisionEvent.class);
        
        // Convert order-events stream to table
        KTable<String, FinalDecisionEvent> table = builder
            .stream("order-events", Consumed.with(Serdes.String(), decisionSerde))
            .filter((key, value) -> value.getStatus() != null)  // Filter out nulls
            .toTable(
                Materialized.<String, FinalDecisionEvent>as(storeSupplier)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(decisionSerde)
            );
        
        log.info("Order state KTable created with RocksDB store");
        
        return table;
    }
}
```

### 3.2 Add Interactive Query API

**File:** `order-service/src/main/java/com/example/orderservice/controller/OrderStateController.java`

```java
package com.example.orderservice.controller;

import com.example.orderservice.event.FinalDecisionEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/order-state")
@Slf4j
@RequiredArgsConstructor
public class OrderStateController {
    
    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;
    
    /**
     * Query order state from Kafka Streams state store (not database).
     */
    @GetMapping("/{orderId}")
    public ResponseEntity<FinalDecisionEvent> getOrderState(@PathVariable String orderId) {
        
        KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
        
        if (kafkaStreams == null) {
            return ResponseEntity.status(503).build();  // Service not ready
        }
        
        try {
            ReadOnlyKeyValueStore<String, FinalDecisionEvent> store = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType(
                    "order-state-store",
                    QueryableStoreTypes.keyValueStore()
                )
            );
            
            FinalDecisionEvent decision = store.get(orderId);
            
            if (decision != null) {
                log.info("Retrieved order state from KTable: orderId={}, status={}", 
                         orderId, decision.getStatus());
                return ResponseEntity.ok(decision);
            } else {
                return ResponseEntity.notFound().build();
            }
            
        } catch (Exception e) {
            log.error("Error querying state store", e);
            return ResponseEntity.status(500).build();
        }
    }
}
```

---

## Part 4: Testing (Days 11-12)

### 4.1 Test Scenario 1: Both Accept → CONFIRMED (Multi-Item Order)

```bash
# Start all services
docker-compose down -v && docker-compose up -d
cd payment-service && mvn spring-boot:run &
cd stock-service && mvn spring-boot:run &
cd order-service && mvn spring-boot:run &

# Create multi-item order (all items available, customer has balance)
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
      },
      {
        "productId": "PROD-002",
        "productName": "Mouse",
        "quantity": 2,
        "price": 29.99
      },
      {
        "productId": "PROD-003",
        "productName": "Keyboard",
        "quantity": 1,
        "price": 79.99
      }
    ]
  }'

# Expected flow:
# 1. order-service publishes OrderCreatedEvent (3 items, total: $1,139.96)
# 2. payment-service: Reserves $1,139.96 from CUST-1 → ACCEPT
# 3. stock-service: Reserves 1 laptop + 2 mice + 1 keyboard → ACCEPT (all available)
# 4. Kafka Streams joins both within 10s: CONFIRMED
# 5. payment-service: Confirms payment (moves $1,139.96 from reserved to deducted)
# 6. stock-service: Confirms stock (moves items from reserved to sold)

# Verify in payment-service logs:
# "Payment RESERVED for order: ... | Total: $1139.96"
# "Payment CONFIRMED for order: ... | Total: $1139.96 | Items: 3"

# Verify in stock-service logs:
# "Stock RESERVED for order: ... | All 3 items available"
# "Stock CONFIRMED for order: ... | All 3 items deducted"

# Verify in H2 databases:
# payment-service: CUST-1 balance reduced by $1,139.96
# stock-service: PROD-001 (49 left), PROD-002 (198 left), PROD-003 (149 left)
```

### 4.2 Test Scenario 2: Stock Reject → ROLLBACK Payment (Partial Stock Failure)

```bash
# Create order where ONE item is out of stock (tests atomic reservation)
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-1",
    "items": [
      {
        "productId": "PROD-001",
        "productName": "Laptop",
        "quantity": 5,
        "price": 999.99
      },
      {
        "productId": "PROD-002",
        "productName": "Mouse",
        "quantity": 300,
        "price": 29.99
      }
    ]
  }'

# Expected flow:
# 1. payment-service: Reserves $13,996.95 from CUST-1 → ACCEPT
# 2. stock-service: 
#    - Tries to reserve 5 laptops → SUCCESS (50 available)
#    - Tries to reserve 300 mice → FAIL (only 200 available)
#    - Atomic rollback: Returns 5 laptops to inventory
#    - Overall result → REJECT
# 3. Kafka Streams: ROLLBACK with source=STOCK
# 4. payment-service: Rollback (return $13,996.95 to CUST-1)
# 5. stock-service: No action (atomic rollback already happened)

# Verify in payment-service logs:
# "Payment RESERVED for order: ... | Total: $13996.95"
# "ROLLBACK triggered by stock failure - compensating payment"
# "Payment ROLLED BACK for order: ... | Total: $13996.95"

# Verify in stock-service logs:
# "Reserved 5 units of product PROD-001"
# "Insufficient stock for product: PROD-002 | Available: 200 | Required: 300"
# "Rolled back 5 units of product PROD-001"  ← Atomic rollback
# "Stock REJECTED for order: ... | Reason: Product PROD-002 insufficient..."

# Verify in H2 databases:
# payment-service: CUST-1 balance unchanged (reservation was rolled back)
# stock-service: All products back to original quantities
```

### 4.3 Test Scenario 3: Payment Reject → ROLLBACK Stock (Insufficient Balance)

```bash
# Create expensive multi-item order (customer doesn't have enough balance)
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-1",
    "items": [
      {
        "productId": "PROD-010",
        "productName": "Chair",
        "quantity": 15,
        "price": 399.99
      },
      {
        "productId": "PROD-004",
        "productName": "Monitor",
        "quantity": 10,
        "price": 299.99
      }
    ]
  }'

# Expected flow:
# 1. payment-service: Tries to reserve $8,999.75 from CUST-1
#    - CUST-1 only has $10,000 available
#    - But $1,139.96 might be reserved from previous test
#    - Insufficient balance → REJECT
# 2. stock-service: Reserves 15 chairs + 10 monitors → ACCEPT (all available)
# 3. Kafka Streams: ROLLBACK with source=PAYMENT
# 4. payment-service: No action (nothing to rollback - reservation never succeeded)
# 5. stock-service: Rollback (return 15 chairs + 10 monitors to inventory)

# Verify in payment-service logs:
# "Payment REJECTED for order: ... | Insufficient balance"

# Verify in stock-service logs:
# "Stock RESERVED for order: ... | All 2 items available"
# "ROLLBACK triggered by payment failure - returning stock to inventory"
# "Stock ROLLED BACK for order: ... | All 2 items returned to inventory"
# "Rolled back 15 units of product PROD-010 | Available now: 20"
# "Rolled back 10 units of product PROD-004 | Available now: 30"

# Verify in H2 databases:
# payment-service: CUST-1 balance unchanged (reservation failed)
# stock-service: All products back to original quantities

# Alternative test with fresh customer (balance known):
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-2",
    "items": [
      {
        "productId": "PROD-001",
        "productName": "Laptop",
        "quantity": 20,
        "price": 999.99
      }
    ]
  }'
# Total: $19,999.80 (exceeds CUST-2 balance of ~$10,000)
```

### 4.4 Test Scenario 4: Both Reject → REJECTED (No Compensation)

```bash
# Create order where both payment AND stock fail
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-1",
    "items": [
      {
        "productId": "PROD-001",
        "productName": "Laptop",
        "quantity": 100,
        "price": 999.99
      },
      {
        "productId": "PROD-002",
        "productName": "Mouse",
        "quantity": 500,
        "price": 29.99
      }
    ]
  }'

# Expected flow:
# 1. payment-service: Tries to reserve $114,994.00
#    - CUST-1 doesn't have this much → REJECT
# 2. stock-service: Tries to reserve 100 laptops + 500 mice
#    - Only 50 laptops available → REJECT (atomic rollback)
# 3. Kafka Streams: Both failed → REJECTED (no compensation needed)
# 4. payment-service: No action (nothing to rollback)
# 5. stock-service: No action (atomic rollback already happened)

# Verify in logs:
# order-service: "Order <id> REJECTED: both payment and stock failed"
# payment-service: "Payment REJECTED for order: ..."
# stock-service: "Stock REJECTED for order: ..."
# Neither service receives compensation request

# Verify in H2 databases:
# All balances and stock quantities unchanged
```

### 4.5 Verify State Store

```bash
# Query Kafka Streams state store
curl http://localhost:8081/api/order-state/{orderId}

# Expected: FinalDecisionEvent with status + source
# Example response:
{
  "orderId": "abc-123",
  "customerId": "CUST-1",
  "items": [...],
  "status": "ROLLBACK",
  "reason": "Product PROD-002 insufficient (available: 200, required: 300)",
  "source": "STOCK"
}
```

### 4.6 Verify Join Window (Advanced Test)

Test that events arriving >10 seconds apart don't join:

```bash
# Stop stock-service (simulate slow service)
kill $(pgrep -f stock-service)

# Create order (only payment will respond)
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-3",
    "items": [{
      "productId": "PROD-005",
      "productName": "Headphones",
      "quantity": 1,
      "price": 149.99
    }]
  }'

# Wait 15 seconds (beyond 10-second join window)
sleep 15

# Start stock-service (will publish stock event now)
cd stock-service && mvn spring-boot:run &

# Expected behavior:
# - payment-service publishes PaymentProcessedEvent immediately
# - Kafka Streams buffers payment event, waiting for stock event
# - After 10 seconds: join window expires, payment event dropped
# - stock-service starts and publishes StockProcessedEvent (too late)
# - No join occurs, no FinalDecisionEvent published
# - Order stays in PENDING state

# Verify in order-service logs:
# - "Received payment event: orderId=... status=ACCEPT"
# - No "Final decision for order ..." message (join failed)

# Verify in Kafka UI:
# - payment-events topic: PaymentProcessedEvent present
# - stock-events topic: StockProcessedEvent present
# - order-events topic: No FinalDecisionEvent (join didn't happen)

# Check order state:
curl http://localhost:8081/api/orders/{orderId}
# Status should be PENDING (never got confirmed/rejected)

# Real-world implication:
# - Order is "stuck" in PENDING state
# - Payment reserved but never confirmed/rolled back
# - Stock reserved (if stock responded) but never confirmed/rolled back
# - Requires monitoring and manual intervention (DLQ topic in production)
```

---

## Part 5: Verification Checklist

### Code Quality
- [ ] All 3 services compile: `mvn clean compile`
- [ ] No IDE errors
- [ ] Lombok annotations processed
- [ ] Jackson dependencies present

### Kafka Streams
- [ ] @EnableKafkaStreams annotation present
- [ ] OrderStreamsTopology bean created
- [ ] Join window configured (10 seconds)
- [ ] Serdes configured correctly
- [ ] State store created (RocksDB)

### 3-Way Decision Logic
- [ ] ACCEPT + ACCEPT → CONFIRMED
- [ ] REJECT + REJECT → REJECTED
- [ ] ACCEPT + REJECT → ROLLBACK (with source)
- [ ] REJECT + ACCEPT → ROLLBACK (with source)

### Compensation Logic
- [ ] payment-service checks source field
- [ ] stock-service checks source field
- [ ] Only failed service gets rolled back
- [ ] Successful service ignores rollback

### State Management
- [ ] RocksDB store created in /tmp/kafka-streams/
- [ ] KTable queryable via REST API
- [ ] State survives service restart (rehydrated from topic)

### Testing
- [ ] Happy path works (both accept)
- [ ] Rollback works (one rejects)
- [ ] Idempotency preserved
- [ ] Databases reflect correct state
- [ ] Kafka UI shows all events

---

## Part 6: Learning Verification

Answer these questions to verify understanding:

1. **What is a KStream vs KTable?**
   - KStream: Event stream (append-only log)
   - KTable: Changelog stream (latest value per key)

2. **Why use stream joins instead of @KafkaListener?**
   - Automatic correlation by key
   - Time windowing (handles late arrivals)
   - Fault tolerance (state survives restarts)
   - Scalability (partitioned state)

3. **What happens if payment arrives but stock doesn't (within 10s)?**
   - No join emitted (payment event buffered)
   - After 10s: window expires, payment event dropped
   - Order stays in PENDING state

4. **Why track source field in ROLLBACK events?**
   - Prevents unnecessary compensation
   - Only rollback the service that succeeded
   - Example: Payment OK, Stock FAIL → only rollback payment

5. **Where is order state stored?**
   - Phase 4: ConcurrentHashMap (in-memory)
   - Phase 5: RocksDB state store (persistent, queryable)

---

## Troubleshooting

### Issue: Kafka Streams doesn't start
**Symptoms:** No "Kafka Streams started" log message

**Solutions:**
1. Check `application-id` is unique per service
2. Verify state-dir is writable: `/tmp/kafka-streams/`
3. Check topics exist: `orders`, `payment-events`, `stock-events`
4. Reset state: `rm -rf /tmp/kafka-streams/order-service`

### Issue: Join not producing output
**Symptoms:** Payment + stock events arrive, but no FinalDecisionEvent

**Solutions:**
1. Verify both events use same orderId as key
2. Check events arrive within 10-second window
3. Look for errors in order-service logs
4. Verify serdes are configured (JsonSerde)

### Issue: "Unable to find store"
**Symptoms:** Interactive queries fail with store not found

**Solutions:**
1. Wait for Kafka Streams to initialize (~10-30 seconds)
2. Verify KTable bean is created
3. Check state store name matches: "order-state-store"
4. Ensure topics have been created and consumed

### Issue: Duplicate rollbacks
**Symptoms:** payment-service rolls back even though it rejected

**Solutions:**
1. Check source field logic in DecisionEventConsumer
2. Verify: only rollback if source != your service name
3. Add idempotency checks (processedDecisions Set)

---

## Success Criteria

You've completed Phase 5 when:

✅ stock-service exists and handles reserve/confirm/rollback  
✅ order-service uses Kafka Streams (no @KafkaListener orchestration)  
✅ Stream join works (payment ⋈ stock within 10s window)  
✅ 3-way decision matrix implemented  
✅ ROLLBACK compensation works correctly  
✅ RocksDB state store persists order decisions  
✅ Interactive queries work via REST API  
✅ All 3 test scenarios pass  
✅ State survives service restarts  
✅ Code is clean and documented  

---

## Next Steps (Phase 6)

After completing Phase 5:

1. **Exactly-Once Semantics**: Configure Kafka Streams transactions
2. **Error Handling**: Dead letter queues for join failures
3. **Monitoring**: Kafka Streams metrics + Grafana dashboards
4. **Testing**: Testcontainers integration tests
5. **Scaling**: Multi-instance deployment with state store rebalancing

---

## References

- **Kafka Streams Docs**: https://kafka.apache.org/documentation/streams/
- **Spring Kafka Streams**: https://docs.spring.io/spring-kafka/reference/streams.html
- **Reference Repo**: https://github.com/piomin/sample-spring-kafka-microservices
- **Learning Doc**: [docs/03-kafka/09-kafka-streams-fundamentals.md](../docs/03-kafka/09-kafka-streams-fundamentals.md)

---

**Estimated Total Time:** 2-3 weeks (12-15 working days)

**Ready to start?** Begin with Part 1: Add Stock Service!
