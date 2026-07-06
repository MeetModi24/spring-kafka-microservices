# Task 04: Implement SAGA Orchestration

> **Current Status:** 🎯 READY TO START  
> **Phase:** 4 of 6  
> **Duration:** 2-3 weeks  
> **Prerequisites:** ✅ Phase 3 complete (payment-service with RESERVE phase)

---

## 🎯 Phase 4 Goals

Complete the **SAGA orchestration pattern** by implementing:

1. **Part A: order-service orchestrator** - Consumes payment responses and makes final decisions
2. **Part B: payment-service completion** - Handles CONFIRM/ROLLBACK based on orchestrator decisions

This closes the SAGA loop and implements distributed transaction coordination without a distributed transaction coordinator.

**Architecture Pattern:** Choreography-based SAGA (decentralized coordination via events)

---

## 📊 SAGA Pattern Overview

### What is SAGA?

A **SAGA** is a pattern for managing distributed transactions across microservices without using two-phase commit (2PC). Instead of locking resources across services, each service:

1. **Reserves** resources tentatively
2. **Publishes** its decision (ACCEPT/REJECT)
3. **Waits** for the orchestrator's final decision
4. **Commits** (if all services accepted) or **Compensates** (if any service rejected)

### SAGA Choreography vs Orchestration

| Pattern | Description | Our Implementation |
|---------|-------------|-------------------|
| **Choreography** | Each service listens to events and decides what to do next | ✅ YES - Services react to events independently |
| **Orchestration** | Central orchestrator tells services what to do | ⚠️ Hybrid - order-service acts as coordinator but uses events (not commands) |

We implement a **choreography-based SAGA with implicit orchestration**: order-service coordinates by publishing events, but services make autonomous decisions.

---

## 📐 Topic Architecture: 2 Topics (Not 3!)

**IMPORTANT:** This implementation uses **2 Kafka topics**, matching the reference repository pattern.

### Topic Strategy

| Topic | Events | Publishers | Consumers |
|-------|--------|-----------|-----------|
| **order-events** | OrderCreatedEvent (status=NEW)<br>FinalDecisionEvent (status=CONFIRMED/ROLLBACK) | order-service | payment-service (2 consumer groups) |
| **payment-events** | PaymentProcessedEvent (status=ACCEPT/REJECT) | payment-service | order-service |

### Why 2 Topics Instead of 3?

**Common Mistake:** Creating separate topics like:
- `order-events` (initial orders)
- `payment-events` (payment responses)
- `order-decision-events` (final decisions) ❌ NOT NEEDED

**Reference Repository Pattern:** 
- `orders` topic carries BOTH OrderCreatedEvent AND FinalDecisionEvent
- Consumers discriminate by event status field
- payment-service has TWO consumer groups on the same topic

### Consumer Group Strategy

**payment-service has TWO @KafkaListener methods on "order-events":**

1. **Consumer Group: "payment-service-group"**
   - Filters: `status == NEW`
   - Action: Reserve funds, publish PaymentProcessedEvent

2. **Consumer Group: "payment-decision-group"**
   - Filters: `status == CONFIRMED || status == ROLLBACK`
   - Action: Confirm or rollback funds

**Why Different Consumer Groups?**
- Ensures both listeners receive the same messages
- Each group maintains its own offset
- Parallel processing of different event types on same topic

---

## 🏗️ Architecture: Complete Event Flow

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                         PHASE 4: COMPLETE SAGA FLOW                           │
└──────────────────────────────────────────────────────────────────────────────┘

Step 1: Order Creation (Already Implemented - Phase 2)
──────────────────────────────────────────────────────
  Client
    │
    │ POST /api/orders
    ▼
  order-service
    │
    │ 1. Create Order (status=PENDING)
    │ 2. Store in orderStateStore (Map)
    │ 3. Publish OrderCreatedEvent
    │
    └──► Kafka topic: "order-events"


Step 2: Payment Validation (Already Implemented - Phase 3)
───────────────────────────────────────────────────────────
  Kafka topic: "order-events"
    │
    │ OrderCreatedEvent
    ▼
  payment-service
    │
    │ 1. Find customer
    │ 2. Check balance
    │ 3. Reserve funds (available → reserved)
    │ 4. Save customer
    │ 5. Determine status: ACCEPT or REJECT
    │ 6. Publish PaymentProcessedEvent
    │
    └──► Kafka topic: "payment-events"


Step 3: Order Orchestration Decision (NEW - Part A)
────────────────────────────────────────────────────
  Kafka topic: "payment-events"
    │
    │ PaymentProcessedEvent
    ▼
  order-service (@KafkaListener)
    │
    │ 1. Retrieve order from orderStateStore
    │ 2. Update order with payment status
    │ 3. Determine final status:
    │    ├─ ACCEPT → status = CONFIRMED
    │    └─ REJECT → status = REJECTED
    │ 4. Update orderStateStore
    │ 5. Publish FinalDecisionEvent
    │
    └──► Kafka topic: "order-events"  ← SAME TOPIC as step 1!


Step 4: Payment Commit or Rollback (NEW - Part B)
──────────────────────────────────────────────────
  Kafka topic: "order-events"  ← SAME TOPIC, different consumer group
    │
    │ FinalDecisionEvent
    ▼
  payment-service (@KafkaListener - second listener)
    │
    │ 1. Find customer
    │ 2. Check decision status:
    │    ├─ CONFIRMED → customer.confirm(amount) 
    │    │             (reserved → deducted)
    │    └─ REJECTED → customer.rollback(amount)
    │                  (reserved → available)
    │ 3. Save customer
    │ 4. Log completion
    │
    └──► END (Transaction complete)


Step 5: Query Final State
──────────────────────────
  Client
    │
    │ GET /api/orders/{orderId}
    ▼
  order-service
    │
    │ Retrieve from orderStateStore
    │
    └──► Return OrderResponse with final status
```

---

## 🔧 Part A: Order Service Orchestrator

### What We're Building

Add these components to order-service:

1. **OrderStateStore** - In-memory state tracking
2. **PaymentEventConsumer** - Listens to payment responses
3. **OrderOrchestrationService** - Decision logic
4. **FinalDecisionEvent** - DTO for final decision

---

### A1: Create Order State Store (30 mins)

**Purpose:** Track order state during SAGA execution (alternative to Kafka Streams state store for simplicity)

**Create:** `order-service/src/main/java/com/example/orderservice/state/OrderStateStore.java`

```java
package com.example.orderservice.state;

import com.example.orderservice.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory state store for tracking orders during SAGA execution
 * 
 * DESIGN NOTE: In production, use Kafka Streams state store or Redis.
 * This implementation uses ConcurrentHashMap for simplicity.
 * 
 * SAGA STATES:
 * - PENDING: Order created, waiting for payment response
 * - CONFIRMED: Payment accepted, order successful
 * - REJECTED: Payment rejected, order failed
 * 
 * Thread-safety: ConcurrentHashMap ensures safe concurrent access
 */
@Component
@Slf4j
public class OrderStateStore {
    
    private final Map<String, Order> orderState = new ConcurrentHashMap<>();
    
    /**
     * Store order when created (PENDING state)
     * 
     * @param order Order to track
     */
    public void put(String orderId, Order order) {
        orderState.put(orderId, order);
        log.debug("Stored order state: orderId={}, status={}", 
            orderId, order.getStatus());
    }
    
    /**
     * Retrieve order by ID
     * 
     * @param orderId Order ID
     * @return Optional<Order> (empty if not found)
     */
    public Optional<Order> get(String orderId) {
        return Optional.ofNullable(orderState.get(orderId));
    }
    
    /**
     * Update order state
     * 
     * @param orderId Order ID
     * @param order Updated order
     */
    public void update(String orderId, Order order) {
        orderState.put(orderId, order);
        log.debug("Updated order state: orderId={}, status={}", 
            orderId, order.getStatus());
    }
    
    /**
     * Remove order from state (cleanup after completion)
     * 
     * @param orderId Order ID
     */
    public void remove(String orderId) {
        Order removed = orderState.remove(orderId);
        if (removed != null) {
            log.debug("Removed order state: orderId={}", orderId);
        }
    }
    
    /**
     * Get all orders (for debugging/monitoring)
     * 
     * @return Map of all tracked orders
     */
    public Map<String, Order> getAll() {
        return new ConcurrentHashMap<>(orderState);
    }
    
    /**
     * Get count of tracked orders
     * 
     * @return Number of orders in state store
     */
    public int size() {
        return orderState.size();
    }
}
```

---

### A2: Create Final Decision Event DTO (30 mins)

**Purpose:** Communicate final SAGA decision to participants

**Create:** `order-service/src/main/java/com/example/orderservice/event/FinalDecisionEvent.java`

```java
package com.example.orderservice.event;

import lombok.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Final decision event published by order-service (SAGA orchestrator)
 * 
 * This event tells payment-service (and future stock-service) whether to:
 * - CONFIRM: Commit the reservation (deduct reserved funds)
 * - ROLLBACK: Compensate the reservation (return reserved funds)
 * 
 * SAGA PATTERN: This is the "decision" phase after all participants respond
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FinalDecisionEvent {
    
    /**
     * Order ID (correlation key)
     */
    private String orderId;
    
    /**
     * Customer ID
     */
    private String customerId;
    
    /**
     * Order total amount
     */
    private BigDecimal amount;
    
    /**
     * Final decision: CONFIRMED or REJECTED
     */
    private DecisionStatus status;
    
    /**
     * Reason for rejection (if status=REJECTED)
     */
    private String reason;
    
    /**
     * Decision timestamp
     */
    private LocalDateTime decidedAt;
    
    /**
     * Final decision status
     */
    public enum DecisionStatus {
        /**
         * All services accepted - commit reservations
         */
        CONFIRMED,
        
        /**
         * At least one service rejected - rollback all reservations
         */
        REJECTED
    }
}
```

---

### A3: Create Payment Event Consumer (1 hour)

**Purpose:** Listen to payment responses and trigger orchestration

**Create:** `order-service/src/main/java/com/example/orderservice/consumer/PaymentEventConsumer.java`

```java
package com.example.orderservice.consumer;

import com.example.orderservice.event.PaymentProcessedEvent;
import com.example.orderservice.service.OrderOrchestrationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer for payment-service responses
 * 
 * SAGA PATTERN: Orchestrator listens to participant responses
 * 
 * This consumer:
 * 1. Receives PaymentProcessedEvent from payment-service
 * 2. Delegates decision logic to OrderOrchestrationService
 * 3. Orchestration service publishes final decision
 * 
 * ERROR HANDLING:
 * - If processing fails, exception prevents offset commit
 * - Kafka will re-deliver message (at-least-once delivery)
 * - Idempotency handling in orchestration service prevents duplicate decisions
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentEventConsumer {
    
    private final OrderOrchestrationService orchestrationService;
    
    /**
     * Consume payment response events
     * 
     * @param event PaymentProcessedEvent from payment-service
     */
    @KafkaListener(
        topics = "payment-events",
        groupId = "order-service-orchestrator-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumePaymentEvent(PaymentProcessedEvent event) {
        log.info("Received PaymentProcessedEvent: orderId={}, status={}", 
            event.getOrderId(), event.getStatus());
        
        try {
            // Delegate to orchestration service for decision logic
            orchestrationService.handlePaymentResponse(event);
            
            log.info("Successfully processed payment response for order: {}", 
                event.getOrderId());
            
        } catch (Exception e) {
            log.error("Failed to process payment response for order: {}", 
                event.getOrderId(), e);
            
            // Re-throw to prevent offset commit (enables retry)
            throw new RuntimeException("Payment response processing failed", e);
        }
    }
}
```

---

### A4: Create Payment Processed Event DTO (30 mins)

**Purpose:** Deserialize payment-service responses

**Create:** `order-service/src/main/java/com/example/orderservice/event/PaymentProcessedEvent.java`

```java
package com.example.orderservice.event;

import lombok.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Payment response event from payment-service
 * 
 * MUST match payment-service's PaymentProcessedEvent structure
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PaymentProcessedEvent {
    
    private String orderId;
    private String customerId;
    private BigDecimal amount;
    private PaymentStatus status;
    private String reason;  // Populated if status=REJECT
    private LocalDateTime processedAt;
    
    public enum PaymentStatus {
        ACCEPT,
        REJECT
    }
}
```

---

### A5: Create Order Orchestration Service (1.5 hours)

**Purpose:** Implement SAGA orchestration decision logic

**Create:** `order-service/src/main/java/com/example/orderservice/service/OrderOrchestrationService.java`

```java
package com.example.orderservice.service;

import com.example.orderservice.event.FinalDecisionEvent;
import com.example.orderservice.event.PaymentProcessedEvent;
import com.example.orderservice.model.Order;
import com.example.orderservice.model.OrderStatus;
import com.example.orderservice.state.OrderStateStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * SAGA Orchestration Service
 * 
 * RESPONSIBILITIES:
 * 1. Receive payment validation responses
 * 2. Determine final order status (CONFIRMED or REJECTED)
 * 3. Publish final decision to payment-service
 * 4. Update order state
 * 
 * SAGA PATTERN: Choreography-based orchestrator
 * - Does NOT send commands to services
 * - Publishes events that services react to autonomously
 * 
 * IDEMPOTENCY:
 * - Tracks processed decisions to prevent duplicate final events
 * - Critical for at-least-once Kafka delivery semantics
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class OrderOrchestrationService {
    
    private static final String DECISION_TOPIC = "order-events";  // Same topic as OrderCreatedEvent
    
    private final OrderStateStore orderStateStore;
    private final KafkaTemplate<String, FinalDecisionEvent> kafkaTemplate;
    
    /**
     * Idempotency tracking: Store order IDs that have received final decisions
     * 
     * PRODUCTION NOTE: Use Redis with TTL or database table
     */
    private final Set<String> processedDecisions = new HashSet<>();
    
    /**
     * Handle payment response and make final decision
     * 
     * SAGA DECISION LOGIC:
     * - ACCEPT → status = CONFIRMED
     * - REJECT → status = REJECTED
     * 
     * FUTURE EXPANSION (Phase 5):
     * - Wait for BOTH payment AND stock responses
     * - Only CONFIRM if BOTH accept
     * - ROLLBACK if one accepts and one rejects
     * 
     * @param paymentEvent Payment response event
     */
    public void handlePaymentResponse(PaymentProcessedEvent paymentEvent) {
        String orderId = paymentEvent.getOrderId();
        
        log.info("Orchestrating decision for order: {}", orderId);
        
        // IDEMPOTENCY: Check if decision already made
        if (processedDecisions.contains(orderId)) {
            log.warn("Order {} already processed, skipping duplicate", orderId);
            return;
        }
        
        // Retrieve order from state store
        Optional<Order> orderOpt = orderStateStore.get(orderId);
        
        if (orderOpt.isEmpty()) {
            log.error("Order not found in state store: {}", orderId);
            throw new IllegalStateException("Order not found: " + orderId);
        }
        
        Order order = orderOpt.get();
        
        // SAGA DECISION LOGIC (Phase 4: Payment only)
        FinalDecisionEvent decision;
        
        if (paymentEvent.getStatus() == PaymentProcessedEvent.PaymentStatus.ACCEPT) {
            // Payment accepted → CONFIRM order
            log.info("Payment ACCEPTED for order: {} → Decision: CONFIRMED", orderId);
            
            order.setStatus(OrderStatus.CONFIRMED);
            order.setUpdatedAt(LocalDateTime.now());
            
            decision = buildDecision(order, FinalDecisionEvent.DecisionStatus.CONFIRMED, null);
            
        } else {
            // Payment rejected → REJECT order
            log.info("Payment REJECTED for order: {} → Decision: REJECTED | Reason: {}", 
                orderId, paymentEvent.getReason());
            
            order.setStatus(OrderStatus.REJECTED);
            order.setUpdatedAt(LocalDateTime.now());
            
            decision = buildDecision(order, FinalDecisionEvent.DecisionStatus.REJECTED, 
                paymentEvent.getReason());
        }
        
        // Update order state
        orderStateStore.update(orderId, order);
        
        // Mark as processed (idempotency)
        processedDecisions.add(orderId);
        
        // Publish final decision
        publishDecision(decision);
        
        log.info("Order orchestration complete: orderId={}, finalStatus={}", 
            orderId, order.getStatus());
    }
    
    /**
     * Build FinalDecisionEvent from order and decision status
     */
    private FinalDecisionEvent buildDecision(Order order, 
                                             FinalDecisionEvent.DecisionStatus status, 
                                             String reason) {
        return FinalDecisionEvent.builder()
            .orderId(order.getOrderId())
            .customerId(order.getCustomerId())
            .amount(order.getTotalAmount())
            .status(status)
            .reason(reason)
            .decidedAt(LocalDateTime.now())
            .build();
    }
    
    /**
     * Publish final decision to Kafka
     * 
     * SAGA PATTERN: Orchestrator publishes decision, participants react
     */
    private void publishDecision(FinalDecisionEvent decision) {
        kafkaTemplate.send(DECISION_TOPIC, decision.getOrderId(), decision)
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to publish decision for order: {}", 
                        decision.getOrderId(), ex);
                } else {
                    log.info("Published FinalDecisionEvent: orderId={}, status={}, offset={}", 
                        decision.getOrderId(), 
                        decision.getStatus(),
                        result.getRecordMetadata().offset());
                }
            });
    }
}
```

---

### A6: Update Order Service to Store State (30 mins)

**Update:** `order-service/src/main/java/com/example/orderservice/service/OrderService.java`

**Add state store injection and update createOrder method:**

```java
// Add to class fields
private final OrderStateStore orderStateStore;

// Update createOrder method to store state
public OrderResponse createOrder(CreateOrderRequest request) {
    // ... existing validation and creation logic ...
    
    Order savedOrder = orderRepository.save(order);
    
    // NEW: Store order in state store for orchestration
    orderStateStore.put(savedOrder.getOrderId(), savedOrder);
    
    // Publish event
    orderEventProducer.publishOrderCreated(savedOrder);
    
    return mapToResponse(savedOrder);
}
```

---

### A7: Configure Kafka Consumer in application.yml (15 mins)

**Update:** `order-service/src/main/resources/application.yml`

**Add consumer configuration:**

```yaml
spring:
  kafka:
    # Existing producer config...
    
    # NEW: Consumer configuration for payment-events
    consumer:
      group-id: order-service-orchestrator-group
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "*"
        spring.json.type.mapping: >
          paymentProcessed:com.example.orderservice.event.PaymentProcessedEvent
    
    # NEW: Producer configuration for decision events
    producer:
      # Existing config...
      properties:
        spring.json.type.mapping: >
          orderCreated:com.example.orderservice.event.OrderCreatedEvent,
          finalDecision:com.example.orderservice.event.FinalDecisionEvent
```

---

## 🔧 Part B: Payment Service Completion

### What We're Building

Add these components to payment-service:

1. **DecisionEventConsumer** - Second Kafka listener for final decisions
2. **Update PaymentService** - Add confirm() and rollback() methods
3. **Idempotency handling** - Prevent duplicate processing

---

### B1: Create Decision Event DTO (15 mins)

**Purpose:** Deserialize final decisions from order-service

**Create:** `payment-service/src/main/java/com/example/paymentservice/event/FinalDecisionEvent.java`

```java
package com.example.paymentservice.event;

import lombok.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Final decision event from order-service
 * 
 * MUST match order-service's FinalDecisionEvent structure
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FinalDecisionEvent {
    
    private String orderId;
    private String customerId;
    private BigDecimal amount;
    private DecisionStatus status;
    private String reason;
    private LocalDateTime decidedAt;
    
    public enum DecisionStatus {
        CONFIRMED,
        REJECTED
    }
}
```

---

### B2: Create Decision Event Consumer (45 mins)

**Purpose:** Listen to final decisions and complete SAGA

**Create:** `payment-service/src/main/java/com/example/paymentservice/consumer/DecisionEventConsumer.java`

```java
package com.example.paymentservice.consumer;

import com.example.paymentservice.event.FinalDecisionEvent;
import com.example.paymentservice.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer for order-service final decisions
 * 
 * SAGA PATTERN: Participant receives final decision
 * 
 * This consumer:
 * 1. Receives FinalDecisionEvent from order-service
 * 2. Commits reservation if CONFIRMED
 * 3. Rolls back reservation if REJECTED
 * 
 * IDEMPOTENCY:
 * - Uses Set to track processed decisions
 * - Prevents double-commit or double-rollback
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class DecisionEventConsumer {
    
    private final PaymentService paymentService;
    
    /**
     * Consume final decision events
     * 
     * @param event FinalDecisionEvent from order-service
     */
    @KafkaListener(
        topics = "order-events",
        groupId = "payment-decision-group",  // Different group from payment-service-group
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeDecisionEvent(FinalDecisionEvent event) {
        log.info("Received FinalDecisionEvent: orderId={}, status={}", 
            event.getOrderId(), event.getStatus());
        
        try {
            // Delegate to service for commit/rollback logic
            paymentService.processFinalDecision(event);
            
            log.info("Successfully processed final decision for order: {}", 
                event.getOrderId());
            
        } catch (Exception e) {
            log.error("Failed to process final decision for order: {}", 
                event.getOrderId(), e);
            
            // Re-throw to prevent offset commit (enables retry)
            throw new RuntimeException("Decision processing failed", e);
        }
    }
}
```

---

### B3: Update Payment Service with Confirm/Rollback (1 hour)

**Update:** `payment-service/src/main/java/com/example/paymentservice/service/PaymentService.java`

**Add these fields and methods:**

```java
/**
 * Idempotency tracking: Store order IDs that have been confirmed/rolled back
 * 
 * PRODUCTION NOTE: Use Redis with TTL or database table
 */
private final Set<String> processedDecisions = new HashSet<>();

/**
 * Process final decision from order-service (SAGA Confirm/Compensate phase)
 * 
 * SAGA PATTERN:
 * - CONFIRMED → Commit reservation (deduct from reserved)
 * - REJECTED → Rollback reservation (return to available)
 * 
 * @param event FinalDecisionEvent from order-service
 */
@Transactional
public void processFinalDecision(FinalDecisionEvent event) {
    String orderId = event.getOrderId();
    
    log.info("Processing final decision for order: {}", orderId);
    
    // IDEMPOTENCY: Check if already processed
    if (processedDecisions.contains(orderId)) {
        log.warn("Order {} already finalized, skipping duplicate", orderId);
        return;
    }
    
    // Find customer
    Optional<Customer> customerOpt = customerRepository.findByCustomerId(event.getCustomerId());
    
    if (customerOpt.isEmpty()) {
        log.error("Customer not found during finalization: {}", event.getCustomerId());
        throw new IllegalStateException("Customer not found: " + event.getCustomerId());
    }
    
    Customer customer = customerOpt.get();
    Integer amountCents = event.getAmount().multiply(new java.math.BigDecimal("100")).intValue();
    
    // Process based on decision status
    if (event.getStatus() == FinalDecisionEvent.DecisionStatus.CONFIRMED) {
        // COMMIT: Deduct from reserved (transaction complete)
        confirmPayment(customer, amountCents, orderId);
        
    } else {
        // ROLLBACK: Return to available (compensate)
        rollbackPayment(customer, amountCents, orderId, event.getReason());
    }
    
    // Mark as processed
    processedDecisions.add(orderId);
    
    log.info("Final decision processing complete: orderId={}, status={}", 
        orderId, event.getStatus());
}

/**
 * Confirm payment - commit reservation (SAGA Confirm phase)
 * 
 * BUSINESS LOGIC:
 * - amountReserved -= amount (release reservation)
 * - Funds are now permanently deducted
 * 
 * @param customer Customer entity
 * @param amountCents Amount in cents
 * @param orderId Order ID (for logging)
 */
private void confirmPayment(Customer customer, Integer amountCents, String orderId) {
    log.info("Confirming payment for order: {} | Customer: {} | Amount: ${}", 
        orderId, customer.getCustomerId(), amountCents / 100.0);
    
    // Verify reserved amount
    if (customer.getAmountReserved() < amountCents) {
        log.warn("Reserved amount insufficient for order: {} | Reserved: {} | Required: {}", 
            orderId, customer.getAmountReserved(), amountCents);
        // Proceed anyway (best effort)
    }
    
    // Commit: Deduct from reserved
    customer.confirm(amountCents);
    
    // Save updated customer
    customerRepository.save(customer);
    
    log.info("Payment CONFIRMED for order: {} | New balance: ${} | Reserved: ${}", 
        orderId, 
        customer.getAmountAvailable() / 100.0, 
        customer.getAmountReserved() / 100.0);
}

/**
 * Rollback payment - compensate reservation (SAGA Compensate phase)
 * 
 * BUSINESS LOGIC:
 * - amountReserved -= amount (release reservation)
 * - amountAvailable += amount (return funds)
 * 
 * @param customer Customer entity
 * @param amountCents Amount in cents
 * @param orderId Order ID (for logging)
 * @param reason Rejection reason
 */
private void rollbackPayment(Customer customer, Integer amountCents, String orderId, String reason) {
    log.info("Rolling back payment for order: {} | Reason: {} | Customer: {} | Amount: ${}", 
        orderId, reason, customer.getCustomerId(), amountCents / 100.0);
    
    // Compensate: Return reserved funds to available
    customer.rollback(amountCents);
    
    // Save updated customer
    customerRepository.save(customer);
    
    log.info("Payment ROLLED BACK for order: {} | New balance: ${} | Reserved: ${}", 
        orderId, 
        customer.getAmountAvailable() / 100.0, 
        customer.getAmountReserved() / 100.0);
}
```

---

### B4: Update Payment Service Config (15 mins)

**Update:** `payment-service/src/main/resources/application.yml`

**Add type mapping for FinalDecisionEvent:**

```yaml
spring:
  kafka:
    consumer:
      properties:
        spring.json.type.mapping: >
          orderCreated:com.example.paymentservice.event.OrderCreatedEvent,
          finalDecision:com.example.paymentservice.event.FinalDecisionEvent
```

---

## 🧪 Testing Strategy

### Test Scenario 1: Happy Path (Payment Accepted)

**Objective:** Verify complete SAGA flow with successful payment

**Steps:**

```bash
# 1. Start all services
docker-compose up -d  # Kafka + Kafka UI
cd order-service && mvn spring-boot:run  # Terminal 1
cd payment-service && mvn spring-boot:run  # Terminal 2

# 2. Create order with valid customer
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

**Expected Logs:**

**order-service:**
```
[OrderService] Created order: orderId=abc-123, status=PENDING
[OrderStateStore] Stored order state: orderId=abc-123, status=PENDING
[OrderEventProducer] Publishing OrderCreatedEvent: orderId=abc-123
[PaymentEventConsumer] Received PaymentProcessedEvent: orderId=abc-123, status=ACCEPT
[OrderOrchestrationService] Payment ACCEPTED → Decision: CONFIRMED
[OrderOrchestrationService] Published FinalDecisionEvent: status=CONFIRMED
```

**payment-service:**
```
[OrderEventConsumer] Received OrderCreatedEvent: orderId=abc-123
[PaymentService] Payment ACCEPTED | Reserved: $999.99
[PaymentService] Published PaymentProcessedEvent: ACCEPT
[DecisionEventConsumer] Received FinalDecisionEvent: status=CONFIRMED
[PaymentService] Confirming payment for order: abc-123
[PaymentService] Payment CONFIRMED | New balance: $1500.01
```

**Verification:**

```bash
# H2 Console: http://localhost:8082/h2-console
SELECT * FROM customers WHERE customer_id = 'CUST-1';

# Expected:
# AMOUNT_AVAILABLE: 150001 (was 250000, reserved 99999, now confirmed)
# AMOUNT_RESERVED: 0 (was 99999, now deducted)
```

**Kafka UI Verification:**

```bash
open http://localhost:8080

# Topics to check (only 2 topics!):
# 1. order-events → BOTH OrderCreatedEvent AND FinalDecisionEvent
#    - First message: status=NEW (OrderCreatedEvent)
#    - Second message: status=CONFIRMED (FinalDecisionEvent)
# 2. payment-events → PaymentProcessedEvent (status=ACCEPT)
```

---

### Test Scenario 2: Payment Rejection

**Objective:** Verify SAGA rollback when payment is rejected

**Steps:**

```bash
# Create order exceeding customer balance
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-1",
    "items": [{
      "productId": "PROD-002",
      "productName": "Server",
      "quantity": 1,
      "price": 9999.99
    }]
  }'
```

**Expected Logs:**

**payment-service:**
```
[PaymentService] Payment REJECTED | Insufficient balance
[PaymentService] Published PaymentProcessedEvent: REJECT | Reason: Insufficient balance
[DecisionEventConsumer] Received FinalDecisionEvent: status=REJECTED
[PaymentService] Rolling back payment | Reason: Insufficient balance
[PaymentService] Payment ROLLED BACK | Funds returned to available
```

**Verification:**

```bash
# Customer balance should be UNCHANGED (nothing was reserved)
SELECT * FROM customers WHERE customer_id = 'CUST-1';

# Expected:
# AMOUNT_AVAILABLE: same as before
# AMOUNT_RESERVED: 0
```

---

### Test Scenario 3: Idempotency Check

**Objective:** Verify duplicate events don't cause double-processing

**Steps:**

```bash
# 1. Manually publish duplicate PaymentProcessedEvent to Kafka
# (Using Kafka UI or kafka-console-producer)

# 2. Check logs for idempotency message
```

**Expected Logs:**

```
[OrderOrchestrationService] Order abc-123 already processed, skipping duplicate
```

**Verification:**

- Order status updated only once
- Customer balance changed only once
- No duplicate FinalDecisionEvent published

---

### Test Scenario 4: Unknown Customer

**Objective:** Verify error handling for non-existent customer

**Steps:**

```bash
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-999",
    "items": [{
      "productId": "P1",
      "productName": "Item",
      "quantity": 1,
      "price": 10.00
    }]
  }'
```

**Expected Behavior:**

- Payment rejected with reason "Customer not found"
- Order status: REJECTED
- No database changes

---

### Test Scenario 5: Service Restart Recovery

**Objective:** Verify state recovery after service restart

**Steps:**

```bash
# 1. Create order
# 2. Stop payment-service BEFORE it processes payment
# 3. Restart payment-service
# 4. Verify order is processed (Kafka re-delivers message)
```

**Expected:**

- Payment processed after restart
- No data loss
- Order completes successfully

---

## 🐛 Troubleshooting Guide

### Issue 1: Order Not Found in State Store

**Symptom:**
```
ERROR [OrderOrchestrationService] Order not found in state store: abc-123
```

**Causes:**
1. Order service restarted (in-memory state lost)
2. OrderService didn't call `orderStateStore.put()`
3. Race condition (payment response arrived before order creation completed)

**Solutions:**
1. **Production:** Use Kafka Streams state store or Redis (persistent)
2. **Development:** Ensure `orderStateStore.put()` called in `OrderService.createOrder()`
3. **Race condition:** Add retry logic in `OrderOrchestrationService`

---

### Issue 2: Duplicate Decision Events

**Symptom:**
```
WARN [PaymentService] Order abc-123 already finalized, skipping duplicate
```

**Causes:**
1. Kafka consumer re-delivered message (normal behavior)
2. Manual replay of Kafka topic

**Solutions:**
- This is EXPECTED behavior (idempotency working correctly)
- Verify `processedDecisions` Set prevents double-processing
- In production: Use Redis Set with TTL

---

### Issue 3: Payment Confirmed But Reserved Amount Zero

**Symptom:**
```
WARN [PaymentService] Reserved amount insufficient for order: abc-123
```

**Causes:**
1. `reserve()` was never called (PaymentProcessedEvent missed)
2. Database reset between reserve and confirm
3. Customer record not saved after reserve

**Solutions:**
1. Check payment-service logs for "Payment ACCEPTED" message
2. Verify `customerRepository.save()` called after `reserve()`
3. Check transaction boundaries (`@Transactional`)

---

### Issue 4: Consumer Not Receiving Messages

**Symptom:**
```
# No logs from @KafkaListener
```

**Causes:**
1. Wrong topic name
2. Kafka consumer not started
3. Deserialization failure (JSON mismatch)
4. Consumer group offset already committed

**Solutions:**

```bash
# Check topics exist
kafka-topics --list --bootstrap-server localhost:9092

# Check messages in topic
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic payment-events --from-beginning

# Reset consumer group offset (development only!)
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group order-service-orchestrator-group --reset-offsets \
  --to-earliest --topic payment-events --execute

# Check application.yml type mappings match DTO package names
```

---

### Issue 5: JSON Deserialization Errors

**Symptom:**
```
ERROR [KafkaListener] Deserialization failed: Unrecognized field "orderId"
```

**Causes:**
1. Event DTO mismatch between services
2. Missing type mapping in application.yml
3. Wrong package name in type mapping

**Solutions:**

1. **Verify DTOs match exactly:**

```bash
# order-service
cat order-service/src/main/java/com/example/orderservice/event/PaymentProcessedEvent.java

# payment-service
cat payment-service/src/main/java/com/example/paymentservice/event/PaymentProcessedEvent.java

# Field names, types, and annotations must be IDENTICAL
```

2. **Check type mappings:**

```yaml
# order-service application.yml
spring.json.type.mapping: >
  paymentProcessed:com.example.orderservice.event.PaymentProcessedEvent

# payment-service application.yml
spring.json.type.mapping: >
  finalDecision:com.example.paymentservice.event.FinalDecisionEvent
```

3. **Enable detailed logging:**

```yaml
logging:
  level:
    org.springframework.kafka: DEBUG
    org.apache.kafka: DEBUG
```

---

## 📊 Monitoring & Observability

### Key Metrics to Track

**Order Service:**
- Orders in PENDING state (should be near zero)
- Orders in CONFIRMED vs REJECTED (success rate)
- Time from order creation to final decision (latency)
- PaymentEventConsumer lag (Kafka consumer lag)

**Payment Service:**
- Payment acceptance rate
- Payment rejection rate by reason
- Average customer balance
- Total reserved funds across all customers

### Health Checks

**Add to order-service:**

```bash
# Check state store size
curl http://localhost:8081/actuator/metrics/order.state.store.size
```

**Add to payment-service:**

```bash
# Check total reserved funds
curl http://localhost:8082/actuator/metrics/payment.reserved.total
```

---

## ✅ Completion Checklist

### Part A: Order Service (Orchestrator)

- [ ] `OrderStateStore` component created
- [ ] `FinalDecisionEvent` DTO created
- [ ] `PaymentProcessedEvent` DTO created
- [ ] `PaymentEventConsumer` with `@KafkaListener` implemented
- [ ] `OrderOrchestrationService` with decision logic implemented
- [ ] `OrderService.createOrder()` updated to store state
- [ ] `application.yml` consumer config added
- [ ] `application.yml` producer type mapping updated
- [ ] Service compiles successfully
- [ ] Service starts without errors

### Part B: Payment Service (Participant)

- [ ] `FinalDecisionEvent` DTO created
- [ ] `DecisionEventConsumer` with `@KafkaListener` implemented
- [ ] `PaymentService.processFinalDecision()` implemented
- [ ] `PaymentService.confirmPayment()` private method added
- [ ] `PaymentService.rollbackPayment()` private method added
- [ ] Idempotency Set (`processedDecisions`) added
- [ ] `application.yml` type mapping updated
- [ ] Service compiles successfully
- [ ] Service starts without errors

### Testing

- [ ] Test 1: Happy path (payment accepted) works
- [ ] Test 2: Payment rejection works
- [ ] Test 3: Idempotency prevents duplicate processing
- [ ] Test 4: Unknown customer handled gracefully
- [ ] Test 5: Service restart recovery works
- [ ] Kafka UI shows all 3 event types
- [ ] H2 database shows correct balance changes
- [ ] No exceptions in logs (except expected validation errors)

---

## 🎯 Success Criteria

Phase 4 is complete when:

1. **Happy Path Works:** Order → Payment Accept → Confirm → Final balance correct
2. **Rejection Works:** Order → Payment Reject → Rollback → Balance unchanged
3. **Idempotency:** Duplicate events don't cause double-processing
4. **State Tracking:** Orders tracked through entire SAGA lifecycle
5. **Event Flow:** All 3 topics (order-events, payment-events, order-decision-events) have messages
6. **No Data Loss:** Service restarts don't lose in-flight orders (Kafka re-delivers)

---

## 🚀 What's Next: Phase 5

After Phase 4, you'll have a complete 2-service SAGA implementation. Phase 5 adds:

1. **stock-service** (third participant)
2. **Multi-participant coordination** (wait for BOTH payment AND stock)
3. **Partial rollback** (one accepts, one rejects → rollback the accepted one)
4. **Complex orchestration** (3-way decision logic)

**Architecture After Phase 5:**

```
Order → Payment + Stock (parallel) → Orchestrator decides:
  - Both accept → CONFIRM both
  - Both reject → REJECT order (nothing to rollback)
  - One accepts, one rejects → ROLLBACK the accepted service
```

This implements the full SAGA compensation pattern with multiple participants.

---

## 📖 Reference Materials

- **Project Plan:** `/docs/PROJECT-PLAN.md` (Phase 5 section)
- **Architecture Doc:** `/docs/05-architecture/saga-orchestration.md`
- **Phase 3 Verification:** `/PHASE-3-VERIFICATION.md`
- **Reference Repo:** [piomin/sample-spring-kafka-microservices](https://github.com/piomin/sample-spring-kafka-microservices)
- **SAGA Pattern:** [Microservices.io - SAGA Pattern](https://microservices.io/patterns/data/saga.html)

---

**Ready to implement Phase 4? Follow the steps above and you'll have a working SAGA orchestration!** 🚀
