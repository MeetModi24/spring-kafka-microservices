# Two Kafka Listeners on Same Topic with Different Consumer Groups

## Overview

This guide explains how to implement **two Kafka listeners on the same topic using different consumer groups** in Spring Boot. This pattern is essential for implementing the SAGA pattern with separate Reserve and Confirm/Rollback phases.

## Pattern Comparison

### Reference Pattern (Status-Based Filtering)

From the GitHub reference repository:

```java
@KafkaListener(id = "orders", topics = "orders", groupId = "payment")
public void onEvent(Order order) {
    if (order.getStatus().equals("NEW")) {
        orderManageService.reserve(order);
    } else if (order.getStatus().equals("CONFIRMED")) {
        orderManageService.confirm(order);
    }
}
```

**Advantages:**
- Single listener (simpler Kafka configuration)
- All logic in one place

**Disadvantages:**
- Runtime string comparison (error-prone)
- Single DTO must handle all event types (not type-safe)
- Mixed concerns (reserve + confirm/rollback logic)
- Hard to scale independently

### Our Pattern (Type-Safe with Separate Listeners)

```java
// Listener 1: Reserve phase
@KafkaListener(topics = "order-events", groupId = "payment-service-group")
public void consumeOrderEvent(OrderCreatedEvent event) {
    paymentService.processOrderPayment(event);
}

// Listener 2: Confirm/Rollback phase
@KafkaListener(topics = "order-events", groupId = "payment-decision-group")
public void consumeDecisionEvent(FinalDecisionEvent event) {
    if (event.getStatus() == CONFIRMED) {
        paymentService.handleConfirm(event);
    } else {
        paymentService.handleRollback(event);
    }
}
```

**Advantages:**
- Type-safe deserialization (compiler-checked)
- Separate DTOs for each event type
- Clear separation of concerns
- Independent scaling (scale each consumer group separately)
- Independent offset tracking (failures in one don't affect the other)

**Disadvantages:**
- Two consumer group connections to Kafka
- Slightly more complex configuration

## How Kafka Routes Messages

### Kafka Consumer Groups

Kafka maintains **separate offsets for each consumer group**:

```
Topic: order-events
├── Partition 0
│   ├── Message 0: OrderCreatedEvent (orderId=123)
│   ├── Message 1: OrderCreatedEvent (orderId=456)
│   └── Message 2: FinalDecisionEvent (orderId=123, status=CONFIRMED)
└── Partition 1
    └── Message 3: FinalDecisionEvent (orderId=456, status=REJECTED)

Consumer Group: payment-service-group
├── Partition 0 offset: 2 (processed messages 0, 1)
└── Partition 1 offset: 1 (processed message 3)

Consumer Group: payment-decision-group
├── Partition 0 offset: 2 (processed messages 0, 1, 2)
└── Partition 1 offset: 1 (processed message 3)
```

**Key Points:**
1. Each consumer group receives **ALL messages** independently
2. Each group tracks its own offset position
3. Messages are not "consumed away" - they stay until topic retention expires
4. Both listeners process the same messages, but deserialize to different DTOs

### Type Mapping and Filtering

Spring Kafka uses **type headers** for polymorphic deserialization:

**Producer side (order-service):**
```java
// When publishing OrderCreatedEvent
ProducerRecord<String, OrderCreatedEvent> record = new ProducerRecord<>(
    "order-events", 
    orderId, 
    orderCreatedEvent
);
// Spring adds header: __TypeId__ = "orderCreated"

// When publishing FinalDecisionEvent
ProducerRecord<String, FinalDecisionEvent> record = new ProducerRecord<>(
    "order-events", 
    orderId, 
    finalDecisionEvent
);
// Spring adds header: __TypeId__ = "finalDecision"
```

**Consumer side (payment-service):**
```yaml
spring.kafka.consumer.properties:
  spring.json.type.mapping: 
    orderCreated:com.example.paymentservice.event.OrderCreatedEvent,
    finalDecision:com.example.paymentservice.event.FinalDecisionEvent
```

**What happens:**
1. `OrderEventConsumer` receives all messages
2. Spring checks `__TypeId__` header
3. If `orderCreated` → deserialize to `OrderCreatedEvent` → call `consumeOrderEvent()`
4. If `finalDecision` → skip (wrong type for this listener)

5. `DecisionEventConsumer` receives all messages
6. Spring checks `__TypeId__` header
7. If `finalDecision` → deserialize to `FinalDecisionEvent` → call `consumeDecisionEvent()`
8. If `orderCreated` → skip (wrong type for this listener)

## Implementation

### Step 1: Define FinalDecisionEvent DTO

**File:** `event/FinalDecisionEvent.java`

```java
package com.example.paymentservice.event;

import lombok.*;
import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FinalDecisionEvent {
    private String orderId;
    private String customerId;
    private DecisionStatus status;
    private String reason;
    private LocalDateTime decidedAt;

    public enum DecisionStatus {
        CONFIRMED,  // Commit transaction
        REJECTED    // Rollback transaction
    }
}
```

**Critical:** This DTO must **exactly match** the structure in order-service.

### Step 2: Create DecisionEventConsumer

**File:** `consumer/DecisionEventConsumer.java`

```java
package com.example.paymentservice.consumer;

import com.example.paymentservice.event.FinalDecisionEvent;
import com.example.paymentservice.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class DecisionEventConsumer {

    private final PaymentService paymentService;

    @KafkaListener(
        topics = "order-events",
        groupId = "payment-decision-group",  // DIFFERENT from OrderEventConsumer
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeDecisionEvent(FinalDecisionEvent event) {
        log.info("Received FinalDecisionEvent: orderId={}, status={}", 
            event.getOrderId(), event.getStatus());

        try {
            if (event.getStatus() == FinalDecisionEvent.DecisionStatus.CONFIRMED) {
                paymentService.handleConfirm(event);
            } else if (event.getStatus() == FinalDecisionEvent.DecisionStatus.REJECTED) {
                paymentService.handleRollback(event);
            }
            
            log.info("Successfully processed decision for order: {}", event.getOrderId());
            
        } catch (Exception e) {
            log.error("Failed to process decision for order: {}", event.getOrderId(), e);
            throw e;  // Prevents offset commit, message will be retried
        }
    }
}
```

### Step 3: Update PaymentService

**File:** `service/PaymentService.java`

Add these methods:

```java
import com.example.paymentservice.event.FinalDecisionEvent;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentService {
    
    private final CustomerRepository customerRepository;
    private final KafkaTemplate<String, PaymentProcessedEvent> kafkaTemplate;
    
    // Idempotency tracking
    private final Set<String> processedReservations = ConcurrentHashMap.newKeySet();
    private final Set<String> processedDecisions = ConcurrentHashMap.newKeySet();
    
    /**
     * Handle order confirmation (SAGA Confirm phase)
     */
    @Transactional
    public void handleConfirm(FinalDecisionEvent event) {
        log.info("Handling CONFIRM for order: {}", event.getOrderId());
        
        // Idempotency check
        if (processedDecisions.contains(event.getOrderId())) {
            log.warn("Decision already processed: {}", event.getOrderId());
            return;
        }
        
        Optional<Customer> customerOpt = customerRepository
            .findByCustomerId(event.getCustomerId());
        
        if (customerOpt.isEmpty()) {
            log.error("Customer not found: {}", event.getCustomerId());
            processedDecisions.add(event.getOrderId());
            return;
        }
        
        Customer customer = customerOpt.get();
        Integer reservedAmount = customer.getAmountReserved();
        
        if (reservedAmount > 0) {
            customer.confirm(reservedAmount);
            customerRepository.save(customer);
            log.info("Payment CONFIRMED for order: {} | Deducted: ${}", 
                event.getOrderId(), reservedAmount / 100.0);
        }
        
        processedDecisions.add(event.getOrderId());
    }
    
    /**
     * Handle order rollback (SAGA Compensate phase)
     */
    @Transactional
    public void handleRollback(FinalDecisionEvent event) {
        log.info("Handling ROLLBACK for order: {} | Reason: {}", 
            event.getOrderId(), event.getReason());
        
        // Idempotency check
        if (processedDecisions.contains(event.getOrderId())) {
            log.warn("Decision already processed: {}", event.getOrderId());
            return;
        }
        
        Optional<Customer> customerOpt = customerRepository
            .findByCustomerId(event.getCustomerId());
        
        if (customerOpt.isEmpty()) {
            log.error("Customer not found: {}", event.getCustomerId());
            processedDecisions.add(event.getOrderId());
            return;
        }
        
        Customer customer = customerOpt.get();
        Integer reservedAmount = customer.getAmountReserved();
        
        if (reservedAmount > 0) {
            customer.rollback(reservedAmount);
            customerRepository.save(customer);
            log.info("Payment ROLLED BACK for order: {} | Returned: ${}", 
                event.getOrderId(), reservedAmount / 100.0);
        }
        
        processedDecisions.add(event.getOrderId());
    }
}
```

### Step 4: Update application.yml

**File:** `resources/application.yml`

```yaml
spring:
  kafka:
    consumer:
      group-id: payment-service-group  # Default group
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "*"
        # Add finalDecision mapping
        spring.json.type.mapping: >
          orderCreated:com.example.paymentservice.event.OrderCreatedEvent,
          finalDecision:com.example.paymentservice.event.FinalDecisionEvent
```

## Consumer Group Isolation

### Independent Scaling

Each consumer group can scale independently:

```bash
# Scale reserve phase (handles high order creation volume)
# Instance 1
java -jar payment-service.jar --kafka.consumer.group-id=payment-service-group

# Instance 2
java -jar payment-service.jar --kafka.consumer.group-id=payment-service-group

# Scale confirm/rollback phase (handles final decisions)
# Instance 3
java -jar payment-service.jar --kafka.consumer.group-id=payment-decision-group
```

### Independent Offset Tracking

Failures in one consumer group don't affect the other:

**Scenario:** Reserve phase processing fails

```
Consumer Group: payment-service-group
├── Partition 0 offset: 5 (stuck, failed processing message 5)
└── Partition 1 offset: 8 (healthy)

Consumer Group: payment-decision-group
├── Partition 0 offset: 10 (healthy, continues processing)
└── Partition 1 offset: 12 (healthy)
```

The decision consumer continues processing even though reserve consumer is stuck.

### Message Ordering

Kafka guarantees ordering **within a partition**:

```
Partition 0:
Message 0: OrderCreatedEvent (orderId=123)
Message 1: FinalDecisionEvent (orderId=123, status=CONFIRMED)

Guarantee: Message 0 is ALWAYS processed before Message 1 (within each consumer group)
```

**Best Practice:** Use order ID as message key:

```java
kafkaTemplate.send("order-events", orderId, event);
```

This ensures all events for the same order go to the same partition.

## Testing

### Test Case 1: Successful Order

```
1. POST /api/orders → OrderCreatedEvent published
2. payment-service-group receives OrderCreatedEvent
3. PaymentService.processOrderPayment() → reserve funds
4. PaymentProcessedEvent published: status=ACCEPT
5. order-service receives acceptance → publishes FinalDecisionEvent: status=CONFIRMED
6. payment-decision-group receives FinalDecisionEvent
7. PaymentService.handleConfirm() → commit reservation
```

**Verification:**
```sql
SELECT * FROM customers WHERE customer_id = 'CUST-123';
-- Before: amountAvailable=10000, amountReserved=0
-- After reserve: amountAvailable=8000, amountReserved=2000
-- After confirm: amountAvailable=8000, amountReserved=0
```

### Test Case 2: Rejected Order

```
1. POST /api/orders → OrderCreatedEvent published
2. payment-service-group receives OrderCreatedEvent
3. PaymentService.processOrderPayment() → insufficient balance
4. PaymentProcessedEvent published: status=REJECT
5. order-service receives rejection → publishes FinalDecisionEvent: status=REJECTED
6. payment-decision-group receives FinalDecisionEvent
7. PaymentService.handleRollback() → no-op (nothing to rollback)
```

### Test Case 3: Rollback After Reserve

```
1. POST /api/orders → OrderCreatedEvent published
2. payment-service: reserve funds (SUCCESS)
3. inventory-service: reserve stock (FAIL)
4. order-service publishes FinalDecisionEvent: status=REJECTED
5. payment-decision-group receives FinalDecisionEvent
6. PaymentService.handleRollback() → return reserved funds
```

**Verification:**
```sql
-- After reserve: amountAvailable=8000, amountReserved=2000
-- After rollback: amountAvailable=10000, amountReserved=0
```

### Test Case 4: Idempotency

```bash
# Simulate duplicate message
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId": "CUST-123", "items": [...]}'

# Kafka retries same OrderCreatedEvent twice
# PaymentService.processOrderPayment() called twice
# First call: processes normally
# Second call: skipped (processedReservations.contains(orderId))
```

## Troubleshooting

### Messages Not Received by DecisionEventConsumer

**Symptoms:**
- OrderEventConsumer works fine
- DecisionEventConsumer never receives messages

**Diagnosis:**
```bash
# Check consumer group offsets
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group payment-decision-group \
  --describe

# Expected output:
# GROUP                      TOPIC         PARTITION  CURRENT-OFFSET  LAG
# payment-decision-group     order-events  0          10              0
# payment-decision-group     order-events  1          8               0
```

**Common causes:**
1. Type mapping missing in application.yml
2. Wrong groupId in @KafkaListener
3. FinalDecisionEvent not published by order-service
4. Type header mismatch

### Deserialization Errors

**Symptoms:**
```
JsonDeserializer: Deserialization failed for type [FinalDecisionEvent]
```

**Diagnosis:**
1. Check type mapping: `spring.json.type.mapping` must match producer's type ID
2. Check DTO structure: fields must match exactly (name, type, case)
3. Check producer: is it setting the type header?

**Fix:**
```yaml
# Producer (order-service)
spring.kafka.producer.properties:
  spring.json.type.mapping: finalDecision:com.example.orderservice.event.FinalDecisionEvent

# Consumer (payment-service)
spring.kafka.consumer.properties:
  spring.json.type.mapping: finalDecision:com.example.paymentservice.event.FinalDecisionEvent
```

### Consumer Lag Building Up

**Symptoms:**
```bash
kafka-consumer-groups.sh --describe
# LAG column shows increasing numbers
```

**Diagnosis:**
1. Check processing time: is handleConfirm/handleRollback slow?
2. Check error rate: are exceptions being thrown?
3. Check CPU/memory: is service overloaded?

**Solutions:**
1. Scale horizontally: add more instances to consumer group
2. Optimize processing: add database indexes, reduce queries
3. Add batch processing (advanced)

## Production Considerations

### 1. Idempotency Tracking

Replace in-memory sets with Redis:

```java
@Service
@RequiredArgsConstructor
public class PaymentService {
    private final RedisTemplate<String, Boolean> redisTemplate;
    
    @Transactional
    public void handleConfirm(FinalDecisionEvent event) {
        String key = "processed:decision:" + event.getOrderId();
        
        // Check if already processed
        Boolean exists = redisTemplate.opsForValue()
            .setIfAbsent(key, true, Duration.ofDays(7));
        
        if (Boolean.FALSE.equals(exists)) {
            log.warn("Decision already processed: {}", event.getOrderId());
            return;
        }
        
        // Process decision...
    }
}
```

### 2. Store Reservation Details

Track per-order reservation amounts:

```sql
CREATE TABLE payment_reservations (
    order_id VARCHAR(255) PRIMARY KEY,
    customer_id VARCHAR(255) NOT NULL,
    amount_cents INT NOT NULL,
    reserved_at TIMESTAMP NOT NULL,
    status VARCHAR(50) NOT NULL  -- RESERVED, CONFIRMED, ROLLED_BACK
);
```

```java
@Transactional
public void processOrderPayment(OrderCreatedEvent event) {
    // Reserve funds...
    
    // Store reservation
    PaymentReservation reservation = new PaymentReservation();
    reservation.setOrderId(event.getOrderId());
    reservation.setCustomerId(event.getCustomerId());
    reservation.setAmountCents(amountCents);
    reservation.setReservedAt(LocalDateTime.now());
    reservation.setStatus(ReservationStatus.RESERVED);
    reservationRepository.save(reservation);
}

@Transactional
public void handleConfirm(FinalDecisionEvent event) {
    // Fetch reservation amount
    PaymentReservation reservation = reservationRepository
        .findByOrderId(event.getOrderId())
        .orElseThrow();
    
    // Confirm specific amount
    customer.confirm(reservation.getAmountCents());
    
    // Update reservation status
    reservation.setStatus(ReservationStatus.CONFIRMED);
    reservationRepository.save(reservation);
}
```

### 3. Error Handling

Configure retry and DLQ:

```yaml
spring:
  kafka:
    consumer:
      # Retry configuration
      properties:
        max.poll.records: 10
        max.poll.interval.ms: 300000  # 5 minutes
        session.timeout.ms: 30000     # 30 seconds
        
    listener:
      # Retry 3 times with exponential backoff
      retry:
        max-attempts: 3
        backoff:
          initial-interval: 1000
          multiplier: 2.0
          max-interval: 10000
      
      # Send to DLQ after max retries
      ack-mode: MANUAL
      
# Custom error handler
@Component
public class CustomErrorHandler implements ConsumerAwareListenerErrorHandler {
    @Override
    public Object handleError(Message<?> message, 
                             ListenerExecutionFailedException exception,
                             Consumer<?, ?> consumer) {
        log.error("Error processing message", exception);
        
        // Send to DLQ
        kafkaTemplate.send("payment-events-dlq", message);
        
        // Commit offset (don't retry)
        consumer.commitSync();
        
        return null;
    }
}
```

### 4. Monitoring

Add metrics:

```java
@Component
@RequiredArgsConstructor
public class DecisionEventConsumer {
    private final MeterRegistry meterRegistry;
    
    @KafkaListener(...)
    public void consumeDecisionEvent(FinalDecisionEvent event) {
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try {
            // Process event...
            
            meterRegistry.counter("payment.decision.success", 
                "status", event.getStatus().name()).increment();
            
        } catch (Exception e) {
            meterRegistry.counter("payment.decision.error",
                "error", e.getClass().getSimpleName()).increment();
            throw e;
            
        } finally {
            sample.stop(meterRegistry.timer("payment.decision.duration",
                "status", event.getStatus().name()));
        }
    }
}
```

## Summary

### Key Takeaways

1. **Same Topic, Different Groups**: Both listeners subscribe to `order-events` but with different consumer groups
2. **Independent Offset Tracking**: Each group maintains its own position in the topic
3. **Type-Safe Deserialization**: Spring uses type headers to route messages to correct DTO
4. **Separation of Concerns**: Reserve logic separate from confirm/rollback logic
5. **Independent Scaling**: Scale each phase independently based on load
6. **Idempotency Required**: Track processed messages to handle retries safely

### When to Use This Pattern

Use this pattern when:
- Implementing SAGA pattern with distinct phases
- Events require different processing logic
- Need independent scaling of different event types
- Want type-safe deserialization
- Need clear separation of concerns

Avoid this pattern when:
- Single event type per topic (simpler to use one listener)
- Events are processed identically
- No need for independent scaling
- Prefer runtime filtering over compile-time type safety

### Comparison Table

| Aspect | Single Listener (Status-Based) | Two Listeners (Consumer Groups) |
|--------|-------------------------------|----------------------------------|
| Type Safety | Runtime string checks | Compile-time type checking |
| Scaling | Scale entire listener | Scale each phase independently |
| Offset Tracking | Single offset | Independent offsets per group |
| Code Clarity | Mixed concerns | Separated concerns |
| Kafka Connections | 1 consumer group | 2 consumer groups |
| Configuration | Simpler | Slightly more complex |
| Error Isolation | Failures affect all events | Failures isolated per phase |
| Best For | Simple use cases | Complex SAGA patterns |
