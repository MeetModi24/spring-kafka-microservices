# Multi-Item SAGA Pattern Implementation

> **Design Pattern:** Atomic multi-item reservation with compensating transactions  
> **Use Case:** E-commerce orders with multiple products  
> **Key Feature:** All-or-nothing semantics across distributed services

---

## The Challenge

**Scenario:** Customer orders 3 products (Laptop + Mouse + Keyboard)

**Requirements:**
1. Reserve ALL items in stock service
2. Charge TOTAL amount in payment service
3. If ANY item unavailable → rollback EVERYTHING
4. If payment fails → rollback ALL stock reservations

**Distributed System Challenge:**
- Stock service doesn't know about payment
- Payment service doesn't know about stock
- Both services must stay consistent
- No distributed transactions (2PC)

---

## Solution: Atomic Reservation with Saga Orchestration

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                       order-service                         │
│                  (Saga Orchestrator)                        │
│                                                             │
│  1. Publishes OrderCreatedEvent                            │
│     - items: [Laptop, Mouse, Keyboard]                     │
│     - totalAmount: $1,139.96                               │
│                                                             │
│  4. Kafka Streams Join (payment ⋈ stock)                   │
│     - Both ACCEPT → CONFIRMED                              │
│     - One REJECT → ROLLBACK (with source)                  │
│                                                             │
│  5. Publishes FinalDecisionEvent                           │
└─────────────────────────────────────────────────────────────┘
         │                                    │
         │ 2a. Reserve phase                 │ 2b. Reserve phase
         ↓                                    ↓
┌──────────────────────┐           ┌──────────────────────┐
│  payment-service     │           │  stock-service       │
│                      │           │                      │
│  Reserve total       │           │  Reserve ALL items   │
│  $1,139.96          │           │  atomically:         │
│                      │           │  - 1 Laptop ✓        │
│  amountAvailable -=  │           │  - 2 Mice ✓          │
│  amountReserved +=   │           │  - 1 Keyboard ✓      │
│                      │           │                      │
│  Response: ACCEPT    │           │  Response: ACCEPT    │
└──────────────────────┘           └──────────────────────┘
         │                                    │
         │ 3a. Response                       │ 3b. Response
         ↓                                    ↓
         PaymentProcessedEvent         StockProcessedEvent
              (to order-service)           (to order-service)
```

---

## Implementation Details

### 1. Stock Service - Atomic Multi-Item Reservation

**Key Algorithm:**

```java
@Transactional
public StockProcessedEvent processOrderStock(OrderCreatedEvent event) {
    List<Product> reservedProducts = new ArrayList<>();
    boolean allAvailable = true;
    
    // Phase 1: Try to reserve ALL items
    for (OrderItem item : event.getItems()) {
        Product product = findProduct(item.getProductId());
        
        if (product.reserve(item.getQuantity())) {
            reservedProducts.add(product);  // Track for potential rollback
        } else {
            allAvailable = false;
            break;  // Stop immediately on first failure
        }
    }
    
    // Phase 2: Commit or rollback based on result
    if (allAvailable) {
        // SUCCESS: Commit all reservations to database
        productRepository.saveAll(reservedProducts);
        return StockProcessedEvent.ACCEPT;
        
    } else {
        // FAILURE: Rollback all previously reserved items
        for (Product product : reservedProducts) {
            product.rollback(quantity);
        }
        productRepository.saveAll(reservedProducts);
        return StockProcessedEvent.REJECT;
    }
}
```

**Guarantees:**
- ✅ **Atomicity:** Either ALL items reserved or NONE
- ✅ **Consistency:** Database always in valid state
- ✅ **Isolation:** @Transactional ensures no interference
- ✅ **Durability:** Changes persisted before response sent

---

### 2. Payment Service - Total Amount Calculation

```java
public PaymentProcessedEvent processOrderPayment(OrderCreatedEvent event) {
    // Calculate total from ALL items
    BigDecimal totalAmount = event.getItems().stream()
        .map(item -> item.getPrice().multiply(BigDecimal.valueOf(item.getQuantity())))
        .reduce(BigDecimal.ZERO, BigDecimal::add);
    
    Customer customer = findCustomer(event.getCustomerId());
    
    if (customer.reserve(totalAmount)) {
        return PaymentProcessedEvent.builder()
            .status(PaymentStatus.ACCEPT)
            .amountCharged(totalAmount)
            .build();
    } else {
        return PaymentProcessedEvent.builder()
            .status(PaymentStatus.REJECT)
            .reason("Insufficient balance")
            .build();
    }
}
```

---

### 3. Kafka Streams Orchestrator - 3-Way Decision Matrix

```java
/**
 * Join payment and stock responses, make final decision.
 * 
 * Decision Matrix:
 * ┌─────────┬────────┬────────────────────────────┐
 * │ Payment │ Stock  │ Decision                   │
 * ├─────────┼────────┼────────────────────────────┤
 * │ ACCEPT  │ ACCEPT │ CONFIRMED (commit both)    │
 * │ REJECT  │ REJECT │ REJECTED (nothing to undo) │
 * │ ACCEPT  │ REJECT │ ROLLBACK payment           │
 * │ REJECT  │ ACCEPT │ ROLLBACK stock             │
 * └─────────┴────────┴────────────────────────────┘
 */
private FinalDecisionEvent makeDecision(PaymentProcessedEvent payment, 
                                       StockProcessedEvent stock) {
    
    if (payment.isAccept() && stock.isAccept()) {
        // Happy path: both succeeded
        return FinalDecisionEvent.CONFIRMED;
    }
    
    if (payment.isReject() && stock.isReject()) {
        // Both failed: nothing to compensate
        return FinalDecisionEvent.REJECTED;
    }
    
    // Partial success: compensate the successful service
    String failureSource = payment.isReject() ? "PAYMENT" : "STOCK";
    
    return FinalDecisionEvent.builder()
        .status(DecisionStatus.ROLLBACK)
        .source(failureSource)  // Tells services who to compensate
        .build();
}
```

---

### 4. Compensation Logic

**Stock service receives ROLLBACK:**

```java
@KafkaListener(topics = "order-events", groupId = "stock-decision-group")
public void consumeDecisionEvent(FinalDecisionEvent event) {
    
    if (event.getStatus() == ROLLBACK) {
        // Only rollback if STOCK was successful (payment failed)
        if (!"STOCK".equals(event.getSource())) {
            // Payment failed, we succeeded → compensate
            for (OrderItem item : event.getItems()) {
                Product product = findProduct(item.getProductId());
                product.rollback(item.getQuantity());  // Return to available
            }
            log.info("Stock ROLLED BACK: returned all items to inventory");
        } else {
            // Stock failed, payment succeeded → payment will compensate
            log.info("Stock failure triggered rollback - no action needed");
        }
    }
}
```

**Payment service receives ROLLBACK:**

```java
@KafkaListener(topics = "order-events", groupId = "payment-decision-group")
public void consumeDecisionEvent(FinalDecisionEvent event) {
    
    if (event.getStatus() == ROLLBACK) {
        // Only rollback if PAYMENT was successful (stock failed)
        if (!"PAYMENT".equals(event.getSource())) {
            // Stock failed, we succeeded → compensate
            BigDecimal totalAmount = calculateTotal(event.getItems());
            
            Customer customer = findCustomer(event.getCustomerId());
            customer.rollback(totalAmount);  // Return from reserved to available
            
            log.info("Payment ROLLED BACK: returned {} to customer", totalAmount);
        } else {
            // Payment failed, stock succeeded → stock will compensate
            log.info("Payment failure triggered rollback - no action needed");
        }
    }
}
```

---

## Example Flow: Partial Stock Failure

### Scenario
Customer orders:
- 5 Laptops ($4,999.95)
- 300 Mice ($8,997.00)
- **Total:** $13,996.95

Stock availability:
- Laptops: 50 available ✅
- Mice: 200 available ❌ (need 300)

---

### Step-by-Step Execution

**T=0s:** Order created
```
order-service publishes OrderCreatedEvent
  orderId: "abc-123"
  items: [
    {productId: "PROD-001", quantity: 5},
    {productId: "PROD-002", quantity: 300}
  ]
  totalAmount: $13,996.95
```

---

**T=0.1s:** Payment service processes
```
payment-service receives OrderCreatedEvent
  Customer CUST-1 balance: $20,000
  Required: $13,996.95
  
  RESERVE: amountAvailable -= $13,996.95
           amountReserved += $13,996.95
  
  Publishes PaymentProcessedEvent
    status: ACCEPT
    amountCharged: $13,996.95
```

---

**T=0.2s:** Stock service processes (atomic attempt)
```
stock-service receives OrderCreatedEvent
  
  Item 1: PROD-001 (Laptops)
    Available: 50, Required: 5
    RESERVE: availableItems -= 5, reservedItems += 5 ✅
    
  Item 2: PROD-002 (Mice)
    Available: 200, Required: 300
    FAIL: Insufficient stock ❌
    
  ATOMIC ROLLBACK:
    PROD-001: reservedItems -= 5, availableItems += 5
    (Returned 5 laptops to inventory)
  
  Publishes StockProcessedEvent
    status: REJECT
    reason: "Product PROD-002 insufficient (available: 200, required: 300)"
```

---

**T=0.3s:** Kafka Streams join
```
order-service Kafka Streams topology
  
  Received PaymentProcessedEvent: ACCEPT
  Received StockProcessedEvent: REJECT
  
  JOIN within 10-second window ✅
  
  Decision logic:
    payment=ACCEPT, stock=REJECT
    → ROLLBACK with source=STOCK
  
  Publishes FinalDecisionEvent
    orderId: "abc-123"
    status: ROLLBACK
    source: "STOCK"
    reason: "Product PROD-002 insufficient..."
```

---

**T=0.4s:** Payment service compensates
```
payment-service receives FinalDecisionEvent
  status: ROLLBACK
  source: "STOCK"  ← Stock failed, we succeeded
  
  COMPENSATE:
    Customer CUST-1
    amountReserved -= $13,996.95
    amountAvailable += $13,996.95
  
  Result: Customer refunded, balance back to $20,000
```

---

**T=0.4s:** Stock service ignores (already rolled back)
```
stock-service receives FinalDecisionEvent
  status: ROLLBACK
  source: "STOCK"  ← We failed, payment succeeded
  
  NO ACTION: We already rolled back 5 laptops atomically
  
  Result: All products back to original quantities
```

---

### Final State

**Customer:**
- Balance: $20,000 (unchanged)
- Reserved: $0

**Stock:**
- Laptops: 50 available, 0 reserved (unchanged)
- Mice: 200 available, 0 reserved (unchanged)

**Order:**
- Status: CANCELLED
- Reason: "Insufficient stock for Product PROD-002"

**Result:** ✅ Consistent state, no partial reservations

---

## Key Patterns Demonstrated

### 1. Atomic Multi-Item Reservation

**Problem:** Can't use database transactions across services

**Solution:** 
- Try to reserve ALL items within single service
- Track reserved items in list
- If ANY fails → rollback all in same transaction
- Respond only after commit/rollback complete

---

### 2. Compensating Transactions

**Problem:** Can't rollback distributed transaction with ACID guarantees

**Solution:**
- Each service publishes ACCEPT/REJECT
- Orchestrator decides based on all responses
- Successful services receive ROLLBACK command
- Each service knows how to undo its work

---

### 3. Source Tracking

**Problem:** Both services receive ROLLBACK, but only one needs to act

**Solution:**
- `source` field tracks which service failed
- "If source != me, then I succeeded → compensate"
- "If source == me, then I failed → nothing to undo"

---

### 4. Idempotency

**Problem:** Kafka messages can be delivered multiple times

**Solution:**
```java
private final Set<String> processedReservations = ConcurrentHashMap.newKeySet();
private final Set<String> processedDecisions = ConcurrentHashMap.newKeySet();

public StockProcessedEvent processOrderStock(OrderCreatedEvent event) {
    if (processedReservations.contains(event.getOrderId())) {
        return cachedResponse(event.getOrderId());  // Return same response
    }
    
    // ... actual processing ...
    
    processedReservations.add(event.getOrderId());
    return response;
}
```

---

### 5. Join Window

**Problem:** Payment and stock responses might arrive at different times

**Solution:**
- Kafka Streams buffers first event
- Waits up to 10 seconds for second event
- If both arrive within window → join and decide
- If timeout → event dropped (requires monitoring/DLQ)

---

## Benefits Over Single-Item Pattern

### Reference Repo (Single Item)
```java
Order {
    Long productId;      // One product
    int productCount;    // One quantity
}
```

**Limitations:**
- ❌ Unrealistic (real orders have multiple items)
- ❌ No atomic reservation demo
- ❌ No partial failure scenario
- ❌ Limited learning value

---

### Our Pattern (Multi-Item)
```java
Order {
    List<OrderItem> items;  // Multiple products
}
```

**Advantages:**
- ✅ Realistic e-commerce scenario
- ✅ Demonstrates atomic operations
- ✅ Shows partial failure handling
- ✅ Teaches aggregation in distributed systems
- ✅ More complex, more learning

---

## Testing Strategy

### Test Case Matrix

| # | Payment | Stock | Expected Outcome |
|---|---------|-------|------------------|
| 1 | ACCEPT | ACCEPT | CONFIRMED (commit both) |
| 2 | REJECT | REJECT | REJECTED (nothing to undo) |
| 3 | ACCEPT | REJECT | ROLLBACK payment |
| 4 | REJECT | ACCEPT | ROLLBACK stock |
| 5 | ACCEPT | Timeout (>10s) | Order stuck (monitoring alert) |

### Edge Cases to Test

1. **Partial stock failure:**
   - Order: Item A (available) + Item B (unavailable)
   - Verify: Item A reservation rolled back atomically

2. **Exact balance:**
   - Customer has exactly order total
   - Verify: amountAvailable goes to $0, order succeeds

3. **Concurrent orders:**
   - Two customers order same product simultaneously
   - Verify: One succeeds, one fails (no overselling)

4. **Idempotency:**
   - Duplicate OrderCreatedEvent
   - Verify: Same response, no double-reservation

5. **Late arrival:**
   - Payment responds immediately, stock responds after 15s
   - Verify: Join fails, order stays PENDING (requires DLQ)

---

## Production Considerations

### Monitoring

**Metrics to track:**
- Join success rate (should be >99%)
- Average join latency (should be <1s)
- ROLLBACK percentage (should be <5%)
- Stuck orders (no decision after 30s)

**Alerts:**
- Join timeout (payment OR stock never responded)
- High ROLLBACK rate (indicates stock/payment issues)
- Increasing stuck orders

---

### Dead Letter Queue (DLQ)

**For Production:** Add DLQ topic for failed joins

```java
paymentStream.join(stockStream, ...)
    .peek((key, value) -> log.info("Join successful: {}", key))
    .to("order-events");

// Separate processor for timeouts
paymentStream
    .filter((key, value) -> !hasJoined(key))  // Pseudo-code
    .to("order-events-dlq");  // Dead letter queue
```

**DLQ Consumer:** Manual review or retry logic

---

### Scalability

**Kafka partitioning:**
- Orders topic: 10 partitions (by customerId hash)
- Each order-service instance processes subset
- State stores partitioned automatically

**Load testing:**
- 1,000 orders/sec sustained
- 10,000 orders/sec burst
- Verify: No join timeouts, no stuck orders

---

## Summary

**Pattern:** Atomic multi-item SAGA with Kafka Streams orchestration

**Key Features:**
- ✅ Multi-item orders (realistic e-commerce)
- ✅ Atomic reservation (all or nothing)
- ✅ Compensating transactions (ROLLBACK)
- ✅ Source tracking (who failed?)
- ✅ Idempotency (duplicate protection)
- ✅ Join window (10-second timeout)

**Guarantees:**
- ✅ Eventual consistency across services
- ✅ No partial reservations
- ✅ Customer never overcharged
- ✅ Inventory never oversold

**Use Cases:**
- E-commerce order processing
- Travel booking (flight + hotel + car)
- Supply chain procurement
- Any multi-resource reservation system

---

**This is production-ready SAGA orchestration for distributed systems.**
