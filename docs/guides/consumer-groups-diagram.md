# Consumer Groups Visualization

## Two Listeners, Same Topic, Different Consumer Groups

```
┌─────────────────────────────────────────────────────────────────────────┐
│                             ORDER-SERVICE                                │
│                                                                          │
│  POST /api/orders                                                        │
│       │                                                                  │
│       ▼                                                                  │
│  ┌─────────────────┐         ┌──────────────────┐                      │
│  │  Create Order   │────────▶│ Publish Event    │                      │
│  │  orderId=123    │         │ OrderCreatedEvent│                      │
│  └─────────────────┘         └────────┬─────────┘                      │
│                                        │                                 │
└────────────────────────────────────────┼─────────────────────────────────┘
                                         │
                                         ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         KAFKA: order-events Topic                        │
│                                                                          │
│  Partition 0                             Partition 1                     │
│  ┌────────────────────────┐             ┌────────────────────────┐     │
│  │ Offset 0: OrderCreated │             │ Offset 0: OrderCreated │     │
│  │ Offset 1: FinalDecision│             │ Offset 1: OrderCreated │     │
│  │ Offset 2: OrderCreated │             │ Offset 2: FinalDecision│     │
│  └────────────────────────┘             └────────────────────────┘     │
│         │                                         │                      │
│         │  Both groups receive ALL messages       │                      │
│         │  independently (fan-out pattern)        │                      │
│         ▼                                         ▼                      │
└─────────────────────────────────────────────────────────────────────────┘
         │                                         │
         │                                         │
         ├─────────────────┬───────────────────────┤
         │                 │                       │
         ▼                 ▼                       ▼
┌─────────────────┐ ┌──────────────────┐ ┌──────────────────┐
│ Consumer Group: │ │ Consumer Group:   │ │ Consumer Group:   │
│ payment-service-│ │ payment-decision- │ │ inventory-service-│
│ group           │ │ group             │ │ group             │
│                 │ │                   │ │                   │
│ Offset P0: 2    │ │ Offset P0: 2      │ │ Offset P0: 2      │
│ Offset P1: 2    │ │ Offset P1: 2      │ │ Offset P1: 2      │
└────────┬────────┘ └─────────┬─────────┘ └─────────┬─────────┘
         │                    │                      │
         │ Filters for        │ Filters for          │ Filters for
         │ OrderCreatedEvent  │ FinalDecisionEvent   │ OrderCreatedEvent
         ▼                    ▼                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          PAYMENT-SERVICE                                 │
│                                                                          │
│  ┌───────────────────────────┐      ┌───────────────────────────┐     │
│  │ OrderEventConsumer        │      │ DecisionEventConsumer      │     │
│  │ @KafkaListener(           │      │ @KafkaListener(            │     │
│  │   topics="order-events"   │      │   topics="order-events"    │     │
│  │   groupId="payment-       │      │   groupId="payment-        │     │
│  │     service-group"        │      │     decision-group"        │     │
│  │ )                         │      │ )                          │     │
│  │                           │      │                            │     │
│  │ consumeOrderEvent(        │      │ consumeDecisionEvent(      │     │
│  │   OrderCreatedEvent)      │      │   FinalDecisionEvent)      │     │
│  └──────────┬────────────────┘      └──────────┬─────────────────┘     │
│             │                                   │                       │
│             │ Type header:                      │ Type header:          │
│             │ __TypeId__="orderCreated"         │ __TypeId__=           │
│             │                                   │   "finalDecision"     │
│             ▼                                   ▼                       │
│  ┌─────────────────────────────────────────────────────────────┐      │
│  │                    PaymentService                            │      │
│  │                                                              │      │
│  │  ┌─────────────────────┐      ┌──────────────────────────┐ │      │
│  │  │ processOrderPayment │      │ handleConfirm()          │ │      │
│  │  │ (Reserve Phase)     │      │ OR                       │ │      │
│  │  │                     │      │ handleRollback()         │ │      │
│  │  │ 1. Check idempotency│      │ (Confirm/Rollback Phase) │ │      │
│  │  │ 2. Find customer    │      │                          │ │      │
│  │  │ 3. Reserve funds    │      │ 1. Check idempotency     │ │      │
│  │  │ 4. Save customer    │      │ 2. Find customer         │ │      │
│  │  │ 5. Publish response │      │ 3. Commit/Rollback funds │ │      │
│  │  │ 6. Mark processed   │      │ 4. Save customer         │ │      │
│  │  └──────────┬──────────┘      │ 5. Mark processed        │ │      │
│  │             │                  └────────────────────────────┘ │      │
│  │             │                                                 │      │
│  └─────────────┼─────────────────────────────────────────────────┘      │
│                │                                                         │
│                ▼                                                         │
│  ┌───────────────────────────┐                                          │
│  │ Database: customers        │                                          │
│  │                            │                                          │
│  │ CUST-123                   │                                          │
│  │ - amountAvailable: 8000    │                                          │
│  │ - amountReserved: 2000     │                                          │
│  └────────────────────────────┘                                          │
│                │                                                         │
│                │ Publishes PaymentProcessedEvent                         │
│                ▼                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    KAFKA: payment-events Topic                           │
│                                                                          │
│  PaymentProcessedEvent { orderId=123, status=ACCEPT }                   │
└─────────────────────────────────────────────────────────────────────────┘
```

## Key Concepts Illustrated

### 1. Fan-Out Pattern
Every consumer group receives **ALL messages** from the topic independently:
- `payment-service-group` gets: Message 0, 1, 2, 3, ...
- `payment-decision-group` gets: Message 0, 1, 2, 3, ... (same messages!)
- They maintain separate offset positions

### 2. Type-Based Filtering
Spring deserializes based on `__TypeId__` header:

```
Message Header:
{
  "__TypeId__": "orderCreated",
  "kafka_receivedTopic": "order-events",
  "kafka_receivedPartition": 0
}

↓ Spring JsonDeserializer checks type mapping

application.yml:
  spring.json.type.mapping: 
    orderCreated → OrderCreatedEvent.class
    finalDecision → FinalDecisionEvent.class

↓ Deserializes to correct DTO

OrderEventConsumer receives: OrderCreatedEvent instance
DecisionEventConsumer receives: null (wrong type, skipped)
```

### 3. Independent Offsets

```
Topic: order-events (Partition 0)
┌─────────────────────────────────────────────┐
│ Offset 0: OrderCreatedEvent (orderId=123)   │
│ Offset 1: FinalDecisionEvent (orderId=123)  │
│ Offset 2: OrderCreatedEvent (orderId=456)   │
│ Offset 3: FinalDecisionEvent (orderId=456)  │
└─────────────────────────────────────────────┘

Consumer Group: payment-service-group
  Current Offset: 2
  Processing: OrderCreatedEvent (orderId=456)
  
Consumer Group: payment-decision-group
  Current Offset: 3
  Processing: FinalDecisionEvent (orderId=456)
```

If `payment-service-group` crashes at offset 2:
- It will restart from offset 2 (reprocess message 2)
- `payment-decision-group` continues unaffected at offset 3

### 4. Message Flow Timeline

```
Time  | Event                              | payment-service-group | payment-decision-group
------|------------------------------------|-----------------------|-----------------------
T0    | OrderCreatedEvent published        | Receives at offset 0  | Receives at offset 0
      | (orderId=123)                      |                       |
T1    | OrderEventConsumer processes       | Processing...         | Skips (wrong type)
T2    | processOrderPayment() reserves     | Commits offset 0      | Skips
T3    | PaymentProcessedEvent published    | Done                  | Waiting...
      | (status=ACCEPT)                    |                       |
T4    | FinalDecisionEvent published       | Receives at offset 1  | Receives at offset 1
      | (orderId=123, status=CONFIRMED)    |                       |
T5    | DecisionEventConsumer processes    | Skips (wrong type)    | Processing...
T6    | handleConfirm() commits funds      | Waiting...            | Commits offset 1
T7    | Both consumers idle                | Offset: 1             | Offset: 1
```

### 5. Scalability

Each consumer group can scale independently:

```
┌──────────────────────────────────────────────────────────┐
│ payment-service-group (Reserve phase)                    │
│                                                          │
│ Instance 1 ──┐                                          │
│ Instance 2 ──┼─▶ Load balanced across Kafka partitions  │
│ Instance 3 ──┘                                          │
│                                                          │
│ Each instance handles different partitions:              │
│ - Instance 1: Partitions 0, 3                           │
│ - Instance 2: Partitions 1, 4                           │
│ - Instance 3: Partitions 2, 5                           │
└──────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────┐
│ payment-decision-group (Confirm/Rollback phase)          │
│                                                          │
│ Instance 1 ──▶ Single instance (low volume)             │
│                                                          │
│ Handles all partitions (can add more if needed)          │
└──────────────────────────────────────────────────────────┘
```

## Comparison: Single Listener vs. Two Listeners

### Single Listener (Status-Based)
```
┌─────────────────────────────────────────┐
│ Topic: order-events                     │
└─────────────┬───────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────┐
│ Consumer Group: payment                 │
└─────────────┬───────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────┐
│ OrderConsumer                           │
│ @KafkaListener(groupId="payment")       │
│                                         │
│ onEvent(Order order) {                  │
│   if (order.status == "NEW")            │
│     reserve();                          │
│   else if (order.status == "CONFIRMED") │
│     confirm();                          │
│ }                                       │
└─────────────────────────────────────────┘

Issues:
❌ Runtime string comparison
❌ Single DTO for all states
❌ Mixed concerns
❌ Can't scale phases independently
❌ Single failure point
```

### Two Listeners (Consumer Groups)
```
┌─────────────────────────────────────────┐
│ Topic: order-events                     │
└─────────┬───────────────┬───────────────┘
          │               │
          ▼               ▼
┌─────────────────┐ ┌─────────────────┐
│ Group: payment- │ │ Group: payment- │
│ service-group   │ │ decision-group  │
└────────┬────────┘ └────────┬────────┘
         │                   │
         ▼                   ▼
┌───────────────┐ ┌──────────────────┐
│ OrderEvent    │ │ DecisionEvent    │
│ Consumer      │ │ Consumer         │
│               │ │                  │
│ onEvent(      │ │ onEvent(         │
│   OrderCreated│ │   FinalDecision  │
│   Event)      │ │   Event)         │
│ {             │ │ {                │
│   reserve()   │ │   if (CONFIRMED) │
│ }             │ │     confirm()    │
│               │ │   else           │
│               │ │     rollback()   │
│               │ │ }                │
└───────────────┘ └──────────────────┘

Benefits:
✅ Compile-time type safety
✅ Separate DTOs per event type
✅ Clear separation of concerns
✅ Independent scaling
✅ Independent failure isolation
```

## Idempotency Tracking

```
┌─────────────────────────────────────────────────────────────────┐
│                        PaymentService                            │
│                                                                  │
│  ┌────────────────────────────────────────────────────────┐    │
│  │ Set<String> processedReservations                      │    │
│  │ [                                                       │    │
│  │   "order-123",   // Already processed                  │    │
│  │   "order-456",   // Already processed                  │    │
│  │   "order-789"    // Already processed                  │    │
│  │ ]                                                       │    │
│  └────────────────────────────────────────────────────────┘    │
│                                                                  │
│  ┌────────────────────────────────────────────────────────┐    │
│  │ Set<String> processedDecisions                         │    │
│  │ [                                                       │    │
│  │   "order-123",   // Already confirmed/rolled back      │    │
│  │   "order-456"    // Already confirmed/rolled back      │    │
│  │ ]                                                       │    │
│  └────────────────────────────────────────────────────────┘    │
│                                                                  │
│  When new OrderCreatedEvent arrives:                            │
│  1. Check: if (processedReservations.contains(orderId))        │
│     → Skip (duplicate)                                          │
│  2. Else: Process + add to set                                 │
│                                                                  │
│  When new FinalDecisionEvent arrives:                           │
│  1. Check: if (processedDecisions.contains(orderId))           │
│     → Skip (duplicate)                                          │
│  2. Else: Process + add to set                                 │
└─────────────────────────────────────────────────────────────────┘

Benefits:
- Handles Kafka retries safely
- Prevents double-deduct on confirm
- Prevents double-refund on rollback
- In production: Use Redis with TTL
```

## Customer Balance State Machine

```
Initial State:
┌──────────────────────────┐
│ amountAvailable: 10000   │
│ amountReserved: 0        │
└──────────────────────────┘
              │
              │ OrderCreatedEvent received
              │ processOrderPayment(amount=2000)
              ▼
Reserve State:
┌──────────────────────────┐
│ amountAvailable: 8000    │ ◀── Moved from available
│ amountReserved: 2000     │ ◀── Held in reserve
└──────────────────────────┘
              │
              ├─────────────────┬─────────────────┐
              │                 │                 │
       CONFIRMED          REJECTED          (no decision yet)
              │                 │                 │
              ▼                 ▼                 ▼
Confirm State:          Rollback State:   Pending State:
┌──────────────────┐    ┌──────────────┐  ┌──────────────┐
│ available: 8000  │    │ available:   │  │ available:   │
│ reserved: 0      │    │   10000      │  │   8000       │
└──────────────────┘    │ reserved: 0  │  │ reserved:    │
                        └──────────────┘  │   2000       │
                        Funds returned    └──────────────┘
                        to available      Waiting for
                                         final decision
```

## Error Handling

```
┌─────────────────────────────────────────────────────────────────────┐
│ DecisionEventConsumer.consumeDecisionEvent()                        │
│                                                                     │
│ try {                                                               │
│   paymentService.handleConfirm(event);                             │
│   // Success path                                                  │
│   ✓ Offset committed                                               │
│   ✓ Message marked as processed                                    │
│ } catch (Exception e) {                                             │
│   log.error("Failed to process", e);                               │
│   throw e;  // ← Important!                                        │
│ }                                                                   │
└─────────────────────────────────────────────────────────────────────┘
                    │
                    │ Exception thrown
                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Kafka Consumer Behavior                                             │
│                                                                     │
│ ❌ Offset NOT committed                                             │
│ 🔄 Message will be retried                                         │
│                                                                     │
│ Retry Strategy (configure in application.yml):                      │
│ - Attempt 1: Immediate                                              │
│ - Attempt 2: After 1 second                                         │
│ - Attempt 3: After 2 seconds                                        │
│ - After max retries: Send to Dead Letter Queue (DLQ)               │
└─────────────────────────────────────────────────────────────────────┘
```

## Summary

This visualization demonstrates how two Kafka listeners on the same topic with different consumer groups enable:

1. **Independent Processing** - Reserve and Confirm phases are decoupled
2. **Type Safety** - Compile-time checked DTOs vs runtime string checks
3. **Scalability** - Scale each phase based on load
4. **Resilience** - Failures isolated per consumer group
5. **Clean Architecture** - Clear separation of concerns

The pattern is ideal for implementing SAGA orchestration in microservices where participants need to handle both tentative (reserve) and final (confirm/rollback) operations.
