# Kafka Error Handling and Fault Tolerance

## Table of Contents
1. [Types of Errors](#types-of-errors)
2. [Producer Error Handling](#producer-error-handling)
3. [Consumer Error Handling](#consumer-error-handling)
4. [Retry Strategies](#retry-strategies)
5. [Dead Letter Queue (DLQ)](#dead-letter-queue-dlq)
6. [Poison Pills](#poison-pills)
7. [Circuit Breaker Pattern](#circuit-breaker-pattern)
8. [Transactional Error Handling](#transactional-error-handling)

---

## Types of Errors

### 1. Transient Errors (Retryable)
Temporary failures that may succeed on retry:
- Network timeouts
- Broker temporarily unavailable
- Leader election in progress
- Rate limiting

**Strategy:** Retry with exponential backoff

### 2. Permanent Errors (Non-Retryable)
Failures that won't succeed even after retry:
- Serialization errors
- Invalid message format
- Authorization failures
- Business validation failures

**Strategy:** Send to Dead Letter Queue (DLQ) or log for manual intervention

### 3. Poison Pills
Messages that repeatedly cause consumer to crash:
- Malformed JSON
- Null pointer in processing logic
- Infinite loop in handler

**Strategy:** Skip after N attempts, send to DLQ

---

## Producer Error Handling

### Basic Error Handling

```java
@Service
public class OrderProducer {
    
    @Autowired
    private KafkaTemplate<String, Order> kafkaTemplate;
    
    public void sendOrder(Order order) {
        CompletableFuture<SendResult<String, Order>> future = 
            kafkaTemplate.send("orders", order.getOrderId(), order);
        
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Order {} sent successfully to partition {}",
                    order.getOrderId(),
                    result.getRecordMetadata().partition());
            } else {
                handleSendFailure(order, ex);
            }
        });
    }
    
    private void handleSendFailure(Order order, Throwable ex) {
        if (ex instanceof RetriableException) {
            log.warn("Transient error for order {}, will retry", order.getOrderId());
            // Kafka will retry automatically based on config
        } else {
            log.error("Non-retryable error for order {}", order.getOrderId(), ex);
            // Store in outbox table or DLQ
            saveToOutbox(order);
        }
    }
}
```

### Producer with Retry Configuration

```yaml
spring:
  kafka:
    producer:
      retries: 3                    # Retry 3 times
      retry-backoff-ms: 1000        # Wait 1 second between retries
      request-timeout-ms: 30000     # Timeout after 30 seconds
      acks: all                     # Wait for all replicas
      enable-idempotence: true      # Prevent duplicates on retry
```

### Manual Retry with Backoff

```java
@Service
public class ResilientOrderProducer {
    
    @Autowired
    private KafkaTemplate<String, Order> kafkaTemplate;
    
    @Retryable(
        value = {RetriableException.class},
        maxAttempts = 5,
        backoff = @Backoff(delay = 1000, multiplier = 2)
    )
    public void sendOrderWithRetry(Order order) throws Exception {
        try {
            kafkaTemplate.send("orders", order.getOrderId(), order).get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RetriableException) {
                throw (RetriableException) e.getCause();
            } else {
                log.error("Non-retryable error", e);
                throw new RuntimeException(e);
            }
        }
    }
    
    @Recover
    public void recover(RetriableException e, Order order) {
        log.error("Failed to send order {} after retries", order.getOrderId());
        saveToDeadLetterQueue(order);
    }
}
```

### Fallback to Outbox Pattern

```java
@Service
public class OutboxProducer {
    
    @Autowired
    private KafkaTemplate<String, Order> kafkaTemplate;
    
    @Autowired
    private OutboxRepository outboxRepository;
    
    public void sendOrder(Order order) {
        CompletableFuture<SendResult<String, Order>> future = 
            kafkaTemplate.send("orders", order.getOrderId(), order);
        
        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.warn("Failed to send order {}, saving to outbox", order.getOrderId());
                saveToOutbox(order);
            }
        });
    }
    
    @Transactional
    private void saveToOutbox(Order order) {
        OutboxEvent event = new OutboxEvent(
            UUID.randomUUID().toString(),
            order.getOrderId(),
            "OrderCreated",
            objectMapper.writeValueAsString(order),
            false
        );
        outboxRepository.save(event);
    }
    
    @Scheduled(fixedDelay = 5000)
    public void publishOutboxEvents() {
        List<OutboxEvent> events = outboxRepository.findByPublishedFalse();
        
        for (OutboxEvent event : events) {
            try {
                kafkaTemplate.send("orders", event.getAggregateId(), event.getPayload()).get();
                event.setPublished(true);
                outboxRepository.save(event);
            } catch (Exception e) {
                log.error("Failed to publish outbox event {}", event.getId(), e);
            }
        }
    }
}
```

---

## Consumer Error Handling

### Basic Error Handling

```java
@Service
public class OrderConsumer {
    
    @KafkaListener(topics = "orders", groupId = "payment-service")
    public void consume(Order order, Acknowledgment ack) {
        try {
            processOrder(order);
            ack.acknowledge();  // Commit offset on success
        } catch (Exception e) {
            log.error("Failed to process order {}", order.getOrderId(), e);
            // Don't commit → Kafka will redeliver
            // Consumer will be stuck on this message until fixed
        }
    }
}
```

### Error Handler with Retry

```java
@Configuration
public class KafkaErrorConfig {
    
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Order> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Order> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        
        // Common error handler with retry
        factory.setCommonErrorHandler(new DefaultErrorHandler(
            new FixedBackOff(1000L, 3L)  // Retry 3 times with 1 second delay
        ));
        
        return factory;
    }
}
```

### Handling Different Error Types

```java
@Service
public class ResilientOrderConsumer {
    
    @Autowired
    private KafkaTemplate<String, Order> dlqTemplate;
    
    @KafkaListener(topics = "orders", groupId = "payment-service")
    public void consume(Order order, Acknowledgment ack) {
        try {
            validateOrder(order);
            processOrder(order);
            ack.acknowledge();
            
        } catch (ValidationException e) {
            // Business validation error → Don't retry
            log.error("Invalid order {}: {}", order.getOrderId(), e.getMessage());
            sendToDLQ(order, e);
            ack.acknowledge();  // Skip this message
            
        } catch (InsufficientFundsException e) {
            // Business rejection → Handle as rejection
            log.warn("Insufficient funds for order {}", order.getOrderId());
            publishRejection(order);
            ack.acknowledge();
            
        } catch (TransientException e) {
            // Temporary error → Retry
            log.warn("Transient error for order {}, will retry", order.getOrderId());
            // Don't commit → Kafka will redeliver
            
        } catch (Exception e) {
            // Unknown error → Log and skip after N attempts
            log.error("Unexpected error for order {}", order.getOrderId(), e);
            handleUnexpectedError(order, e);
        }
    }
}
```

---

## Retry Strategies

### 1. Kafka Native Retry (Blocking)

Consumer retries immediately (blocks partition processing).

```java
@Configuration
public class KafkaRetryConfig {
    
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Order> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Order> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        
        // Retry 3 times with exponential backoff
        factory.setCommonErrorHandler(new DefaultErrorHandler(
            new ExponentialBackOff(1000L, 2.0)  // Start at 1s, double each time
        ) {
            @Override
            public void handleRemaining(Exception thrownException, List<ConsumerRecord<?, ?>> records,
                                       Consumer<?, ?> consumer, MessageListenerContainer container) {
                // After all retries exhausted
                records.forEach(record -> {
                    log.error("Failed after retries: {}", record.value());
                    sendToDLQ(record);
                });
            }
        });
        
        return factory;
    }
}
```

**Pros:** Simple, built-in  
**Cons:** Blocks partition (other messages wait)

### 2. Non-Blocking Retry (Retry Topics)

Failed messages sent to retry topic, partition continues processing.

```yaml
# application.yml
spring:
  kafka:
    consumer:
      enable-auto-commit: false
```

```java
@Service
public class NonBlockingRetryConsumer {
    
    @KafkaListener(topics = "orders", groupId = "payment-service")
    @RetryableTopic(
        attempts = "4",  // 1 initial + 3 retries
        backoff = @Backoff(delay = 1000, multiplier = 2, maxDelay = 10000),
        autoCreateTopics = "true",
        include = {RetriableException.class}
    )
    public void consume(Order order) {
        if (shouldFail(order)) {
            throw new RetriableException("Temporary failure");
        }
        processOrder(order);
    }
    
    @DltHandler
    public void handleDlt(Order order, 
                         @Header(KafkaHeaders.EXCEPTION_MESSAGE) String exception) {
        log.error("Order {} sent to DLT after retries: {}", 
            order.getOrderId(), exception);
        alertOps(order, exception);
    }
}
```

**Topics created:**
```
orders                    ← Original topic
orders-retry-1000         ← Retry 1 (1 second delay)
orders-retry-2000         ← Retry 2 (2 second delay)
orders-retry-4000         ← Retry 3 (4 second delay)
orders-dlt                ← Dead letter topic
```

**Flow:**
```
orders → Fail → orders-retry-1000 (wait 1s) → Fail → orders-retry-2000 (wait 2s) 
→ Fail → orders-retry-4000 (wait 4s) → Fail → orders-dlt
```

**Pros:** Non-blocking, partition continues  
**Cons:** More complex, more topics

### 3. Custom Retry Logic

```java
@Service
public class CustomRetryConsumer {
    
    @Autowired
    private RetryAttemptRepository retryRepo;
    
    @Autowired
    private KafkaTemplate<String, Order> kafkaTemplate;
    
    @KafkaListener(topics = "orders", groupId = "payment-service")
    public void consume(Order order, Acknowledgment ack) {
        int attempts = retryRepo.getAttempts(order.getOrderId());
        
        try {
            processOrder(order);
            retryRepo.delete(order.getOrderId());
            ack.acknowledge();
            
        } catch (RetriableException e) {
            if (attempts < 3) {
                // Schedule retry
                retryRepo.incrementAttempts(order.getOrderId());
                scheduleRetry(order, attempts + 1);
                ack.acknowledge();  // Don't block partition
            } else {
                // Max retries exceeded
                log.error("Max retries exceeded for order {}", order.getOrderId());
                sendToDLQ(order);
                ack.acknowledge();
            }
        }
    }
    
    private void scheduleRetry(Order order, int attempt) {
        long delay = (long) Math.pow(2, attempt) * 1000;  // Exponential backoff
        scheduler.schedule(() -> {
            kafkaTemplate.send("orders", order.getOrderId(), order);
        }, delay, TimeUnit.MILLISECONDS);
    }
}
```

---

## Dead Letter Queue (DLQ)

### What is a DLQ?

A separate topic for messages that failed processing after all retries.

**Use cases:**
- Messages that cause exceptions
- Messages that exceed retry limit
- Poison pills
- Business validation failures

### DLQ Implementation

```java
@Service
public class DLQHandler {
    
    @Autowired
    private KafkaTemplate<String, Order> dlqTemplate;
    
    public void sendToDLQ(Order order, Exception exception) {
        // Add metadata about failure
        ProducerRecord<String, Order> record = 
            new ProducerRecord<>("orders-dlq", order.getOrderId(), order);
        
        record.headers()
            .add("exception", exception.getMessage().getBytes())
            .add("exception-class", exception.getClass().getName().getBytes())
            .add("timestamp", String.valueOf(System.currentTimeMillis()).getBytes())
            .add("original-topic", "orders".getBytes());
        
        dlqTemplate.send(record);
        log.info("Order {} sent to DLQ", order.getOrderId());
    }
}

// Monitor DLQ
@KafkaListener(topics = "orders-dlq", groupId = "dlq-monitor")
public void monitorDLQ(Order order, ConsumerRecord<String, Order> record) {
    log.error("DLQ message: order={}, exception={}", 
        order.getOrderId(),
        new String(record.headers().lastHeader("exception").value()));
    
    // Alert operations team
    alertOps(order, record.headers());
}
```

### DLQ Reprocessing

```java
@Service
public class DLQReprocessor {
    
    @Autowired
    private KafkaTemplate<String, Order> kafkaTemplate;
    
    public void reprocessDLQMessage(String orderId) {
        // Read from DLQ
        Order order = consumeFromDLQ(orderId);
        
        // Fix the issue (e.g., update data, fix code)
        // ...
        
        // Republish to original topic
        kafkaTemplate.send("orders", orderId, order);
        log.info("Reprocessing order {} from DLQ", orderId);
    }
}
```

---

## Poison Pills

### What is a Poison Pill?

A message that repeatedly causes the consumer to crash, blocking partition processing.

**Example:**
```json
{
  "orderId": "O123",
  "customerId": null,  ← NPE in consumer
  "amount": -100       ← Invalid value
}
```

### Detecting Poison Pills

```java
@Service
public class PoisonPillDetector {
    
    private Map<String, Integer> failureCount = new ConcurrentHashMap<>();
    
    @KafkaListener(topics = "orders", groupId = "payment-service")
    public void consume(Order order, Acknowledgment ack) {
        String messageId = order.getOrderId();
        
        try {
            processOrder(order);
            failureCount.remove(messageId);
            ack.acknowledge();
            
        } catch (Exception e) {
            int count = failureCount.getOrDefault(messageId, 0) + 1;
            
            if (count > 3) {
                // Poison pill detected
                log.error("Poison pill detected: {}", messageId);
                sendToDLQ(order, e);
                failureCount.remove(messageId);
                ack.acknowledge();  // Skip this message
            } else {
                failureCount.put(messageId, count);
                // Don't commit → retry
            }
        }
    }
}
```

### Skipping Poison Pills

```java
@Configuration
public class PoisonPillConfig {
    
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Order> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Order> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        
        // Skip poison pills after 3 attempts
        factory.setCommonErrorHandler(new DefaultErrorHandler(
            (record, exception) -> {
                log.error("Poison pill detected, sending to DLQ: {}", record.value());
                sendToDLQ(record, exception);
            },
            new FixedBackOff(1000L, 3L)
        ));
        
        return factory;
    }
}
```

---

## Circuit Breaker Pattern

Prevent cascading failures by stopping calls to failing service.

```java
@Service
public class PaymentService {
    
    private final CircuitBreaker circuitBreaker = 
        CircuitBreaker.ofDefaults("paymentService");
    
    @KafkaListener(topics = "orders", groupId = "payment-service")
    public void consume(Order order, Acknowledgment ack) {
        try {
            // Wrap in circuit breaker
            circuitBreaker.executeSupplier(() -> {
                processPayment(order);
                return null;
            });
            
            ack.acknowledge();
            
        } catch (CallNotPermittedException e) {
            // Circuit is open → Service unavailable
            log.warn("Circuit breaker open for order {}", order.getOrderId());
            // Don't commit → retry later when circuit closes
            
        } catch (Exception e) {
            log.error("Failed to process order {}", order.getOrderId(), e);
            sendToDLQ(order, e);
            ack.acknowledge();
        }
    }
}
```

**Configuration:**
```java
CircuitBreakerConfig config = CircuitBreakerConfig.custom()
    .failureRateThreshold(50)                    // Open if 50% fail
    .waitDurationInOpenState(Duration.ofSeconds(30))  // Wait 30s before retry
    .slidingWindowSize(10)                       // Track last 10 calls
    .build();
```

---

## Transactional Error Handling

### Transactional Producer

```java
@Service
public class TransactionalOrderService {
    
    @Autowired
    private KafkaTemplate<String, Order> kafkaTemplate;
    
    @Transactional("kafkaTransactionManager")
    public void processOrders(List<Order> orders) {
        try {
            for (Order order : orders) {
                kafkaTemplate.send("orders", order.getOrderId(), order);
            }
            // All messages committed together
        } catch (Exception e) {
            // All messages rolled back
            log.error("Transaction failed, rolling back", e);
            throw e;
        }
    }
}
```

### Transactional Consumer

```yaml
spring:
  kafka:
    consumer:
      isolation-level: read_committed  # Only read committed transactions
```

---

## Best Practices

1. **Always handle exceptions in consumers** - Don't let them propagate
2. **Use manual offset commits** for critical data
3. **Implement idempotency** - Assume messages may be redelivered
4. **Monitor DLQ** - Alert on messages in DLQ
5. **Use retry topics** for non-blocking retries
6. **Add metadata to DLQ messages** - Exception, timestamp, original topic
7. **Test failure scenarios** - Simulate network errors, poison pills
8. **Set max retries** - Don't retry forever
9. **Log extensively** - Track message flow through retries

---

## Next Steps

1. [Testing Strategies](./06-testing-strategies.md) - Test error handling
2. [Monitoring](./10-monitoring-observability.md) - Monitor errors and DLQ
3. [Idempotency](./08-idempotency-transactions.md) - Handle duplicates

---

**Project Context:** Our microservices use manual commits and retry topics to handle failures in the SAGA orchestration.
