# Kafka Producers & Consumers with Spring Kafka

## Table of Contents
1. [Producing Messages](#producing-messages)
2. [Consuming Messages](#consuming-messages)
3. [Message Keys and Partitioning](#message-keys-and-partitioning)
4. [Headers and Metadata](#headers-and-metadata)
5. [Error Handling Basics](#error-handling-basics)
6. [Real-World Examples](#real-world-examples)

---

## Producing Messages

### Basic Producer with KafkaTemplate

```java
package com.example.orderservice.producer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import com.example.orderservice.dto.Order;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
public class OrderProducer {
    
    private final KafkaTemplate<String, Order> kafkaTemplate;
    private static final String TOPIC = "orders";
    
    public OrderProducer(KafkaTemplate<String, Order> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
    
    // Method 1: Fire-and-forget (async, no callback)
    public void sendOrderFireAndForget(Order order) {
        kafkaTemplate.send(TOPIC, order.getOrderId(), order);
        log.info("Order sent (fire-and-forget): {}", order.getOrderId());
    }
    
    // Method 2: Async with callback
    public void sendOrderAsync(Order order) {
        CompletableFuture<SendResult<String, Order>> future = 
            kafkaTemplate.send(TOPIC, order.getOrderId(), order);
        
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Order sent successfully: {} to partition {}",
                    order.getOrderId(),
                    result.getRecordMetadata().partition());
            } else {
                log.error("Failed to send order: {}", order.getOrderId(), ex);
            }
        });
    }
    
    // Method 3: Sync (blocks until ack)
    public void sendOrderSync(Order order) throws Exception {
        SendResult<String, Order> result = 
            kafkaTemplate.send(TOPIC, order.getOrderId(), order).get();
        
        log.info("Order sent synchronously: {} to partition {} at offset {}",
            order.getOrderId(),
            result.getRecordMetadata().partition(),
            result.getRecordMetadata().offset());
    }
}
```

### When to Use Each Method

| Method | Use Case | Pros | Cons |
|---|---|---|---|
| Fire-and-forget | Logs, metrics, non-critical data | Fastest | Can lose messages |
| Async with callback | Most use cases | Fast + notification | Requires callback handling |
| Sync | Critical data, when you need confirmation | Guaranteed delivery | Blocks thread (slow) |

### ProducerRecord Details

```java
import org.apache.kafka.clients.producer.ProducerRecord;

ProducerRecord<String, Order> record = new ProducerRecord<>(
    "orders",                    // topic
    0,                           // partition (optional, null for auto)
    System.currentTimeMillis(),  // timestamp (optional)
    order.getOrderId(),          // key
    order,                       // value
    headers                      // headers (optional)
);

kafkaTemplate.send(record);
```

**Partitioning Logic:**
- If partition specified → Use that partition
- If key provided → Hash(key) % numPartitions
- If no key → Round-robin across partitions

### Advanced Producer: With Headers

```java
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

public void sendOrderWithHeaders(Order order) {
    Message<Order> message = MessageBuilder
        .withPayload(order)
        .setHeader(KafkaHeaders.TOPIC, "orders")
        .setHeader(KafkaHeaders.MESSAGE_KEY, order.getOrderId())
        .setHeader("event-type", "ORDER_CREATED")
        .setHeader("source-service", "order-service")
        .setHeader("correlation-id", UUID.randomUUID().toString())
        .build();
    
    kafkaTemplate.send(message);
}
```

### Transactional Producer (Exactly-Once)

```yaml
# application.yml
spring:
  kafka:
    producer:
      transaction-id-prefix: order-tx-
```

```java
@Service
public class TransactionalOrderProducer {
    
    @Autowired
    private KafkaTemplate<String, Order> kafkaTemplate;
    
    @Transactional("kafkaTransactionManager")
    public void sendOrdersTransactionally(List<Order> orders) {
        for (Order order : orders) {
            kafkaTemplate.send("orders", order.getOrderId(), order);
        }
        // All messages committed together (atomic)
        // If any fails, all rollback
    }
}
```

### Producer with Custom Callback

```java
public void sendOrderWithCallback(Order order) {
    ListenableFuture<SendResult<String, Order>> future = 
        kafkaTemplate.send("orders", order.getOrderId(), order);
    
    future.addCallback(
        // Success callback
        result -> {
            RecordMetadata metadata = result.getRecordMetadata();
            log.info("Order {} sent to partition {} at offset {}, timestamp {}",
                order.getOrderId(),
                metadata.partition(),
                metadata.offset(),
                metadata.timestamp());
        },
        // Failure callback
        ex -> {
            log.error("Failed to send order {}", order.getOrderId(), ex);
            // Retry logic, alert, DLQ, etc.
            handleSendFailure(order, ex);
        }
    );
}

private void handleSendFailure(Order order, Throwable ex) {
    if (ex instanceof RetriableException) {
        // Retry
        retrySend(order);
    } else {
        // Send to dead letter queue
        kafkaTemplate.send("orders-dlq", order.getOrderId(), order);
    }
}
```

---

## Consuming Messages

### Basic Consumer with @KafkaListener

```java
package com.example.paymentservice.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import com.example.paymentservice.dto.Order;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class OrderConsumer {
    
    @KafkaListener(
        topics = "orders",
        groupId = "payment-service"
    )
    public void consume(Order order) {
        log.info("Received order: {}", order.getOrderId());
        processOrder(order);
    }
    
    private void processOrder(Order order) {
        // Business logic
        log.info("Processing order for customer: {}", order.getCustomerId());
    }
}
```

### Consumer with Manual Offset Commit

```yaml
# application.yml
spring:
  kafka:
    consumer:
      enable-auto-commit: false
    listener:
      ack-mode: manual
```

```java
import org.springframework.kafka.support.Acknowledgment;

@KafkaListener(topics = "orders", groupId = "payment-service")
public void consume(Order order, Acknowledgment ack) {
    try {
        log.info("Processing order: {}", order.getOrderId());
        
        // Business logic (may throw exception)
        reserveFunds(order);
        
        // Commit offset only on success
        ack.acknowledge();
        log.info("Order processed successfully: {}", order.getOrderId());
        
    } catch (InsufficientFundsException e) {
        log.warn("Insufficient funds for order: {}", order.getOrderId());
        // Don't commit → message will be reprocessed
        // Or handle as business rejection (commit + send rejection event)
        ack.acknowledge();
        publishRejection(order, "INSUFFICIENT_FUNDS");
        
    } catch (Exception e) {
        log.error("Failed to process order: {}", order.getOrderId(), e);
        // Don't commit → Kafka will redeliver
        // Optionally: retry with backoff, send to DLQ after N attempts
    }
}
```

### Consumer with Metadata

```java
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

@KafkaListener(topics = "orders", groupId = "payment-service")
public void consume(
    @Payload Order order,
    @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
    @Header(KafkaHeaders.OFFSET) long offset,
    @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp,
    @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
    @Header(value = "correlation-id", required = false) String correlationId
) {
    log.info("Received order {} from partition {} at offset {} (key: {})",
        order.getOrderId(), partition, offset, key);
    
    if (correlationId != null) {
        log.info("Correlation ID: {}", correlationId);
    }
}
```

### Batch Listener

```yaml
# application.yml
spring:
  kafka:
    listener:
      type: batch
    consumer:
      max-poll-records: 100
```

```java
import org.springframework.kafka.annotation.KafkaListener;
import java.util.List;

@KafkaListener(topics = "orders", groupId = "payment-service")
public void consumeBatch(List<Order> orders) {
    log.info("Received batch of {} orders", orders.size());
    
    // Process all orders in batch
    for (Order order : orders) {
        processOrder(order);
    }
    
    // Offset committed after processing entire batch
}
```

**Batch with Metadata:**
```java
import org.springframework.kafka.support.KafkaHeaders;

@KafkaListener(topics = "orders", groupId = "payment-service")
public void consumeBatch(
    @Payload List<Order> orders,
    @Header(KafkaHeaders.RECEIVED_PARTITION_ID) List<Integer> partitions,
    @Header(KafkaHeaders.OFFSET) List<Long> offsets
) {
    for (int i = 0; i < orders.size(); i++) {
        log.info("Order: {}, Partition: {}, Offset: {}",
            orders.get(i).getOrderId(),
            partitions.get(i),
            offsets.get(i));
    }
}
```

### Multiple Listeners (Different Consumer Groups)

```java
@Service
public class OrderEventHandler {
    
    // Payment service consumes orders
    @KafkaListener(
        topics = "orders",
        groupId = "payment-service"
    )
    public void handleForPayment(Order order) {
        log.info("[PAYMENT] Processing order: {}", order.getOrderId());
        reserveFunds(order);
    }
    
    // Stock service also consumes orders (different group)
    @KafkaListener(
        topics = "orders",
        groupId = "stock-service"
    )
    public void handleForStock(Order order) {
        log.info("[STOCK] Processing order: {}", order.getOrderId());
        reserveStock(order);
    }
    
    // Analytics service (read-only)
    @KafkaListener(
        topics = "orders",
        groupId = "analytics-service"
    )
    public void handleForAnalytics(Order order) {
        log.info("[ANALYTICS] Recording order: {}", order.getOrderId());
        updateDashboard(order);
    }
}
```

### Filtering Messages

```java
@KafkaListener(
    topics = "orders",
    groupId = "payment-service",
    containerFactory = "filteringListenerFactory"
)
public void consumeFilteredOrders(Order order) {
    // Only receives orders matching filter
}

@Bean
public ConcurrentKafkaListenerContainerFactory<String, Order> filteringListenerFactory() {
    ConcurrentKafkaListenerContainerFactory<String, Order> factory = 
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory());
    
    // Only consume orders with status=NEW
    factory.setRecordFilterStrategy(record -> {
        Order order = record.value();
        return !order.getStatus().equals("NEW");
    });
    
    return factory;
}
```

### Pausing and Resuming Consumers

```java
@Service
public class ConsumerController {
    
    @Autowired
    private KafkaListenerEndpointRegistry registry;
    
    public void pauseConsumer(String listenerId) {
        MessageListenerContainer container = 
            registry.getListenerContainer(listenerId);
        container.pause();
        log.info("Consumer {} paused", listenerId);
    }
    
    public void resumeConsumer(String listenerId) {
        MessageListenerContainer container = 
            registry.getListenerContainer(listenerId);
        container.resume();
        log.info("Consumer {} resumed", listenerId);
    }
}

@KafkaListener(
    id = "order-consumer",  // Use this ID to pause/resume
    topics = "orders",
    groupId = "payment-service"
)
public void consume(Order order) {
    processOrder(order);
}
```

---

## Message Keys and Partitioning

### Why Message Keys Matter

**Without keys (null key):**
```java
kafkaTemplate.send("orders", null, order);
// Messages distributed round-robin across partitions
```

```
Partition 0: Order1, Order4, Order7
Partition 1: Order2, Order5, Order8
Partition 2: Order3, Order6, Order9
```

**No ordering guarantee across partitions.**

**With keys (same key → same partition):**
```java
kafkaTemplate.send("orders", order.getCustomerId(), order);
// All orders from same customer go to same partition
```

```
Customer A (key="A"):
Partition 0: OrderA1, OrderA2, OrderA3  ← All in order

Customer B (key="B"):
Partition 1: OrderB1, OrderB2, OrderB3  ← All in order

Customer C (key="C"):
Partition 2: OrderC1, OrderC2, OrderC3  ← All in order
```

**Ordering guaranteed per customer.**

### Custom Partitioner

```java
public class CustomPartitioner implements Partitioner {
    
    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                        Object value, byte[] valueBytes, Cluster cluster) {
        
        // Get available partitions
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        
        // Custom partitioning logic
        if (key instanceof String) {
            String customerId = (String) key;
            
            // VIP customers → Partition 0
            if (customerId.startsWith("VIP")) {
                return 0;
            }
            
            // Premium customers → Partition 1
            if (customerId.startsWith("PREMIUM")) {
                return 1;
            }
            
            // Regular customers → Hash-based
            return Math.abs(customerId.hashCode()) % numPartitions;
        }
        
        // Fallback: round-robin
        return ThreadLocalRandom.current().nextInt(numPartitions);
    }
    
    @Override
    public void close() {}
    
    @Override
    public void configure(Map<String, ?> configs) {}
}
```

**Configuration:**
```yaml
spring:
  kafka:
    producer:
      properties:
        partitioner.class: com.example.orderservice.config.CustomPartitioner
```

### Sticky Partitioning (Kafka 2.4+)

Default since Kafka 2.4 for better batching:
```
Without sticky:
Msg1 → P0, Msg2 → P1, Msg3 → P2, Msg4 → P0, ...

With sticky:
Msg1-100 → P0 (until batch full), Msg101-200 → P1, ...
```

**Result:** Better batching, higher throughput.

---

## Headers and Metadata

### Adding Custom Headers

```java
public void sendOrderWithTracing(Order order) {
    ProducerRecord<String, Order> record = new ProducerRecord<>(
        "orders",
        order.getOrderId(),
        order
    );
    
    // Add headers
    record.headers()
        .add("event-type", "ORDER_CREATED".getBytes())
        .add("source", "order-service".getBytes())
        .add("version", "1.0".getBytes())
        .add("correlation-id", UUID.randomUUID().toString().getBytes())
        .add("timestamp", String.valueOf(System.currentTimeMillis()).getBytes());
    
    kafkaTemplate.send(record);
}
```

### Reading Headers in Consumer

```java
import org.apache.kafka.clients.consumer.ConsumerRecord;

@KafkaListener(topics = "orders", groupId = "payment-service")
public void consume(ConsumerRecord<String, Order> record) {
    Order order = record.value();
    
    // Read headers
    record.headers().forEach(header -> {
        log.info("Header: {} = {}", header.key(), new String(header.value()));
    });
    
    // Extract specific header
    Header eventTypeHeader = record.headers().lastHeader("event-type");
    if (eventTypeHeader != null) {
        String eventType = new String(eventTypeHeader.value());
        log.info("Event type: {}", eventType);
    }
}
```

### Use Cases for Headers

1. **Tracing:** correlation-id, span-id, trace-id
2. **Routing:** event-type, source-service
3. **Versioning:** schema-version, message-version
4. **Metadata:** timestamp, user-id, tenant-id
5. **Debugging:** debug-mode, test-flag

---

## Error Handling Basics

### Handling Deserialization Errors

```java
@Bean
public ConcurrentKafkaListenerContainerFactory<String, Order> kafkaListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<String, Order> factory = 
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory());
    
    // Handle deserialization errors
    factory.setErrorHandler((thrownException, data) -> {
        log.error("Failed to deserialize message: {}", data, thrownException);
        // Send to DLQ
        kafkaTemplate.send("orders-dlq", data.key(), data.value());
    });
    
    return factory;
}
```

### Retry with Backoff

```java
@KafkaListener(topics = "orders", groupId = "payment-service")
@RetryableTopic(
    attempts = "3",
    backoff = @Backoff(delay = 1000, multiplier = 2)
)
public void consume(Order order) {
    if (order.getAmount() > 10000) {
        throw new RuntimeException("Amount too high");
    }
    processOrder(order);
}

@DltHandler
public void handleDlt(Order order, @Header(KafkaHeaders.EXCEPTION_MESSAGE) String exceptionMessage) {
    log.error("Order {} sent to DLT: {}", order.getOrderId(), exceptionMessage);
    // Alert, manual intervention, etc.
}
```

See [Error Handling](./05-error-handling.md) for comprehensive error handling patterns.

---

## Real-World Examples

### Example 1: Order Service (Producer)

```java
@RestController
@RequestMapping("/api/orders")
public class OrderController {
    
    @Autowired
    private OrderProducer orderProducer;
    
    @PostMapping
    public ResponseEntity<OrderResponse> createOrder(@RequestBody CreateOrderRequest request) {
        Order order = new Order(
            UUID.randomUUID().toString(),
            request.getCustomerId(),
            request.getItems(),
            OrderStatus.NEW
        );
        
        // Publish to Kafka
        orderProducer.sendOrderAsync(order);
        
        return ResponseEntity.accepted().body(
            new OrderResponse(order.getOrderId(), "Order submitted for processing")
        );
    }
}

@Service
public class OrderProducer {
    
    @Autowired
    private KafkaTemplate<String, Order> kafkaTemplate;
    
    public void sendOrderAsync(Order order) {
        CompletableFuture<SendResult<String, Order>> future = 
            kafkaTemplate.send("orders", order.getCustomerId(), order);
        
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Order {} published to partition {}",
                    order.getOrderId(),
                    result.getRecordMetadata().partition());
            } else {
                log.error("Failed to publish order {}", order.getOrderId(), ex);
                // Retry or save to outbox table
            }
        });
    }
}
```

### Example 2: Payment Service (Consumer + Producer)

```java
@Service
public class PaymentService {
    
    @Autowired
    private CustomerRepository customerRepository;
    
    @Autowired
    private KafkaTemplate<String, PaymentResponse> kafkaTemplate;
    
    @KafkaListener(topics = "orders", groupId = "payment-service")
    public void processOrder(Order order, Acknowledgment ack) {
        try {
            // Check balance
            Customer customer = customerRepository.findById(order.getCustomerId())
                .orElseThrow(() -> new CustomerNotFoundException(order.getCustomerId()));
            
            if (customer.getBalance() >= order.getTotalAmount()) {
                // Reserve funds
                customer.setBalance(customer.getBalance() - order.getTotalAmount());
                customerRepository.save(customer);
                
                // Publish success
                PaymentResponse response = new PaymentResponse(
                    order.getOrderId(),
                    order.getCustomerId(),
                    PaymentStatus.ACCEPT
                );
                kafkaTemplate.send("payment-orders", order.getOrderId(), response);
                log.info("Payment reserved for order {}", order.getOrderId());
                
            } else {
                // Publish rejection
                PaymentResponse response = new PaymentResponse(
                    order.getOrderId(),
                    order.getCustomerId(),
                    PaymentStatus.REJECT,
                    "Insufficient funds"
                );
                kafkaTemplate.send("payment-orders", order.getOrderId(), response);
                log.warn("Payment rejected for order {}: insufficient funds", order.getOrderId());
            }
            
            // Commit offset
            ack.acknowledge();
            
        } catch (Exception e) {
            log.error("Failed to process payment for order {}", order.getOrderId(), e);
            // Don't commit → Kafka will redeliver
        }
    }
}
```

### Example 3: Idempotent Consumer (Handling Duplicates)

```java
@Service
public class IdempotentPaymentService {
    
    @Autowired
    private ProcessedMessageRepository processedRepo;
    
    @KafkaListener(topics = "orders", groupId = "payment-service")
    public void processOrder(Order order, Acknowledgment ack) {
        String messageId = order.getOrderId();
        
        // Check if already processed (idempotency)
        if (processedRepo.existsById(messageId)) {
            log.info("Order {} already processed, skipping", messageId);
            ack.acknowledge();
            return;
        }
        
        try {
            // Process order
            reserveFunds(order);
            
            // Mark as processed
            processedRepo.save(new ProcessedMessage(messageId, Instant.now()));
            
            // Commit offset
            ack.acknowledge();
            
        } catch (Exception e) {
            log.error("Failed to process order {}", messageId, e);
            // Don't commit
        }
    }
}
```

---

## Performance Tips

### 1. Use Async Sending
```java
// Good: Async (non-blocking)
kafkaTemplate.send("orders", order);

// Bad: Sync (blocks thread)
kafkaTemplate.send("orders", order).get();
```

### 2. Batch Processing
```yaml
spring:
  kafka:
    listener:
      type: batch
    consumer:
      max-poll-records: 500
```

### 3. Parallel Consumer Threads
```yaml
spring:
  kafka:
    listener:
      concurrency: 3  # 3 threads per partition
```

### 4. Tune Fetch Settings
```yaml
spring:
  kafka:
    consumer:
      max-poll-records: 500
      fetch-min-bytes: 1024
      fetch-max-wait-ms: 500
```

---

## Next Steps

1. [Kafka Streams](./04-kafka-streams.md) - Stream processing and joins
2. [Error Handling](./05-error-handling.md) - Retries, DLQ, fault tolerance
3. [Testing Strategies](./06-testing-strategies.md) - Unit and integration testing

---

**Project Context:** These patterns are used in order-service, payment-service, and stock-service for event-driven communication.
