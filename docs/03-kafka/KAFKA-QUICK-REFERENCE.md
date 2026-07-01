# Kafka Quick Reference Guide

## Common Commands

### Topic Management

```bash
# Create topic
docker exec -it kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic orders \
  --partitions 3 \
  --replication-factor 1

# List topics
docker exec -it kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --list

# Describe topic
docker exec -it kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic orders

# Delete topic
docker exec -it kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --delete \
  --topic orders
```

### Producer Commands

```bash
# Produce messages (simple)
docker exec -it kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic orders

# Produce with keys
docker exec -it kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --property "parse.key=true" \
  --property "key.separator=:"
# Format: key:value
```

### Consumer Commands

```bash
# Consume from beginning
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --from-beginning

# Consume with key
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --from-beginning \
  --property print.key=true \
  --property key.separator=":"

# Consume with consumer group
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --group my-group
```

### Consumer Group Commands

```bash
# List consumer groups
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --list

# Describe consumer group
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group payment-service

# Reset offsets to beginning
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group payment-service \
  --topic orders \
  --reset-offsets \
  --to-earliest \
  --execute

# Reset offsets to specific offset
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group payment-service \
  --topic orders:0 \
  --reset-offsets \
  --to-offset 100 \
  --execute
```

---

## Spring Kafka Code Snippets

### Producer

```java
@Service
public class OrderProducer {
    @Autowired
    private KafkaTemplate<String, Order> kafkaTemplate;
    
    // Async
    public void send(Order order) {
        kafkaTemplate.send("orders", order.getOrderId(), order)
            .whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("Sent: {}", order.getOrderId());
                } else {
                    log.error("Failed: {}", order.getOrderId(), ex);
                }
            });
    }
    
    // Sync
    public void sendSync(Order order) throws Exception {
        kafkaTemplate.send("orders", order.getOrderId(), order).get();
    }
}
```

### Consumer

```java
@Service
public class OrderConsumer {
    
    // Basic
    @KafkaListener(topics = "orders", groupId = "payment-service")
    public void consume(Order order) {
        log.info("Received: {}", order.getOrderId());
    }
    
    // With manual commit
    @KafkaListener(topics = "orders", groupId = "payment-service")
    public void consume(Order order, Acknowledgment ack) {
        try {
            processOrder(order);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Failed: {}", order.getOrderId(), e);
            // Don't commit → redeliver
        }
    }
    
    // With metadata
    @KafkaListener(topics = "orders", groupId = "payment-service")
    public void consume(
        @Payload Order order,
        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
        @Header(KafkaHeaders.OFFSET) long offset
    ) {
        log.info("Received {} from partition {} at offset {}", 
            order.getOrderId(), partition, offset);
    }
    
    // Batch
    @KafkaListener(topics = "orders", groupId = "payment-service")
    public void consumeBatch(List<Order> orders) {
        log.info("Received batch of {} orders", orders.size());
        orders.forEach(this::processOrder);
    }
}
```

---

## Configuration Cheat Sheet

### Producer Config

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      acks: all              # Wait for all replicas
      retries: 3             # Retry 3 times
      enable-idempotence: true
      compression-type: snappy
      batch-size: 16384      # 16 KB
      linger-ms: 10          # Wait 10ms for batching
```

### Consumer Config

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: ${spring.application.name}
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      enable-auto-commit: false
      auto-offset-reset: earliest
      max-poll-records: 500
      properties:
        '[spring.json.trusted.packages]': com.example.*
    listener:
      ack-mode: manual
      concurrency: 3
```

---

## Common Patterns

### Idempotent Consumer

```java
@Service
public class IdempotentOrderConsumer {
    @Autowired
    private ProcessedMessageRepository processedRepo;
    
    @KafkaListener(topics = "orders", groupId = "payment-service")
    public void consume(Order order, Acknowledgment ack) {
        if (processedRepo.existsById(order.getOrderId())) {
            log.info("Already processed: {}", order.getOrderId());
            ack.acknowledge();
            return;
        }
        
        try {
            processOrder(order);
            processedRepo.save(new ProcessedMessage(order.getOrderId()));
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Failed: {}", order.getOrderId(), e);
        }
    }
}
```

### Retry with DLQ

```java
@KafkaListener(topics = "orders", groupId = "payment-service")
@RetryableTopic(
    attempts = "3",
    backoff = @Backoff(delay = 1000, multiplier = 2),
    dltTopicSuffix = "-dlt"
)
public void consume(Order order) {
    if (shouldFail(order)) {
        throw new RetryableException("Temporary failure");
    }
    processOrder(order);
}

@DltHandler
public void handleDlt(Order order, @Header(KafkaHeaders.EXCEPTION_MESSAGE) String error) {
    log.error("Order {} sent to DLT: {}", order.getOrderId(), error);
}
```

### Request-Reply Pattern

```java
// Producer
@Service
public class OrderService {
    @Autowired
    private ReplyingKafkaTemplate<String, Order, PaymentResponse> replyingTemplate;
    
    public PaymentResponse processOrder(Order order) throws Exception {
        ProducerRecord<String, Order> record = 
            new ProducerRecord<>("orders", order.getOrderId(), order);
        record.headers().add(KafkaHeaders.REPLY_TOPIC, "payment-replies".getBytes());
        
        RequestReplyFuture<String, Order, PaymentResponse> future = 
            replyingTemplate.sendAndReceive(record);
        
        return future.get(10, TimeUnit.SECONDS).value();
    }
}

// Consumer
@KafkaListener(topics = "orders", groupId = "payment-service")
@SendTo("payment-replies")
public PaymentResponse handleOrder(Order order) {
    return new PaymentResponse(order.getOrderId(), PaymentStatus.ACCEPT);
}
```

---

## Troubleshooting

### Problem: Consumer not receiving messages

**Check 1: Consumer group offset**
```bash
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group payment-service
```

**Check 2: Reset offsets**
```bash
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group payment-service \
  --topic orders \
  --reset-offsets \
  --to-earliest \
  --execute
```

### Problem: Messages not persisting

**Check acks setting:**
```yaml
spring.kafka.producer.acks: all  # NOT 0
```

**Verify topic replication:**
```bash
docker exec -it kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic orders
# Check Replication Factor
```

### Problem: Deserialization error

**Check trusted packages:**
```yaml
spring:
  kafka:
    consumer:
      properties:
        '[spring.json.trusted.packages]': com.example.*
```

### Problem: Consumer lag increasing

**Check 1: Increase concurrency**
```yaml
spring.kafka.listener.concurrency: 5
```

**Check 2: Increase partitions**
```bash
docker exec -it kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --alter \
  --topic orders \
  --partitions 10
```

**Check 3: Tune fetch settings**
```yaml
spring:
  kafka:
    consumer:
      max-poll-records: 1000
      fetch-min-bytes: 65536
```

---

## Performance Tuning

### High Throughput

```yaml
spring:
  kafka:
    producer:
      acks: 1
      compression-type: lz4
      batch-size: 65536
      linger-ms: 20
    consumer:
      max-poll-records: 1000
      fetch-min-bytes: 65536
```

### Low Latency

```yaml
spring:
  kafka:
    producer:
      acks: 1
      compression-type: none
      batch-size: 0
      linger-ms: 0
    consumer:
      max-poll-records: 1
      fetch-min-bytes: 1
```

---

## Monitoring

### Actuator Endpoints

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,metrics
```

**Access:**
```bash
# Health
curl http://localhost:8081/actuator/health

# Kafka metrics
curl http://localhost:8081/actuator/metrics/kafka.producer.request.latency.avg
curl http://localhost:8081/actuator/metrics/kafka.consumer.fetch.latency.avg
```

### Consumer Lag Monitoring

```bash
# Check lag
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group payment-service
```

**Output:**
```
GROUP           TOPIC    PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
payment-service orders   0          100             105             5
payment-service orders   1          200             200             0
payment-service orders   2          150             160             10
```

**Lag = LOG-END-OFFSET - CURRENT-OFFSET**

---

## Testing

### Embedded Kafka

```java
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"orders"})
public class OrderProducerTest {
    
    @Autowired
    private KafkaTemplate<String, Order> kafkaTemplate;
    
    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;
    
    @Test
    public void testSendOrder() {
        Order order = new Order("O1", "C1", 100.0);
        kafkaTemplate.send("orders", order.getOrderId(), order);
        
        // Verify message sent
    }
}
```

### Test Consumer

```java
@KafkaListener(topics = "orders", groupId = "test-consumer")
public void testConsume(Order order) {
    receivedOrders.add(order);
}

@Test
public void testOrderConsumption() throws Exception {
    kafkaTemplate.send("orders", "O1", new Order("O1", "C1", 100.0));
    
    await().atMost(5, SECONDS).until(() -> receivedOrders.size() == 1);
    assertEquals("O1", receivedOrders.get(0).getOrderId());
}
```

---

## Useful Resources

- **Kafka UI:** http://localhost:8080
- **Order Service:** http://localhost:8081
- **Spring Kafka Docs:** https://docs.spring.io/spring-kafka/reference/
- **Kafka Documentation:** https://kafka.apache.org/documentation/

---

## Project-Specific Topics

```
orders           - Order lifecycle events (NEW, CONFIRMED, ROLLBACK)
payment-orders   - Payment service responses (ACCEPT, REJECT)
stock-orders     - Stock service responses (ACCEPT, REJECT)
```

**Architecture:** SAGA Orchestration Pattern
- order-service = Orchestrator
- payment-service = Participant
- stock-service = Participant

See [Event-Driven Patterns](./07-event-driven-patterns.md) for details.
