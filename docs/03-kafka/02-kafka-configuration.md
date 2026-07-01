# Kafka Configuration Guide

## Table of Contents
1. [Configuration Overview](#configuration-overview)
2. [Broker Configuration](#broker-configuration)
3. [Producer Configuration](#producer-configuration)
4. [Consumer Configuration](#consumer-configuration)
5. [Spring Kafka Configuration](#spring-kafka-configuration)
6. [Common Configuration Scenarios](#common-configuration-scenarios)
7. [Configuration Best Practices](#configuration-best-practices)

---

## Configuration Overview

Kafka configuration happens at three levels:

1. **Broker Configuration** - Server-side settings (defined in `docker-compose.yml` for our project)
2. **Producer Configuration** - How producers send messages
3. **Consumer Configuration** - How consumers read messages

All configurations can be set via:
- **Spring Boot `application.yml`** (preferred for our project)
- **Java code** (`@Bean` configurations)
- **Environment variables** (for Docker deployments)

---

## Broker Configuration

### Our Docker Compose Setup

```yaml
# /Users/mhiteshkumar/spring-kafka-microservices/docker-compose.yml
kafka:
  image: confluentinc/cp-kafka:7.6.1
  environment:
    # Broker Identity
    KAFKA_BROKER_ID: 1
    # Each broker in a cluster needs a unique ID
    
    # Zookeeper Connection
    KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
    # Connects to Zookeeper for cluster coordination
    
    # Listener Configuration
    KAFKA_LISTENERS: INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092
    # INTERNAL: For inter-broker and container-to-kafka communication
    # EXTERNAL: For host machine (Spring Boot apps) to Kafka
    
    KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:29092,EXTERNAL://localhost:9092
    # What addresses clients should use to connect
    # Spring Boot apps use localhost:9092
    
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
    # No security (PLAINTEXT) for local development
    # In production, use SASL_SSL or SSL
    
    KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
    # Brokers communicate using INTERNAL listener
    
    # Replication Settings
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    # __consumer_offsets topic replication (1 for single-broker dev setup)
    # In production with 3 brokers, set to 3
    
    KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
    KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
    # Transaction log settings (for exactly-once semantics)
    
    # Topic Auto-Creation
    KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    # Creates topics automatically when first message is produced
    # In production, set to "false" and create topics explicitly
```

### Key Broker Settings Explained

#### 1. Listeners vs Advertised Listeners

**Listeners:** What interfaces/ports Kafka binds to
```
KAFKA_LISTENERS: INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092
```
- `0.0.0.0` = bind to all network interfaces
- INTERNAL on port 29092, EXTERNAL on port 9092

**Advertised Listeners:** What clients should connect to
```
KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:29092,EXTERNAL://localhost:9092
```
- Docker containers use `kafka:29092` (via Docker network)
- Host apps use `localhost:9092`

**Common Mistake:**
```yaml
# WRONG: Spring Boot can't connect
KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:9092

# RIGHT: Spring Boot connects to localhost:9092
KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:29092,EXTERNAL://localhost:9092
```

#### 2. Replication Factor

Controls data durability:
```
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1   # Dev setup
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3   # Production (3 brokers)
```

**Why 3 in production?**
- Tolerates 1 broker failure (leader + 2 replicas)
- Provides fault tolerance

**Our setup (1 broker):**
- Replication factor = 1 (only one broker)
- No fault tolerance (if broker dies, data is lost)
- Fine for local development

#### 3. Auto Create Topics

```
KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
```

**Development:** Convenient (topics created on first produce)
**Production:** Disable and pre-create topics with proper config (partitions, replication)

```bash
# Production: Create topics explicitly
kafka-topics --create --topic orders \
  --partitions 10 \
  --replication-factor 3 \
  --config retention.ms=604800000  # 7 days
```

---

## Producer Configuration

### Spring Boot application.yml

```yaml
# /Users/mhiteshkumar/spring-kafka-microservices/order-service/src/main/resources/application.yml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    # Kafka broker address (comma-separated for multiple brokers)
    
    producer:
      # Serialization
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      
      # Acknowledgments
      acks: all
      
      # Retries
      retries: 3
      
      # Batching (optional, for performance)
      batch-size: 16384
      linger-ms: 10
      
      # Compression (optional)
      compression-type: snappy
      
      # Idempotence (exactly-once)
      enable-idempotence: true
```

### Configuration Parameters Explained

#### 1. bootstrap-servers

```yaml
bootstrap-servers: localhost:9092
```

**Purpose:** Initial connection to Kafka cluster
**How it works:**
1. Producer connects to `localhost:9092`
2. Kafka returns metadata (all brokers, topic partitions, leaders)
3. Producer connects directly to partition leaders

**Multiple brokers:**
```yaml
bootstrap-servers: kafka1:9092,kafka2:9092,kafka3:9092
# Comma-separated list (only need one reachable broker)
```

#### 2. Serializers

```yaml
key-serializer: org.apache.kafka.common.serialization.StringSerializer
value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
```

**Purpose:** Convert Java objects to bytes for Kafka

**Built-in Serializers:**
- `StringSerializer` - For String keys/values
- `ByteArraySerializer` - For byte[] data
- `IntegerSerializer` - For Integer keys/values
- `JsonSerializer` (Spring Kafka) - For POJOs to JSON

**Example:**
```java
// Order object
Order order = new Order("O1", "C1", 100.0);

// JsonSerializer converts to:
// {"orderId":"O1","customerId":"C1","amount":100.0}

// Then to bytes: [123, 34, 111, 114, 100, 101, ...] for Kafka
```

**Custom Serializer:**
```java
public class OrderSerializer implements Serializer<Order> {
    @Override
    public byte[] serialize(String topic, Order data) {
        // Custom serialization logic (e.g., Avro, Protobuf)
        return data.toBytes();
    }
}
```

#### 3. acks (Acknowledgment Level)

```yaml
acks: all  # Safest option
```

**Options:**

| Value | Meaning | Durability | Performance |
|---|---|---|---|
| `0` | Fire-and-forget (no ack) | Lowest | Fastest |
| `1` | Leader acknowledges | Medium | Fast |
| `all` / `-1` | Leader + all ISR replicas acknowledge | Highest | Slowest |

**When to use:**

**acks=0** (Fire-and-forget)
```yaml
acks: 0
```
- Use case: Metrics, logs (losing a few messages is OK)
- Risk: Messages may be lost if broker fails before writing

**acks=1** (Leader only)
```yaml
acks: 1
```
- Use case: Balanced durability/performance
- Risk: If leader fails before replication, data is lost

**acks=all** (All replicas)
```yaml
acks: all
```
- Use case: Critical data (financial transactions, orders)
- Guarantee: Data replicated to all in-sync replicas before ack
- Our project uses this

**Example:**
```java
// acks=all
kafkaTemplate.send("orders", order).get();  // Blocks until all replicas ack
```

#### 4. retries

```yaml
retries: 3
```

**Purpose:** Retry failed sends (network issues, broker unavailable)

**How it works:**
1. Producer sends message
2. Broker unavailable → Retry 1
3. Still unavailable → Retry 2
4. Still unavailable → Retry 3
5. Fail after 3 attempts

**Retry Configuration:**
```yaml
retries: 3                      # Max retry attempts
retry-backoff-ms: 100          # Wait 100ms between retries
request-timeout-ms: 30000      # Timeout after 30 seconds
```

**Idempotence + Retries:**
```yaml
enable-idempotence: true   # Prevents duplicates on retry
retries: 2147483647        # Infinite retries (safe with idempotence)
```

#### 5. Batching (Performance Optimization)

```yaml
batch-size: 16384    # 16 KB batch
linger-ms: 10        # Wait 10ms to accumulate messages
```

**Purpose:** Group multiple messages into a single network request

**How it works:**
- Producer accumulates messages for 10ms or until batch reaches 16 KB
- Sends batch in one request (reduces network overhead)

**Trade-off:**
- Higher throughput (fewer network calls)
- Higher latency (10ms wait)

**Example:**
```java
// Without batching (linger-ms=0):
kafkaTemplate.send("orders", order1);  // Sends immediately
kafkaTemplate.send("orders", order2);  // Sends immediately
// Result: 2 network requests

// With batching (linger-ms=10):
kafkaTemplate.send("orders", order1);  // Waits 10ms
kafkaTemplate.send("orders", order2);  // Batched together
// Result: 1 network request with 2 messages
```

**When to disable batching:**
```yaml
linger-ms: 0   # Send immediately (low latency use case)
```

#### 6. Compression

```yaml
compression-type: snappy
```

**Options:** `none`, `gzip`, `snappy`, `lz4`, `zstd`

**Comparison:**

| Algorithm | Compression Ratio | Speed | CPU Usage |
|---|---|---|---|
| `none` | 1x | Fastest | None |
| `snappy` | 2-3x | Fast | Low |
| `lz4` | 2-3x | Fastest | Low |
| `gzip` | 4-5x | Slow | High |
| `zstd` | 4-5x | Medium | Medium |

**Recommendation:**
- **snappy** or **lz4** for most use cases (good balance)
- **gzip** or **zstd** for low-bandwidth networks

**Example:**
```yaml
compression-type: snappy
# Original message: 1 KB
# Compressed: ~300-500 bytes
# Saves 50-70% bandwidth
```

#### 7. Idempotence (Exactly-Once Semantics)

```yaml
enable-idempotence: true
```

**Problem without idempotence:**
```
Producer sends message → Broker receives → Ack lost (network issue)
Producer retries → Broker receives AGAIN → Duplicate message
```

**Solution with idempotence:**
```yaml
enable-idempotence: true
# Kafka assigns a unique ID to each message
# Retries send the same ID → Broker deduplicates
```

**Requirements:**
- `acks=all` (or -1)
- `retries > 0`
- `max-in-flight-requests-per-connection ≤ 5`

**Spring Kafka Default (3.0+):**
```yaml
enable-idempotence: true  # Enabled by default
```

---

## Consumer Configuration

### Spring Boot application.yml

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    
    consumer:
      # Consumer Group
      group-id: ${spring.application.name}
      # Example: "order-service"
      
      # Deserialization
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      
      # Auto Offset Commit
      enable-auto-commit: true
      auto-commit-interval-ms: 5000
      
      # Offset Reset Strategy
      auto-offset-reset: earliest
      
      # Trusted Packages (Security)
      properties:
        '[spring.json.trusted.packages]': com.example.orderservice.dto
      
      # Fetch Settings
      max-poll-records: 500
      fetch-min-bytes: 1
      fetch-max-wait-ms: 500
```

### Configuration Parameters Explained

#### 1. group-id

```yaml
group-id: ${spring.application.name}
# Resolves to "order-service"
```

**Purpose:** Identifies the consumer group

**Load Balancing:**
```
Topic: orders (3 partitions)
Consumer Group: payment-service (2 consumers)

Partition 0 → Consumer 1
Partition 1 → Consumer 2
Partition 2 → Consumer 1
```

**Broadcasting:**
```
Topic: orders
Consumer Group "payment-service" → Receives all messages
Consumer Group "stock-service"   → Receives all messages
```

**Dynamic Group ID:**
```java
@KafkaListener(topics = "orders", groupId = "payment-service")
public void consume(Order order) {
    // This listener is part of "payment-service" group
}
```

#### 2. Deserializers

```yaml
key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
```

**Purpose:** Convert bytes from Kafka to Java objects

**Must match producer serializers:**
```
Producer: StringSerializer → Consumer: StringDeserializer ✓
Producer: JsonSerializer   → Consumer: JsonDeserializer   ✓
Producer: JsonSerializer   → Consumer: StringDeserializer ✗ (deserialization error)
```

**JsonDeserializer with Type Mapping:**
```yaml
properties:
  '[spring.json.value.default.type]': com.example.orderservice.dto.Order
  # All messages deserialized to Order class
```

**Handling Multiple Types:**
```java
@KafkaListener(topics = "orders")
public void consume(
    @Payload Order order,
    @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key
) {
    // Order object available directly
}
```

#### 3. Auto Offset Commit

```yaml
enable-auto-commit: true
auto-commit-interval-ms: 5000
```

**How it works:**
- Consumer fetches messages
- Processes messages
- Every 5 seconds, Kafka auto-commits the last read offset

**Risk:**
```java
@KafkaListener(topics = "orders")
public void consume(Order order) {
    processOrder(order);           // Success
    // Auto-commit happens here (5 seconds later)
    saveToDatabase(order);         // Exception thrown
}
// Offset already committed → Message lost (not reprocessed)
```

**Solution: Manual Commit**
```yaml
enable-auto-commit: false
```

```java
@KafkaListener(topics = "orders")
public void consume(Order order, Acknowledgment ack) {
    try {
        processOrder(order);
        saveToDatabase(order);
        ack.acknowledge();  // Commit offset only on success
    } catch (Exception e) {
        // Don't commit → message will be reprocessed
    }
}
```

**When to use auto-commit:**
- Processing is idempotent (safe to reprocess)
- Message loss is acceptable

**When to use manual commit:**
- Critical data (financial transactions)
- Non-idempotent operations (duplicate processing is harmful)

#### 4. auto-offset-reset

```yaml
auto-offset-reset: earliest
```

**Purpose:** What to do when no offset exists (first startup or offset expired)

**Options:**

| Value | Behavior | Use Case |
|---|---|---|
| `earliest` | Read from beginning of topic | Replay all messages |
| `latest` | Read only new messages | Skip old messages |
| `none` | Throw exception | Force explicit offset management |

**Example:**
```
Topic: orders
Messages: [M1, M2, M3, M4, M5]
           ^              ^
         offset 0      offset 4

New consumer starts:
- auto-offset-reset: earliest → Reads M1, M2, M3, M4, M5
- auto-offset-reset: latest   → Waits for M6 (skips M1-M5)
```

**Our project uses `earliest`:**
```yaml
auto-offset-reset: earliest
# Why? For learning, we want to reprocess all events
```

**Production recommendation:**
```yaml
auto-offset-reset: latest
# Skip old messages to avoid overwhelming new consumers
```

#### 5. Trusted Packages (Security)

```yaml
properties:
  '[spring.json.trusted.packages]': com.example.orderservice.dto
```

**Purpose:** Prevent arbitrary code execution via malicious messages

**How JsonDeserializer works:**
1. Kafka message includes type metadata: `{"@class":"com.example.Order",...}`
2. JsonDeserializer deserializes to that class
3. **Security risk:** Attacker could inject `{"@class":"java.lang.Runtime",...}` to execute code

**Solution:**
```yaml
'[spring.json.trusted.packages]': com.example.orderservice.dto
# Only classes in this package are allowed
```

**Wildcard:**
```yaml
'[spring.json.trusted.packages]': com.example.*
# Trust all classes in com.example package
```

**Disable type checking (unsafe):**
```yaml
'[spring.json.trusted.packages]': '*'
# Trust all classes (use only in dev/testing)
```

#### 6. Fetch Settings

```yaml
max-poll-records: 500        # Fetch up to 500 records per poll
fetch-min-bytes: 1           # Return immediately if 1+ byte available
fetch-max-wait-ms: 500       # Wait max 500ms for fetch-min-bytes
```

**Purpose:** Control how consumers fetch data

**max-poll-records:**
```yaml
max-poll-records: 500
# Consumer fetches 500 messages at a time
# Increase for higher throughput (1000+)
# Decrease if processing is slow (100)
```

**fetch-min-bytes + fetch-max-wait-ms:**
```yaml
fetch-min-bytes: 1024        # Wait for 1 KB
fetch-max-wait-ms: 500       # Or wait max 500ms
```

**Scenario:**
- Consumer polls for messages
- Kafka waits until 1 KB accumulated OR 500ms elapsed
- Returns data to consumer

**Low latency:**
```yaml
fetch-min-bytes: 1
fetch-max-wait-ms: 0
# Return immediately (single messages OK)
```

**High throughput:**
```yaml
fetch-min-bytes: 65536       # 64 KB
fetch-max-wait-ms: 1000      # 1 second
# Batch messages for efficiency
```

---

## Spring Kafka Configuration

### Full application.yml Example

```yaml
spring:
  application:
    name: order-service
  
  kafka:
    bootstrap-servers: localhost:9092
    
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      acks: all
      retries: 3
      enable-idempotence: true
      compression-type: snappy
      batch-size: 16384
      linger-ms: 10
      
    consumer:
      group-id: ${spring.application.name}
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      enable-auto-commit: false
      auto-offset-reset: earliest
      max-poll-records: 500
      properties:
        '[spring.json.trusted.packages]': com.example.orderservice.dto
    
    listener:
      ack-mode: manual
      concurrency: 3

logging:
  level:
    org.apache.kafka: WARN
    org.springframework.kafka: DEBUG
```

### Java Configuration (Alternative)

```java
@Configuration
@EnableKafka
public class KafkaConfig {
    
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    
    // Producer Configuration
    @Bean
    public ProducerFactory<String, Order> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.RETRIES_CONFIG, 3);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new DefaultKafkaProducerFactory<>(config);
    }
    
    @Bean
    public KafkaTemplate<String, Order> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
    
    // Consumer Configuration
    @Bean
    public ConsumerFactory<String, Order> consumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "order-service");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.orderservice.dto");
        return new DefaultKafkaConsumerFactory<>(config);
    }
    
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Order> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Order> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(3);  // 3 consumer threads
        factory.getContainerProperties().setAckMode(AckMode.MANUAL);
        return factory;
    }
}
```

---

## Common Configuration Scenarios

### Scenario 1: High Throughput System

```yaml
spring:
  kafka:
    producer:
      acks: 1                        # Leader only (faster)
      compression-type: lz4          # Fast compression
      batch-size: 65536              # 64 KB batches
      linger-ms: 20                  # Wait 20ms for batching
      buffer-memory: 67108864        # 64 MB buffer
      
    consumer:
      max-poll-records: 1000         # Fetch 1000 records
      fetch-min-bytes: 65536         # Wait for 64 KB
      fetch-max-wait-ms: 1000        # Or 1 second
```

### Scenario 2: Low Latency System

```yaml
spring:
  kafka:
    producer:
      acks: 1
      compression-type: none         # No compression overhead
      batch-size: 0                  # No batching
      linger-ms: 0                   # Send immediately
      
    consumer:
      max-poll-records: 1            # Process 1 message at a time
      fetch-min-bytes: 1             # Return immediately
      fetch-max-wait-ms: 0
```

### Scenario 3: Exactly-Once Semantics

```yaml
spring:
  kafka:
    producer:
      acks: all                      # All replicas must ack
      retries: 2147483647            # Infinite retries
      enable-idempotence: true       # Prevent duplicates
      transactional-id: order-producer-${random.uuid}
      
    consumer:
      enable-auto-commit: false      # Manual commits
      isolation-level: read_committed  # Read only committed transactions
    
    listener:
      ack-mode: manual               # Explicit ack control
```

### Scenario 4: Local Development

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      acks: 1                        # Faster than 'all'
      retries: 0                     # Fail fast
      
    consumer:
      auto-offset-reset: earliest    # Replay all messages
      
logging:
  level:
    org.springframework.kafka: DEBUG  # Verbose logging
```

### Scenario 5: Production

```yaml
spring:
  kafka:
    bootstrap-servers: kafka1:9092,kafka2:9092,kafka3:9092
    producer:
      acks: all
      retries: 3
      enable-idempotence: true
      compression-type: snappy
      
    consumer:
      enable-auto-commit: false
      auto-offset-reset: latest      # Skip old messages
      
    listener:
      ack-mode: manual
      
logging:
  level:
    org.apache.kafka: WARN           # Less verbose
```

---

## Configuration Best Practices

### 1. Use Environment-Specific Configs

```yaml
# application.yml (default)
spring:
  kafka:
    bootstrap-servers: localhost:9092

---
# application-dev.yml
spring:
  kafka:
    bootstrap-servers: localhost:9092

---
# application-prod.yml
spring:
  kafka:
    bootstrap-servers: kafka1.prod:9092,kafka2.prod:9092,kafka3.prod:9092
```

### 2. Externalize Sensitive Config

```yaml
# application.yml
spring:
  kafka:
    bootstrap-servers: ${KAFKA_BROKERS}
    producer:
      properties:
        sasl.jaas.config: ${KAFKA_SASL_JAAS_CONFIG}
```

```bash
# .env file
KAFKA_BROKERS=kafka1:9092,kafka2:9092
KAFKA_SASL_JAAS_CONFIG=org.apache.kafka.common.security.plain.PlainLoginModule required username="user" password="pass";
```

### 3. Monitor Configuration via Actuator

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics,kafkaconfig
  
  metrics:
    export:
      prometheus:
        enabled: true
```

```bash
# View Kafka metrics
curl http://localhost:8081/actuator/metrics/kafka.producer.request.latency.avg
```

### 4. Validate Configuration on Startup

```java
@Component
public class KafkaHealthCheck {
    
    @Autowired
    private KafkaTemplate<String, Order> kafkaTemplate;
    
    @EventListener(ApplicationReadyEvent.class)
    public void validateKafkaConnection() {
        try {
            kafkaTemplate.send("health-check", "ping").get(5, TimeUnit.SECONDS);
            log.info("Kafka connection successful");
        } catch (Exception e) {
            log.error("Failed to connect to Kafka", e);
            // Optionally fail application startup
            System.exit(1);
        }
    }
}
```

### 5. Use ConfigurationProperties for Type Safety

```java
@ConfigurationProperties(prefix = "app.kafka")
@Component
public class KafkaProperties {
    private String ordersTopic;
    private String paymentOrdersTopic;
    private int retries;
    
    // Getters and setters
}
```

```yaml
app:
  kafka:
    orders-topic: orders
    payment-orders-topic: payment-orders
    retries: 3
```

---

## Troubleshooting Configuration Issues

### Issue 1: Producer Can't Connect

**Error:**
```
org.apache.kafka.common.errors.TimeoutException: Topic orders not present in metadata after 60000 ms
```

**Solution:**
```yaml
# Check bootstrap-servers matches Docker advertised listener
spring:
  kafka:
    bootstrap-servers: localhost:9092  # NOT kafka:29092
```

### Issue 2: JsonDeserializer Type Error

**Error:**
```
org.springframework.kafka.support.serializer.DeserializationException: 
The class 'com.example.Order' is not in the trusted packages
```

**Solution:**
```yaml
spring:
  kafka:
    consumer:
      properties:
        '[spring.json.trusted.packages]': com.example.*
```

### Issue 3: Consumer Not Receiving Messages

**Check 1: Offset reset**
```yaml
auto-offset-reset: earliest  # Read from beginning
```

**Check 2: Consumer group**
```bash
# List consumer groups
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --list

# Describe group
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group order-service
```

### Issue 4: Messages Not Persisting

**Check acks setting:**
```yaml
spring:
  kafka:
    producer:
      acks: all  # NOT 0 (fire-and-forget)
```

---

## Next Steps

1. Implement producers/consumers: [Producers & Consumers](./03-producers-consumers.md)
2. Add error handling: [Error Handling](./05-error-handling.md)
3. Write tests: [Testing Strategies](./06-testing-strategies.md)

---

**Project Context:** This configuration will be used in our order-service, payment-service, and stock-service microservices.
