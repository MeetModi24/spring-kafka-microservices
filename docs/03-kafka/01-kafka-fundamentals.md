# Kafka Fundamentals

## Table of Contents
1. [What is Apache Kafka?](#what-is-apache-kafka)
2. [Core Concepts](#core-concepts)
3. [Kafka Architecture](#kafka-architecture)
4. [Why Kafka for Microservices?](#why-kafka-for-microservices)
5. [Kafka vs Traditional Messaging](#kafka-vs-traditional-messaging)
6. [Hands-On: Exploring Kafka](#hands-on-exploring-kafka)

---

## What is Apache Kafka?

Apache Kafka is a **distributed event streaming platform** used for building real-time data pipelines and streaming applications.

### Key Characteristics
- **High throughput:** Millions of messages per second
- **Fault-tolerant:** Data replicated across multiple brokers
- **Scalable:** Horizontally scalable by adding brokers
- **Durable:** Messages persisted to disk (not just in-memory)
- **Distributed:** Runs as a cluster of servers (brokers)

### Real-World Use Cases
1. **Microservices Communication** (our project) - Services communicate via events
2. **Log Aggregation** - Collecting logs from distributed systems
3. **Stream Processing** - Real-time analytics, fraud detection
4. **Event Sourcing** - Storing state changes as immutable events
5. **Data Integration** - Moving data between systems (Kafka Connect)

---

## Core Concepts

### 1. Topic

A **topic** is a category or feed name to which records are published. Think of it as a database table or a message queue channel.

**Example in our project:**
```
topics:
  - orders           # Order lifecycle events
  - payment-orders   # Payment service responses
  - stock-orders     # Stock service responses
```

**Key Properties:**
- Topics are **append-only** logs
- Messages in a topic are ordered by timestamp
- Topics can be replicated across multiple brokers for fault tolerance
- Topics are divided into **partitions** for parallelism

### 2. Partition

A **partition** is an ordered, immutable sequence of records within a topic. Each partition is stored on disk as a log file.

**Why partitions matter:**
- **Parallelism:** Multiple consumers can read different partitions simultaneously
- **Ordering:** Messages within a partition are strictly ordered
- **Scalability:** More partitions = higher throughput

**Visual Representation:**
```
Topic: orders (3 partitions)

Partition 0: [Msg1] -> [Msg4] -> [Msg7] -> ...
Partition 1: [Msg2] -> [Msg5] -> [Msg8] -> ...
Partition 2: [Msg3] -> [Msg6] -> [Msg9] -> ...
```

**Partitioning Strategy:**
```java
// Messages with the same key go to the same partition
ProducerRecord<String, Order> record = 
    new ProducerRecord<>("orders", order.getCustomerId(), order);
// All orders from customer "C123" will always go to the same partition
// This guarantees ordering per customer
```

### 3. Offset

An **offset** is a unique identifier for each message within a partition. It's a sequential integer starting from 0.

**Key Points:**
- Offsets are **partition-specific** (Partition 0 has offset 0, 1, 2...; Partition 1 has its own 0, 1, 2...)
- Consumers track which offset they've read up to
- Offsets are stored in a special Kafka topic `__consumer_offsets`

**Example:**
```
Partition 0:
Offset: 0    1    2    3    4    5
Data:  [A] -[B]- [C]- [D]- [E]- [F]
              ^
              Consumer last read offset 1, next read is offset 2
```

### 4. Producer

A **producer** is an application that publishes (writes) messages to Kafka topics.

**How it works:**
1. Producer creates a `ProducerRecord(topic, key, value)`
2. Producer serializes key and value to bytes
3. Partitioner determines which partition to send to (based on key or round-robin)
4. Producer sends to the leader broker for that partition
5. Broker writes to disk and replicates to followers
6. Broker sends acknowledgment back to producer

**Spring Kafka Example:**
```java
@Service
public class OrderProducer {
    @Autowired
    private KafkaTemplate<String, Order> kafkaTemplate;

    public void publishOrder(Order order) {
        // Async send (returns CompletableFuture)
        kafkaTemplate.send("orders", order.getOrderId(), order);
    }
}
```

### 5. Consumer

A **consumer** is an application that subscribes to (reads) messages from Kafka topics.

**How it works:**
1. Consumer subscribes to one or more topics
2. Kafka assigns partitions to the consumer
3. Consumer fetches messages from assigned partitions
4. Consumer processes messages and commits offset (marks as read)

**Spring Kafka Example:**
```java
@Service
public class PaymentConsumer {
    @KafkaListener(topics = "orders", groupId = "payment-service")
    public void consume(Order order) {
        // Process order
        paymentService.reserveFunds(order);
    }
}
```

### 6. Consumer Group

A **consumer group** is a group of consumers that cooperate to consume messages from a topic. Each partition is read by **only one consumer** within the group.

**Why consumer groups?**
- **Load balancing:** Distribute partitions across consumers for parallel processing
- **Fault tolerance:** If a consumer crashes, its partitions are reassigned to others
- **Scalability:** Add more consumers to handle higher load (up to # of partitions)

**Visual Representation:**
```
Topic: orders (3 partitions)
Consumer Group: payment-service

Partition 0  -->  Consumer A
Partition 1  -->  Consumer B
Partition 2  -->  Consumer C

If Consumer B crashes:
Partition 0  -->  Consumer A
Partition 1  -->  Consumer A or C (rebalanced)
Partition 2  -->  Consumer C
```

**Important Rule:**
- Same consumer group: Messages are **load-balanced** (each message read by one consumer)
- Different consumer groups: Messages are **broadcast** (each group receives all messages)

**Example:**
```
Topic: orders

Consumer Group "payment-service":
  - Consumer P1 reads partitions 0, 1
  - Consumer P2 reads partition 2

Consumer Group "stock-service":
  - Consumer S1 reads partitions 0, 1, 2

Result: Both groups receive ALL messages, but within each group, messages are split
```

### 7. Broker

A **broker** is a Kafka server that stores data and serves clients (producers and consumers).

**Key Responsibilities:**
- Persist messages to disk
- Replicate data to other brokers
- Serve fetch requests from consumers
- Manage partition leadership

**Broker Cluster:**
```
Cluster: 3 brokers

Broker 1 (Leader for orders-0)
Broker 2 (Leader for orders-1, Follower for orders-0)
Broker 3 (Leader for orders-2, Follower for orders-0, orders-1)
```

### 8. Replication

**Replication** ensures fault tolerance by copying each partition to multiple brokers.

**Key Concepts:**
- **Replication Factor:** Number of copies (typically 3 in production)
- **Leader:** One broker handles all reads/writes for a partition
- **Followers (ISR - In-Sync Replicas):** Brokers that have replicated the latest data
- **Failover:** If leader fails, a follower is promoted to leader

**Example:**
```
Topic: orders, Partition 0, Replication Factor: 3

Broker 1: Leader (handles all reads/writes)
Broker 2: Follower (replicates from leader)
Broker 3: Follower (replicates from leader)

If Broker 1 crashes:
Broker 2: Promoted to leader
Broker 3: Now follows Broker 2
```

---

## Kafka Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Kafka Cluster                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │   Broker 1   │  │   Broker 2   │  │   Broker 3   │          │
│  │              │  │              │  │              │          │
│  │ Partition 0  │  │ Partition 1  │  │ Partition 2  │          │
│  │  (Leader)    │  │  (Leader)    │  │  (Leader)    │          │
│  │ Partition 1  │  │ Partition 2  │  │ Partition 0  │          │
│  │  (Follower)  │  │  (Follower)  │  │  (Follower)  │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
└─────────────────────────────────────────────────────────────────┘
           ↑                                        ↓
    ┌──────────────┐                       ┌──────────────┐
    │  Producers   │                       │  Consumers   │
    │              │                       │              │
    │ order-svc    │                       │ payment-svc  │
    │ stock-svc    │                       │ stock-svc    │
    └──────────────┘                       └──────────────┘
           
┌──────────────────────────────────────────────────────────────────┐
│                        Zookeeper                                 │
│  (Manages broker metadata, leader election, configuration)       │
└──────────────────────────────────────────────────────────────────┘
```

### Data Flow

**1. Producer sends message:**
```
Producer -> Serializer -> Partitioner -> Network -> Broker (Leader) -> Disk
                                                    |
                                                    v
                                            Replicate to Followers
```

**2. Consumer receives message:**
```
Consumer -> Fetch Request -> Broker (Leader) -> Read from Disk -> 
Deserializer -> Application Code -> Commit Offset
```

### Zookeeper (Legacy, being replaced by KRaft)

**What is Zookeeper?**
- A centralized service for maintaining configuration and coordination
- Manages Kafka cluster metadata (broker list, topic configs, ACLs)
- Performs leader election when a broker fails

**Why it's being replaced:**
- Adds operational complexity (separate system to manage)
- Single point of failure
- KRaft (Kafka Raft) removes Zookeeper dependency (GA in Kafka 3.3+)

**In our docker-compose.yml:**
```yaml
zookeeper:
  image: confluentinc/cp-zookeeper:7.6.1
  environment:
    ZOOKEEPER_CLIENT_PORT: 2181

kafka:
  image: confluentinc/cp-kafka:7.6.1
  environment:
    KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
```

---

## Why Kafka for Microservices?

### Traditional Synchronous Communication (REST)

**Problems:**
1. **Tight coupling:** Service A directly calls Service B (B must be available)
2. **Cascading failures:** If B is down, A fails
3. **Scalability limits:** Synchronous = slow response times under load
4. **No replay:** If message is lost, it's gone forever

**Example:**
```java
// order-service calls payment-service directly
PaymentResponse response = restTemplate.postForObject(
    "http://payment-service/reserve-funds", 
    orderRequest, 
    PaymentResponse.class
);
// If payment-service is down, order-service throws exception
```

### Event-Driven Architecture with Kafka

**Benefits:**
1. **Loose coupling:** Services don't know about each other, only events
2. **Resilience:** If a service is down, messages wait in Kafka
3. **Scalability:** Kafka handles millions of messages/sec
4. **Auditability:** Full event history (event sourcing)
5. **Flexibility:** New services can subscribe to existing events without changes

**Example:**
```java
// order-service publishes event
kafkaTemplate.send("orders", new OrderCreated(orderId, customerId));

// payment-service subscribes to event (decoupled)
@KafkaListener(topics = "orders")
public void handleOrder(OrderCreated event) {
    paymentService.reserveFunds(event);
}
```

### When to Use Kafka vs REST

| Use Kafka When | Use REST When |
|---|---|
| Asynchronous operations (eventual consistency OK) | Synchronous operations (immediate response needed) |
| High throughput (thousands of messages/sec) | Low traffic (< 100 req/sec) |
| Event-driven workflows (SAGA, event sourcing) | Simple CRUD operations |
| Decoupled services (pub/sub) | Direct service-to-service calls |
| Need message replay (audit, debugging) | Stateless operations |

**Our Project Uses Both:**
- **REST:** Client → order-service (create order API)
- **Kafka:** order-service ↔ payment-service ↔ stock-service (internal coordination)

---

## Kafka vs Traditional Messaging

### RabbitMQ (Traditional Message Broker)

| Feature | Kafka | RabbitMQ |
|---|---|---|
| **Model** | Distributed commit log | Message broker |
| **Storage** | Persists all messages to disk | Messages deleted after consumption |
| **Replay** | Yes (consumers can rewind offsets) | No (messages are gone) |
| **Throughput** | 100k+ msg/sec per broker | 20k msg/sec per broker |
| **Ordering** | Per partition (strict) | Per queue (strict) |
| **Use Case** | Event streaming, high throughput | Task queues, routing |

**When to choose Kafka:**
- High throughput (millions of events/day)
- Need event replay (audit, reprocessing)
- Stream processing (Kafka Streams)

**When to choose RabbitMQ:**
- Complex routing (topic exchanges, fanout)
- Priority queues
- Lower message volume

---

## Hands-On: Exploring Kafka

### Setup

Ensure Docker Compose is running:
```bash
cd /Users/mhiteshkumar/spring-kafka-microservices
docker compose up -d
```

**Verify services:**
```bash
docker ps
# Should show: zookeeper, kafka, kafka-ui
```

**Access Kafka UI:**
- URL: http://localhost:8080
- Explore topics, messages, consumer groups

### 1. Create a Topic

**Option A: Using Kafka UI**
1. Open http://localhost:8080
2. Click "Topics" → "Add Topic"
3. Name: `test-topic`, Partitions: 3, Replication Factor: 1

**Option B: Using CLI**
```bash
docker exec -it kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic test-topic \
  --partitions 3 \
  --replication-factor 1
```

**List topics:**
```bash
docker exec -it kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --list
```

### 2. Produce Messages

**Using CLI:**
```bash
docker exec -it kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic test-topic \
  --property "parse.key=true" \
  --property "key.separator=:"

# Type messages (key:value format):
# customer1:{"orderId":"O1","amount":100}
# customer2:{"orderId":"O2","amount":200}
# customer1:{"orderId":"O3","amount":150}
# (Press Ctrl+C to exit)
```

**Why use keys?**
- Messages with the same key go to the same partition
- Ensures ordering for related events (e.g., all events for customer1)

### 3. Consume Messages

**Using CLI (from beginning):**
```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic test-topic \
  --from-beginning \
  --property print.key=true \
  --property key.separator=":"
```

**Consumer Groups (load balancing):**
```bash
# Terminal 1: Consumer in group "my-group"
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic test-topic \
  --group my-group

# Terminal 2: Second consumer in same group
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic test-topic \
  --group my-group

# Produce messages and observe: they're split between consumers!
```

### 4. Describe Topic Details

```bash
docker exec -it kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic test-topic
```

**Output explains:**
- Number of partitions
- Replication factor
- Leader broker for each partition
- In-Sync Replicas (ISR)

### 5. Check Consumer Group Offsets

```bash
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group my-group
```

**Output shows:**
- Current offset (last committed position)
- Log end offset (total messages in partition)
- Lag (how far behind the consumer is)

---

## Key Takeaways

1. **Kafka is a distributed commit log, not a traditional message queue**
   - Messages persist even after consumption (retention policy)
   - Consumers track their own position (offset)

2. **Partitions enable parallelism and ordering**
   - More partitions = higher throughput
   - Ordering is guaranteed only within a partition

3. **Consumer groups provide load balancing**
   - Each partition is read by only one consumer in a group
   - Scale consumers up to the number of partitions

4. **Replication ensures fault tolerance**
   - Leaders handle reads/writes
   - Followers replicate data

5. **Event-driven architecture decouples services**
   - Services communicate via events, not direct calls
   - Kafka acts as the "backbone" for microservices

---

## Next Steps

1. Read [Kafka Configuration](./02-kafka-configuration.md) to understand producer/consumer settings
2. Learn [Producers & Consumers](./03-producers-consumers.md) for Spring Kafka integration
3. Explore [Kafka Streams](./04-kafka-streams.md) for advanced stream processing

---

## Further Reading

- **Apache Kafka Documentation:** https://kafka.apache.org/documentation/
- **Confluent Kafka Guide:** https://docs.confluent.io/platform/current/kafka/introduction.html
- **Spring Kafka Reference:** https://docs.spring.io/spring-kafka/reference/
- **Book:** "Kafka: The Definitive Guide" by Neha Narkhede (O'Reilly)

---

**Project Context:** This documentation supports the Spring Boot + Kafka Microservices learning project. See `../PROJECT-PLAN.md` for the full implementation roadmap.
