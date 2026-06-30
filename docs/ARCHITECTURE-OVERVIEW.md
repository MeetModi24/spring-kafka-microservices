# Architecture Overview
**Spring Boot + Kafka Microservices System**

---

## Current Architecture (Phase 2)

```
┌─────────────────────────────────────────────────────────────┐
│                    Client (Browser/Postman)                 │
└────────────────────────┬────────────────────────────────────┘
                         │
                         │ HTTP REST
                         │
                         ↓
┌─────────────────────────────────────────────────────────────┐
│                    order-service (Port 8081)                │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  OrderController                                     │  │
│  │  • POST /api/orders       (Create order)           │  │
│  │  • GET  /api/orders       (List all)               │  │
│  │  • GET  /api/orders/{id}  (Get by ID)              │  │
│  └────────────────┬─────────────────────────────────────┘  │
│                   │                                          │
│                   ↓                                          │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  OrderService                                        │  │
│  │  • createOrder()     (Business logic)               │  │
│  │  • getOrderById()    (Retrieve order)               │  │
│  │  • getAllOrders()    (List orders)                  │  │
│  └────────────────┬─────────────────────────────────────┘  │
│                   │                                          │
│                   ↓                                          │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  In-Memory Storage (ConcurrentHashMap)              │  │
│  │  Map<String, Order>                                 │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  GlobalExceptionHandler (@RestControllerAdvice)      │  │
│  │  • OrderNotFoundException      → 404               │  │
│  │  • InvalidOrderException        → 400               │  │
│  │  • ValidationException          → 400               │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

---

## Target Architecture (Phase 5+)

```
┌────────────────────────────────────────────────────────────────────────────┐
│                          Frontend (React SPA)                               │
│  • Order creation form    • Order status tracking    • Real-time updates   │
└──────────────────────────────┬─────────────────────────────────────────────┘
                               │ HTTP REST
                               ↓
┌────────────────────────────────────────────────────────────────────────────┐
│                      API Gateway (Spring Cloud Gateway)                     │
│  • Route aggregation  • CORS  • Authentication  • Rate limiting            │
└──────────────────────────────┬─────────────────────────────────────────────┘
                               │
          ┌────────────────────┼────────────────────┐
          │                    │                    │
          ↓                    ↓                    ↓
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│  order-service   │  │ payment-service  │  │  stock-service   │
│  (Port 8080)     │  │  (Kafka only)    │  │  (Kafka only)    │
│                  │  │                  │  │                  │
│  • REST API      │  │  • No REST API   │  │  • No REST API   │
│  • Kafka Streams │  │  • Kafka Consumer│  │  • Kafka Consumer│
│  • State Store   │  │  • JPA + H2      │  │  • JPA + H2      │
│  • SAGA Orch.    │  │  • Balance Mgmt  │  │  • Stock Mgmt    │
└────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘
         │                     │                     │
         │                     │                     │
         └─────────────────────┼─────────────────────┘
                               │
                               ↓
┌────────────────────────────────────────────────────────────────────────────┐
│                        Apache Kafka (Message Broker)                        │
│                                                                             │
│  Topics:                                                                    │
│  ┌──────────────────────────────────────────────────────────────────────┐ │
│  │  orders            (Order events: NEW, CONFIRMED, REJECTED, ROLLBACK)│ │
│  ├──────────────────────────────────────────────────────────────────────┤ │
│  │  payment-orders    (Payment responses: ACCEPT, REJECT)               │ │
│  ├──────────────────────────────────────────────────────────────────────┤ │
│  │  stock-orders      (Stock responses: ACCEPT, REJECT)                 │ │
│  └──────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
│  Infrastructure:                                                            │
│  • Zookeeper (coordination)                                                │
│  • Kafka UI (management)                                                   │
│  • 3 partitions per topic (parallelism)                                    │
└────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────┐
│                              Databases                                      │
│                                                                             │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐        │
│  │  order-service   │  │ payment-service  │  │  stock-service   │        │
│  │  State Store     │  │  H2 Database     │  │  H2 Database     │        │
│  │  (Kafka)         │  │  Customer table  │  │  Product table   │        │
│  └──────────────────┘  └──────────────────┘  └──────────────────┘        │
└────────────────────────────────────────────────────────────────────────────┘
```

---

## Event Flow: SAGA Pattern (Distributed Transaction)

```
Step 1: User Creates Order
──────────────────────────
┌────────┐     POST /orders      ┌──────────────┐
│ Client │ ───────────────────→  │ order-service│
└────────┘                        └──────┬───────┘
                                         │
                                         │ Publish: Order (status=NEW)
                                         ↓
                                  ┌──────────────┐
                                  │    Kafka     │
                                  │ orders topic │
                                  └──────┬───────┘
                                         │
                        ┌────────────────┴────────────────┐
                        │                                 │
                        ↓                                 ↓

Step 2: Services Reserve Resources (Parallel)
──────────────────────────────────────────────
         ┌──────────────────┐        ┌──────────────────┐
         │ payment-service  │        │  stock-service   │
         │                  │        │                  │
         │ • Check balance  │        │ • Check stock    │
         │ • Reserve funds  │        │ • Reserve items  │
         └────────┬─────────┘        └────────┬─────────┘
                  │                            │
                  │ Publish: ACCEPT/REJECT     │ Publish: ACCEPT/REJECT
                  ↓                            ↓
         ┌────────────────────┐       ┌────────────────────┐
         │      Kafka         │       │      Kafka         │
         │ payment-orders     │       │  stock-orders      │
         └────────┬───────────┘       └────────┬───────────┘
                  │                            │
                  └────────────────┬───────────┘
                                   │

Step 3: order-service Joins Responses (Kafka Streams)
──────────────────────────────────────────────────────
                                   ↓
                        ┌──────────────────┐
                        │  order-service   │
                        │  Kafka Streams   │
                        │                  │
                        │  • Join payment  │
                        │  • Join stock    │
                        │  • 10s window    │
                        └────────┬─────────┘
                                 │
                                 │ Decision Logic:
                                 │ If (payment=ACCEPT && stock=ACCEPT)
                                 │    → CONFIRMED
                                 │ Else if (payment=REJECT)
                                 │    → REJECTED (source=PAYMENT)
                                 │ Else if (stock=REJECT)
                                 │    → REJECTED (source=STOCK)
                                 │ Else if (only one responds)
                                 │    → ROLLBACK
                                 │
                                 │ Publish: Final Order Status
                                 ↓
                        ┌──────────────────┐
                        │      Kafka       │
                        │  orders topic    │
                        └────────┬─────────┘
                                 │
                ┌────────────────┴────────────────┐
                │                                 │
                ↓                                 ↓

Step 4: Commit or Compensate (SAGA Compensation)
─────────────────────────────────────────────────
         ┌──────────────────┐        ┌──────────────────┐
         │ payment-service  │        │  stock-service   │
         │                  │        │                  │
         │ If CONFIRMED:    │        │ If CONFIRMED:    │
         │   Commit funds   │        │   Commit stock   │
         │                  │        │                  │
         │ If ROLLBACK:     │        │ If ROLLBACK:     │
         │   Release funds  │        │   Release stock  │
         └──────────────────┘        └──────────────────┘
```

---

## Layered Architecture (Single Service)

```
┌────────────────────────────────────────────────────────────┐
│                   Presentation Layer                        │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  @RestController                                     │  │
│  │  • Handle HTTP requests                             │  │
│  │  • Validate input (@Valid)                          │  │
│  │  • Map Domain → DTO                                 │  │
│  │  • Return HTTP responses                            │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────┬───────────────────────────────────┘
                         │
                         ↓
┌────────────────────────────────────────────────────────────┐
│                   Service Layer                             │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  @Service                                            │  │
│  │  • Business logic                                   │  │
│  │  • Transaction management (@Transactional)          │  │
│  │  • Validation (business rules)                      │  │
│  │  • Orchestrate between repositories                 │  │
│  │  • Publish events                                   │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────┬───────────────────────────────────┘
                         │
                         ↓
┌────────────────────────────────────────────────────────────┐
│                   Data Access Layer                         │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Repository / DAO                                    │  │
│  │  • CRUD operations                                   │  │
│  │  • Database queries                                  │  │
│  │  • Current: ConcurrentHashMap (in-memory)           │  │
│  │  • Future: JpaRepository (database)                 │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────┬───────────────────────────────────┘
                         │
                         ↓
┌────────────────────────────────────────────────────────────┐
│                   Domain Layer                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Domain Models                                       │  │
│  │  • Order, OrderItem, OrderStatus                    │  │
│  │  • Business logic (calculateTotal, isValid)         │  │
│  │  • No dependencies on other layers                  │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────┐
│                Cross-Cutting Concerns (AOP)                 │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  • Exception Handling (@RestControllerAdvice)       │  │
│  │  • Logging                                          │  │
│  │  • Security                                         │  │
│  │  • Metrics                                          │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

---

## Design Patterns Used

### 1. DTO Pattern (Data Transfer Object)
**Purpose:** Separate API contracts from domain models

```
Request DTOs          Domain Model          Response DTOs
─────────────         ──────────           ───────────────
CreateOrderRequest    Order                OrderResponse
OrderItemRequest  →   OrderItem        →   OrderItemResponse
```

**Benefits:**
- API can evolve independently of domain
- Hide internal fields (e.g., internalNotes)
- Validation at API boundary
- Version compatibility

### 2. Repository Pattern
**Purpose:** Abstract data access logic

```
Service Layer
     ↓
Repository Interface
     ↓
Implementation (ConcurrentHashMap → JpaRepository later)
```

**Benefits:**
- Swap storage (in-memory → database) without changing service
- Testable (mock repository)
- Single responsibility

### 3. Layered Architecture
**Purpose:** Separation of concerns

```
Controller → Service → Repository → Domain
```

**Benefits:**
- Each layer has single responsibility
- Easy to test (mock dependencies)
- Changes isolated to specific layer

### 4. Dependency Injection (IoC)
**Purpose:** Inversion of Control

```java
@Service
public class OrderService {
    private final OrderEventProducer eventProducer;
    
    // Spring injects dependency
    public OrderService(OrderEventProducer eventProducer) {
        this.eventProducer = eventProducer;
    }
}
```

**Benefits:**
- Loose coupling
- Testable (inject mocks)
- Spring manages lifecycle

### 5. Global Exception Handling (AOP)
**Purpose:** Cross-cutting concern separation

```
Controller throws → @RestControllerAdvice catches → ErrorResponse
```

**Benefits:**
- Consistent error format
- Clean controllers
- Single place for error logic

### 6. SAGA Pattern (Distributed Transaction)
**Purpose:** Coordinate transactions across services

```
order-service orchestrates:
  payment-service (reserve funds)
  stock-service (reserve stock)
  
If any fails → Compensation (rollback)
```

**Benefits:**
- No distributed locks
- Each service manages own data
- Eventual consistency

### 7. Event-Driven Architecture
**Purpose:** Asynchronous, decoupled communication

```
order-service publishes events
payment-service subscribes
stock-service subscribes
```

**Benefits:**
- Services don't know about each other
- Scale independently
- Add services without changing existing ones

### 8. CQRS (Command Query Responsibility Segregation)
**Purpose:** Separate read and write models

```
Command: POST /orders (writes to Kafka)
Query: GET /orders (reads from Kafka state store)
```

**Benefits:**
- Optimize reads and writes independently
- Scale reads separately
- Event sourcing compatibility

---

## Technology Stack

### Backend
| Component | Technology | Version |
|-----------|-----------|---------|
| Language | Java | 17 |
| Framework | Spring Boot | 3.5.16 |
| Build Tool | Maven | 3.9+ |
| Message Broker | Apache Kafka | Latest (Docker) |
| Coordination | Zookeeper | Latest (Docker) |
| Database (future) | PostgreSQL / H2 | Latest |

### Libraries
| Dependency | Purpose |
|-----------|---------|
| spring-boot-starter-web | REST API |
| spring-kafka | Kafka integration |
| spring-boot-starter-validation | Bean validation |
| spring-boot-starter-data-jpa | Database (Phase 6) |
| spring-cloud-gateway | API Gateway (Phase 7) |
| lombok | Reduce boilerplate |

### Tools
| Tool | Purpose |
|------|---------|
| Docker Compose | Kafka + Zookeeper |
| Kafka UI | Topic management |
| Postman / curl | API testing |
| Maven | Build & dependency management |

---

## Ports & URLs

### Services
| Service | Port | URL |
|---------|------|-----|
| order-service | 8081 | http://localhost:8081 |
| payment-service | TBD | Kafka-only (no REST) |
| stock-service | TBD | Kafka-only (no REST) |
| API Gateway | 8080 | http://localhost:8080 (Phase 7) |

### Infrastructure
| Component | Port | URL |
|-----------|------|-----|
| Kafka Broker | 9092 | localhost:9092 |
| Zookeeper | 2181 | localhost:2181 |
| Kafka UI | 8080 | http://localhost:8080 |

---

## API Endpoints (order-service)

### Current (Phase 2)
```
POST   /api/orders          Create new order
GET    /api/orders          Get all orders
GET    /api/orders/{id}     Get order by ID
```

### Future (Phase 7+)
```
POST   /api/orders          Create new order
GET    /api/orders          Get all orders (paginated)
GET    /api/orders/{id}     Get order by ID
GET    /api/orders/status   Get order status (real-time)
DELETE /api/orders/{id}     Cancel order (compensation)

WebSocket:
ws://localhost:8080/ws/orders/status   Real-time order updates
```

---

## Security Considerations (Future)

### Authentication & Authorization
- Spring Security + JWT
- OAuth2 integration
- Role-based access control (RBAC)

### API Gateway
- Rate limiting
- Request/response logging
- CORS configuration

### Data Security
- Encrypt sensitive data at rest
- TLS for Kafka communication
- Secrets management (Vault)

---

## Monitoring & Observability (Future)

### Metrics
- Spring Boot Actuator
- Prometheus integration
- Grafana dashboards

### Logging
- SLF4J + Logback
- Centralized logging (ELK stack)
- Structured logging (JSON)

### Tracing
- Spring Cloud Sleuth
- Zipkin / Jaeger
- Distributed trace correlation

---

## Scalability Considerations

### Horizontal Scaling
```
Load Balancer
     ↓
order-service (3 instances)
     ↓
Kafka (3 brokers, 3 partitions)
     ↓
payment-service (2 instances)
stock-service (2 instances)
```

### Kafka Partitioning
- 3 partitions per topic
- Key = orderId (same order → same partition)
- Consumer groups for parallel processing

### Database Scaling
- Read replicas
- Connection pooling
- Caching (Redis)

---

This architecture supports:
- ✅ High throughput (Kafka handles millions of events/sec)
- ✅ Fault tolerance (service failures don't cascade)
- ✅ Scalability (add instances without code changes)
- ✅ Maintainability (clear separation of concerns)
- ✅ Testability (each layer independently testable)
