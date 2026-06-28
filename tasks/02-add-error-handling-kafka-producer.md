# Next Steps - Phase 2: Add Error Handling & Kafka Producer

> **Current Status:** ✅ OrderService REST API working  
> **Phase:** 2 of 6 (Hybrid Approach)  
> **Duration:** 1-2 days

---

## What You've Accomplished (Phase 1)

✅ Spring Boot project structure  
✅ OrderService with REST endpoints (POST, GET, GET all)  
✅ DTO pattern (Request/Response separation)  
✅ Domain model (Order, OrderItem, OrderStatus)  
✅ Constructor-based Dependency Injection  
✅ Bean Validation (@Valid)  
✅ In-memory storage (ConcurrentHashMap)  
✅ Stream-based data transformations  
✅ Comprehensive Java fundamentals documentation

**Working Endpoints:**
- `POST /api/orders` - Create order
- `GET /api/orders/{id}` - Get order by ID
- `GET /api/orders` - Get all orders

---

## Phase 2 Overview: Error Handling + Kafka Producer

**Goals:**
1. Add proper exception handling with custom exceptions
2. Implement global error handling with `@ControllerAdvice`
3. Add Kafka producer to publish `OrderCreatedEvent`
4. Configure Kafka connection
5. Test event publishing

**Why This Order?**
- Error handling first: Foundation for robust REST APIs
- Kafka producer next: Learn publishing before consuming (simpler)
- Sets stage for building payment-service consumer (Phase 3)

---

## Step 1: Add Custom Exceptions (30 mins)

### 1.1 Create Custom Exceptions

**Create:** `order-service/src/main/java/com/example/orderservice/exception/OrderNotFoundException.java`

```java
package com.example.orderservice.exception;

/**
 * Thrown when an order is not found in the system
 * HTTP Status: 404 NOT FOUND
 */
public class OrderNotFoundException extends RuntimeException {
    
    private final String orderId;
    
    public OrderNotFoundException(String orderId) {
        super("Order not found: " + orderId);
        this.orderId = orderId;
    }
    
    public String getOrderId() {
        return orderId;
    }
}
```

**Create:** `order-service/src/main/java/com/example/orderservice/exception/InvalidOrderException.java`

```java
package com.example.orderservice.exception;

/**
 * Thrown when order validation fails
 * HTTP Status: 400 BAD REQUEST
 */
public class InvalidOrderException extends RuntimeException {
    
    public InvalidOrderException(String message) {
        super(message);
    }
    
    public InvalidOrderException(String message, Throwable cause) {
        super(message, cause);
    }
}
```

### 1.2 Update OrderService to Use Custom Exceptions

**File:** `order-service/src/main/java/com/example/orderservice/service/OrderService.java`

```java
// Change this:
if (!order.isValid()) {
    throw new IllegalArgumentException("Order validation failed");
}

// To this:
if (!order.isValid()) {
    throw new InvalidOrderException("Order validation failed: " + 
        "Check that all items have valid productId, quantity > 0, and price > 0");
}

// Change this:
if (order == null) {
    throw new IllegalArgumentException("Id not found");
}

// To this:
if (order == null) {
    throw new OrderNotFoundException(id);
}
```

### 1.3 Test

```bash
# Should return 404 with proper error message
curl http://localhost:8081/api/orders/invalid-id
```

**Expected:** Currently returns 500 Internal Server Error (we'll fix this next)

---

## Step 2: Implement Global Exception Handler (45 mins)

### 2.1 Create Error Response DTO

**Create:** `order-service/src/main/java/com/example/orderservice/dto/ErrorResponse.java`

```java
package com.example.orderservice.dto;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Standard error response format
 * Provides consistent error structure across all endpoints
 */
public class ErrorResponse {
    
    private LocalDateTime timestamp;
    private int status;
    private String error;
    private String message;
    private String path;
    private List<String> details;  // For validation errors
    
    // Constructors
    public ErrorResponse() {
        this.timestamp = LocalDateTime.now();
    }
    
    public ErrorResponse(int status, String error, String message, String path) {
        this();
        this.status = status;
        this.error = error;
        this.message = message;
        this.path = path;
    }
    
    // Getters and Setters
    // ... (generate all getters/setters)
}
```

### 2.2 Create Global Exception Handler

**Create:** `order-service/src/main/java/com/example/orderservice/exception/GlobalExceptionHandler.java`

```java
package com.example.orderservice.exception;

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;

import com.example.orderservice.dto.ErrorResponse;

/**
 * Global exception handler using @ControllerAdvice
 * 
 * DESIGN PATTERN: Aspect-Oriented Programming (AOP)
 * - Separates cross-cutting concerns (error handling) from business logic
 * - Single place to handle all exceptions
 * - Consistent error response format
 * 
 * HOW IT WORKS:
 * 1. Spring intercepts exceptions thrown by controllers
 * 2. Matches exception type to @ExceptionHandler method
 * 3. Returns ResponseEntity with appropriate HTTP status
 */
@RestControllerAdvice
public class GlobalExceptionHandler {
    
    /**
     * Handle OrderNotFoundException
     * HTTP 404 Not Found
     */
    @ExceptionHandler(OrderNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleOrderNotFound(
            OrderNotFoundException ex, 
            WebRequest request) {
        
        ErrorResponse error = new ErrorResponse(
            HttpStatus.NOT_FOUND.value(),
            "Not Found",
            ex.getMessage(),
            request.getDescription(false).replace("uri=", "")
        );
        
        return new ResponseEntity<>(error, HttpStatus.NOT_FOUND);
    }
    
    /**
     * Handle InvalidOrderException
     * HTTP 400 Bad Request
     */
    @ExceptionHandler(InvalidOrderException.class)
    public ResponseEntity<ErrorResponse> handleInvalidOrder(
            InvalidOrderException ex, 
            WebRequest request) {
        
        ErrorResponse error = new ErrorResponse(
            HttpStatus.BAD_REQUEST.value(),
            "Bad Request",
            ex.getMessage(),
            request.getDescription(false).replace("uri=", "")
        );
        
        return new ResponseEntity<>(error, HttpStatus.BAD_REQUEST);
    }
    
    /**
     * Handle Bean Validation errors (@Valid)
     * HTTP 400 Bad Request
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidationErrors(
            MethodArgumentNotValidException ex,
            WebRequest request) {
        
        // Extract validation error messages
        List<String> details = ex.getBindingResult()
            .getFieldErrors()
            .stream()
            .map(error -> error.getField() + ": " + error.getDefaultMessage())
            .collect(Collectors.toList());
        
        ErrorResponse error = new ErrorResponse(
            HttpStatus.BAD_REQUEST.value(),
            "Validation Failed",
            "Input validation failed. Check 'details' field.",
            request.getDescription(false).replace("uri=", "")
        );
        error.setDetails(details);
        
        return new ResponseEntity<>(error, HttpStatus.BAD_REQUEST);
    }
    
    /**
     * Handle all other exceptions
     * HTTP 500 Internal Server Error
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGlobalException(
            Exception ex, 
            WebRequest request) {
        
        ErrorResponse error = new ErrorResponse(
            HttpStatus.INTERNAL_SERVER_ERROR.value(),
            "Internal Server Error",
            "An unexpected error occurred. Please contact support.",
            request.getDescription(false).replace("uri=", "")
        );
        
        // Log the full exception (in real app, use proper logger)
        ex.printStackTrace();
        
        return new ResponseEntity<>(error, HttpStatus.INTERNAL_SERVER_ERROR);
    }
}
```

### 2.3 Test Error Handling

```bash
# Test 1: Order not found - Should return 404
curl -i http://localhost:8081/api/orders/invalid-id

# Expected:
# HTTP/1.1 404
# {
#   "timestamp": "...",
#   "status": 404,
#   "error": "Not Found",
#   "message": "Order not found: invalid-id",
#   "path": "/api/orders/invalid-id"
# }

# Test 2: Validation error - Should return 400
curl -i -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId": "", "items": []}'

# Expected:
# HTTP/1.1 400
# {
#   "timestamp": "...",
#   "status": 400,
#   "error": "Validation Failed",
#   "message": "Input validation failed...",
#   "details": [
#     "customerId: must not be blank",
#     "items: must not be empty"
#   ]
# }
```

**Learning Point:** Notice how `@RestControllerAdvice` intercepts exceptions and returns proper HTTP status codes. This is **Aspect-Oriented Programming (AOP)** - separating cross-cutting concerns from business logic.

---

## Step 3: Setup Kafka with Docker Compose (30 mins)

### 3.1 Verify Docker Compose File

**File:** `/Users/mhiteshkumar/spring-kafka-microservices/docker-compose.yml`

Should already exist from Phase 1. Verify it contains:
- Zookeeper (port 2181)
- Kafka broker (port 9092)
- kafka-ui (port 8080)

### 3.2 Start Kafka

```bash
# Start Kafka in background
docker-compose up -d

# Verify containers are running
docker-compose ps

# Expected:
# NAME                 IMAGE                          STATUS
# kafka                confluentinc/cp-kafka:latest   Up
# zookeeper            confluentinc/cp-zookeeper...   Up
# kafka-ui             provectuslabs/kafka-ui:latest  Up

# Access Kafka UI
open http://localhost:8080
```

**Kafka UI Features:**
- View topics
- Browse messages
- Monitor consumer groups
- Create topics manually

---

## Step 4: Configure Kafka in Spring Boot (30 mins)

### 4.1 Update application.yml

**File:** `order-service/src/main/resources/application.yml`

```yaml
server:
  port: 8081

spring:
  application:
    name: order-service
  
  # Kafka Configuration
  kafka:
    bootstrap-servers: localhost:9092  # Kafka broker address
    
    # Producer Configuration
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      acks: all  # Wait for all replicas to acknowledge
      retries: 3  # Retry failed sends
      
      # JsonSerializer properties
      properties:
        spring.json.type.mapping: orderCreated:com.example.orderservice.event.OrderCreatedEvent

# Logging
logging:
  level:
    com.example.orderservice: DEBUG
    org.springframework.kafka: DEBUG  # See Kafka activity

# Actuator
management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics
```

**Key Points:**
- `bootstrap-servers`: Kafka broker address
- `key-serializer`: Converts key to bytes (String)
- `value-serializer`: Converts value to bytes (JSON)
- `acks: all`: Ensures message is written to all replicas (most reliable)
- `type.mapping`: Maps event class name for deserialization

### 4.2 Create Event DTO

**Create:** `order-service/src/main/java/com/example/orderservice/event/OrderCreatedEvent.java`

```java
package com.example.orderservice.event;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Event published when an order is created
 * 
 * DESIGN PATTERN: Event-Driven Architecture
 * - Decouples order-service from payment-service/stock-service
 * - Services communicate via events, not direct API calls
 * - Enables asynchronous processing
 * 
 * WHY A SEPARATE EVENT CLASS (not reuse Order)?
 * - Domain model (Order) may have fields we don't want to publish
 * - Event schema should be stable (versioned contract)
 * - Domain model can evolve independently
 */
public class OrderCreatedEvent {
    
    private String orderId;
    private String customerId;
    private List<OrderItemEvent> items;
    private BigDecimal totalAmount;
    private LocalDateTime createdAt;
    
    // Default constructor (for Jackson deserialization)
    public OrderCreatedEvent() {
    }
    
    public OrderCreatedEvent(String orderId, String customerId, 
                            List<OrderItemEvent> items, BigDecimal totalAmount,
                            LocalDateTime createdAt) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.items = items;
        this.totalAmount = totalAmount;
        this.createdAt = createdAt;
    }
    
    // Getters and Setters
    // ... (generate all)
    
    // Inner class for order item
    public static class OrderItemEvent {
        private String productId;
        private String productName;
        private int quantity;
        private BigDecimal price;
        
        public OrderItemEvent() {
        }
        
        public OrderItemEvent(String productId, String productName, 
                             int quantity, BigDecimal price) {
            this.productId = productId;
            this.productName = productName;
            this.quantity = quantity;
            this.price = price;
        }
        
        // Getters and Setters
        // ... (generate all)
    }
}
```

---

## Step 5: Implement Kafka Producer (1 hour)

### 5.1 Create Kafka Producer Service

**Create:** `order-service/src/main/java/com/example/orderservice/service/OrderEventProducer.java`

```java
package com.example.orderservice.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.example.orderservice.event.OrderCreatedEvent;

import java.util.concurrent.CompletableFuture;

/**
 * Kafka Producer for Order Events
 * 
 * DESIGN PATTERN: Publisher in Pub/Sub pattern
 * - Publishes events to Kafka topics
 * - Other services (payment, stock) subscribe to topics
 * - Asynchronous, non-blocking communication
 * 
 * WHY SEPARATE PRODUCER SERVICE?
 * - Single Responsibility: OrderService handles business logic,
 *   OrderEventProducer handles event publishing
 * - Easier to test (can mock producer)
 * - Reusable for different event types
 */
@Service
public class OrderEventProducer {
    
    private static final Logger log = LoggerFactory.getLogger(OrderEventProducer.class);
    private static final String TOPIC = "order-events";
    
    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;
    
    public OrderEventProducer(KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
    
    /**
     * Publish OrderCreatedEvent to Kafka
     * 
     * @param event The event to publish
     * @return CompletableFuture for async result
     */
    public CompletableFuture<SendResult<String, OrderCreatedEvent>> publishOrderCreated(
            OrderCreatedEvent event) {
        
        log.info("Publishing OrderCreatedEvent: orderId={}", event.getOrderId());
        
        // Send to Kafka
        // Key: orderId (ensures all events for same order go to same partition)
        // Value: event object (serialized to JSON)
        CompletableFuture<SendResult<String, OrderCreatedEvent>> future = 
            kafkaTemplate.send(TOPIC, event.getOrderId(), event);
        
        // Add callbacks
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Message sent successfully: orderId={}, offset={}", 
                    event.getOrderId(),
                    result.getRecordMetadata().offset());
            } else {
                log.error("Failed to send message: orderId={}", event.getOrderId(), ex);
            }
        });
        
        return future;
    }
}
```

**Key Concepts:**
- `KafkaTemplate`: Spring's abstraction for Kafka producer
- **Key** (orderId): Ensures messages with same key go to same partition (ordering guarantee)
- **Value** (event): Actual message payload
- `CompletableFuture`: Asynchronous, non-blocking
- Callbacks: Handle success/failure

### 5.2 Update OrderService to Publish Events

**File:** `order-service/src/main/java/com/example/orderservice/service/OrderService.java`

```java
@Service
public class OrderService {
    
    private final Map<String, Order> orderStore = new ConcurrentHashMap<>();
    private final OrderEventProducer eventProducer;  // ← Add this
    
    // Update constructor
    public OrderService(OrderEventProducer eventProducer) {
        this.eventProducer = eventProducer;
    }
    
    public Order createOrder(CreateOrderRequest request) {
        // ... existing code (create order, validate, store) ...
        
        // NEW: Publish event AFTER order is stored
        OrderCreatedEvent event = mapToEvent(order);
        eventProducer.publishOrderCreated(event);
        
        return order;
    }
    
    // NEW: Helper method to map Order → OrderCreatedEvent
    private OrderCreatedEvent mapToEvent(Order order) {
        List<OrderCreatedEvent.OrderItemEvent> itemEvents = order.getItems().stream()
            .map(item -> new OrderCreatedEvent.OrderItemEvent(
                item.getProductId(),
                item.getProductName(),
                item.getQuantity(),
                item.getPrice()
            ))
            .collect(Collectors.toList());
        
        return new OrderCreatedEvent(
            order.getOrderId(),
            order.getCustomerId(),
            itemEvents,
            order.getTotalAmount(),
            order.getCreatedAt()
        );
    }
    
    // ... rest of the methods unchanged ...
}
```

---

## Step 6: Test Kafka Integration (30 mins)

### 6.1 Start Application

```bash
# Make sure Kafka is running
docker-compose ps

# Start Spring Boot app
mvn spring-boot:run

# Look for Kafka connection logs
# Expected: "Kafka version: ...", "Cluster ID: ..."
```

### 6.2 Create an Order

```bash
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-123",
    "items": [
      {
        "productId": "PROD-001",
        "productName": "Laptop",
        "quantity": 1,
        "price": 999.99
      }
    ]
  }'
```

### 6.3 Verify Event in Kafka UI

1. Open http://localhost:8080
2. Click on "order-events" topic
3. Click "Messages" tab
4. You should see the OrderCreatedEvent JSON

**Example Message:**
```json
{
  "orderId": "abc-123",
  "customerId": "CUST-123",
  "items": [
    {
      "productId": "PROD-001",
      "productName": "Laptop",
      "quantity": 1,
      "price": 999.99
    }
  ],
  "totalAmount": 999.99,
  "createdAt": "2026-06-29T10:30:00"
}
```

### 6.4 Check Application Logs

```bash
# Should see:
# Publishing OrderCreatedEvent: orderId=abc-123
# Message sent successfully: orderId=abc-123, offset=0
```

---

## Troubleshooting

### Issue 1: "Connection refused to localhost:9092"

**Solution:**
```bash
# Check if Kafka is running
docker-compose ps

# If not running, start it
docker-compose up -d

# Wait 30 seconds for Kafka to initialize
```

### Issue 2: "Failed to send message: Timeout"

**Solution:**
```yaml
# In application.yml, add timeout config
spring:
  kafka:
    producer:
      properties:
        request.timeout.ms: 30000
        delivery.timeout.ms: 120000
```

### Issue 3: Topic "order-events" not created

**Solution:**
Kafka auto-creates topics by default. If disabled:

```bash
# Manually create topic
docker exec -it kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic order-events \
  --partitions 3 \
  --replication-factor 1
```

---

## Review Questions

After completing Phase 2, you should be able to answer:

1. **What is the purpose of `@RestControllerAdvice`?**

2. **Why do we create custom exceptions instead of using `RuntimeException`?**

3. **What's the difference between `@ExceptionHandler` and try-catch?**

4. **What is `KafkaTemplate` and how does it work?**

5. **Why do we use a separate `OrderCreatedEvent` class instead of sending `Order` directly?**

6. **What is the role of the event key (orderId) in Kafka?**

7. **What happens if Kafka is down when we try to publish an event?**

8. **What is the difference between synchronous and asynchronous message publishing?**

9. **How do you verify that a message was successfully published to Kafka?**

10. **What is the benefit of event-driven architecture over direct API calls?**

---

## What's Next? (Phase 3)

After completing Phase 2, you'll have:
- ✅ Robust error handling
- ✅ Kafka producer publishing OrderCreatedEvent
- ✅ Working order-service with event publishing

**Phase 3 Preview: Build Payment Service (Consumer)**
1. Create payment-service project
2. Implement Kafka consumer
3. Listen to `order-events` topic
4. Process OrderCreatedEvent
5. Validate payment
6. Publish PaymentProcessedEvent

**Duration:** 2-3 days

---

## Helpful Commands

```bash
# Start Kafka
docker-compose up -d

# Stop Kafka
docker-compose down

# View Kafka logs
docker-compose logs -f kafka

# Start Spring Boot app
mvn spring-boot:run

# Run with debug logging
mvn spring-boot:run -Dspring-boot.run.arguments=--logging.level.com.example=DEBUG

# Create order (test endpoint)
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d @test-order.json

# View all orders
curl http://localhost:8081/api/orders

# Check app health
curl http://localhost:8081/actuator/health
```

---

**Ready to start Phase 2? Let me know when you want to begin!** 🚀
