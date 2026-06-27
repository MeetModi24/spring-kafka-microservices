# Spring Boot Microservices - Deep Learning Guide

## 📚 Your Next Task: Implement Service and Controller

You now have:
- ✅ Project structure
- ✅ Domain models (Order, OrderItem, OrderStatus)
- ✅ DTOs (Request and Response objects)

**NOW YOU WILL CODE:**
- ⏳ Service layer (OrderService)
- ⏳ Controller layer (OrderController)

---

## 🎯 PART 1: Understanding Dependency Injection

### What is Dependency Injection (DI)?

**Bad approach (tight coupling):**
```java
public class OrderController {
    private OrderService orderService = new OrderService(); // ❌ Creating dependency manually
    
    public OrderResponse createOrder(CreateOrderRequest request) {
        return orderService.createOrder(request);
    }
}
```

**Problems:**
- Controller creates OrderService → tight coupling
- Hard to test (can't mock OrderService)
- Can't swap implementations
- If OrderService constructor changes, Controller breaks

**Good approach (Dependency Injection):**
```java
public class OrderController {
    private final OrderService orderService; // ✅ Dependency injected
    
    public OrderController(OrderService orderService) {
        this.orderService = orderService; // Spring injects this
    }
    
    public OrderResponse createOrder(CreateOrderRequest request) {
        return orderService.createOrder(request);
    }
}
```

**Benefits:**
- Spring creates OrderService and injects it
- Easy to test (pass mock in constructor)
- Loose coupling
- Single Responsibility (Controller doesn't manage OrderService lifecycle)

---

### How Spring DI Works (IoC Container)

```
┌─────────────────────────────────────────────┐
│   Spring IoC Container                      │
│   (ApplicationContext)                      │
│                                             │
│   ┌───────────────┐     ┌────────────────┐│
│   │ OrderService  │     │ OrderController││
│   │    Bean       │◄────┤     Bean       ││
│   └───────────────┘     └────────────────┘│
│         ▲                                   │
│         │ Managed by Spring                 │
│         │ Created once (Singleton)          │
└─────────┴───────────────────────────────────┘
```

**Steps:**
1. Spring scans packages for `@Component`, `@Service`, `@Controller` classes
2. Creates instances (beans) and stores them in ApplicationContext
3. Resolves dependencies and injects them
4. You never use `new` for these classes - Spring manages lifecycle

---

### Spring Stereotypes (Annotations)

| Annotation | Purpose | Where to Use |
|------------|---------|--------------|
| `@Component` | Generic Spring-managed bean | Utility classes, helpers |
| `@Service` | Business logic layer | Service classes |
| `@Repository` | Data access layer | DAO, JPA repositories |
| `@Controller` | Web controller (returns views) | MVC controllers |
| `@RestController` | REST API controller (returns JSON) | REST endpoints |

**They're all `@Component` under the hood!**
- `@Service` = `@Component` (semantic difference)
- `@Repository` = `@Component` + exception translation
- `@RestController` = `@Controller` + `@ResponseBody`

**Why semantic annotations?**
- Code readability (you know what layer a class belongs to)
- Future Spring features might treat them differently
- AOP pointcuts can target specific stereotypes

---

### Three Ways to Inject Dependencies

```java
// 1. CONSTRUCTOR INJECTION (RECOMMENDED)
@Service
public class OrderService {
    private final OrderRepository repo; // final = immutable after construction
    
    public OrderService(OrderRepository repo) {
        this.repo = repo;
    }
}
// ✅ Immutable, testable, clear dependencies
// ✅ Lombok @RequiredArgsConstructor can generate this

// 2. SETTER INJECTION (OPTIONAL DEPENDENCIES)
@Service
public class OrderService {
    private OrderRepository repo;
    
    @Autowired
    public void setRepo(OrderRepository repo) {
        this.repo = repo;
    }
}
// ⚠️ Mutable, less clear, used for optional deps

// 3. FIELD INJECTION (AVOID)
@Service
public class OrderService {
    @Autowired
    private OrderRepository repo;
}
// ❌ Can't test without Spring, hides dependencies, mutable
// ❌ Common but considered bad practice
```

**BEST PRACTICE: Use constructor injection**

---

## 🎯 PART 2: Service Layer Pattern

### What is the Service Layer?

**Purpose:** Contains business logic, orchestrates operations, manages transactions.

**What goes in Service:**
- Business rules validation
- Orchestration (calling multiple repositories/services)
- Transaction boundaries
- Event publishing
- Complex calculations

**What DOESN'T go in Service:**
- HTTP request/response handling → Controller
- Database queries → Repository
- DTO ↔ Domain mapping → Sometimes Service, sometimes separate Mapper

---

### Service Layer Responsibilities

```java
@Service
public class OrderService {
    
    // RESPONSIBILITY 1: Dependency Management
    private final OrderRepository orderRepository;
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;
    
    public OrderService(OrderRepository repo, KafkaTemplate kafka) {
        this.orderRepository = repo;
        this.kafkaTemplate = kafka;
    }
    
    // RESPONSIBILITY 2: Business Logic
    public Order createOrder(CreateOrderRequest request) {
        // 1. Convert DTO → Domain Model
        Order order = mapToOrder(request);
        
        // 2. Validate business rules
        if (!order.isValid()) {
            throw new InvalidOrderException("Order validation failed");
        }
        
        // 3. Apply business logic
        order.setOrderId(generateOrderId());
        order.setStatus(OrderStatus.PENDING);
        
        // 4. Persist (in real app with DB)
        // Order saved = orderRepository.save(order);
        
        // 5. Publish event (later with Kafka)
        // publishOrderCreatedEvent(saved);
        
        return order;
    }
    
    // RESPONSIBILITY 3: Mapping (or use separate Mapper class)
    private Order mapToOrder(CreateOrderRequest request) {
        // Map DTO → Domain
        List<OrderItem> items = request.getItems().stream()
            .map(this::mapToOrderItem)
            .toList();
        return new Order(request.getCustomerId(), items);
    }
    
    // Helper methods...
}
```

---

### Transaction Management (Preview)

```java
@Service
public class OrderService {
    
    @Transactional // ACID transaction boundary
    public Order createOrder(CreateOrderRequest request) {
        // Everything in this method runs in ONE database transaction
        // If exception occurs, everything rolls back
        
        Order order = orderRepository.save(new Order(...));
        inventoryService.reserveStock(order.getItems());
        
        // If reserveStock() throws exception, order.save() is rolled back
        
        return order;
    }
}
```

**When to use @Transactional:**
- Multiple database operations that must succeed/fail together
- Reading data that must be consistent
- NOT needed for read-only single queries

---

## 🎯 PART 3: Controller Layer Pattern

### What is the Controller Layer?

**Purpose:** Handles HTTP requests, validates input, calls service, returns HTTP responses.

**What goes in Controller:**
- HTTP method mapping (`@GetMapping`, `@PostMapping`)
- Request validation (`@Valid`)
- DTO ↔ Domain mapping (request DTO → service, service result → response DTO)
- HTTP status codes
- Error handling

**What DOESN'T go in Controller:**
- Business logic → Service
- Database access → Repository/Service
- Complex calculations → Service

---

### REST Controller Anatomy

```java
@RestController // = @Controller + @ResponseBody (returns JSON, not views)
@RequestMapping("/api/orders") // Base path for all methods
public class OrderController {
    
    // DEPENDENCY INJECTION
    private final OrderService orderService;
    
    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }
    
    // ENDPOINT: POST /api/orders
    @PostMapping // HTTP POST
    @ResponseStatus(HttpStatus.CREATED) // Return 201 Created
    public OrderResponse createOrder(
        @Valid @RequestBody CreateOrderRequest request // Validate + deserialize JSON
    ) {
        // 1. Call service (service handles business logic)
        Order order = orderService.createOrder(request);
        
        // 2. Map domain → response DTO
        return mapToResponse(order);
    }
    
    // ENDPOINT: GET /api/orders/{id}
    @GetMapping("/{id}") // Path variable
    public OrderResponse getOrder(@PathVariable String id) {
        Order order = orderService.getOrderById(id);
        return mapToResponse(order);
    }
    
    // ENDPOINT: GET /api/orders
    @GetMapping
    public List<OrderResponse> getAllOrders() {
        List<Order> orders = orderService.getAllOrders();
        return orders.stream()
            .map(this::mapToResponse)
            .toList();
    }
}
```

---

### Controller Annotations Explained

```java
// CLASS LEVEL
@RestController // Marks class as REST controller (returns JSON)
@RequestMapping("/api/orders") // Base URL for all methods

// METHOD LEVEL - HTTP METHODS
@GetMapping // GET requests (read)
@PostMapping // POST requests (create)
@PutMapping // PUT requests (full update)
@PatchMapping // PATCH requests (partial update)
@DeleteMapping // DELETE requests (delete)

// METHOD LEVEL - PARAMETERS
@RequestBody // Deserialize JSON body → Java object
@PathVariable // Extract value from URL path (/orders/{id})
@RequestParam // Extract query parameter (/orders?status=PENDING)
@Valid // Trigger validation on @RequestBody object

// METHOD LEVEL - RESPONSE
@ResponseStatus(HttpStatus.CREATED) // Set HTTP status code (201)
```

---

### Request Validation with @Valid

```java
@PostMapping
public OrderResponse createOrder(@Valid @RequestBody CreateOrderRequest request) {
    // @Valid triggers validation based on annotations in CreateOrderRequest:
    // - @NotBlank on customerId
    // - @NotEmpty on items
    // - @Valid on items (cascades to OrderItemRequest validation)
    
    // If validation fails, Spring automatically:
    // 1. Returns HTTP 400 Bad Request
    // 2. Sends JSON with validation errors
    // 3. Method is NOT called (fails fast)
}
```

**Validation error response example:**
```json
{
  "timestamp": "2026-06-27T10:30:00",
  "status": 400,
  "error": "Bad Request",
  "message": "Validation failed",
  "errors": [
    {
      "field": "customerId",
      "rejectedValue": "",
      "message": "Customer ID is required"
    },
    {
      "field": "items",
      "rejectedValue": [],
      "message": "Order must contain at least one item"
    }
  ]
}
```

---

### HTTP Status Codes Best Practices

| Code | Meaning | When to Use |
|------|---------|-------------|
| 200 | OK | Successful GET, PUT, PATCH, DELETE |
| 201 | Created | Successful POST that creates resource |
| 204 | No Content | Successful DELETE (no response body) |
| 400 | Bad Request | Validation failed, malformed request |
| 404 | Not Found | Resource doesn't exist |
| 409 | Conflict | Business rule violation (e.g., duplicate order) |
| 500 | Internal Server Error | Unexpected error |

```java
@PostMapping
@ResponseStatus(HttpStatus.CREATED) // 201
public OrderResponse createOrder(@Valid @RequestBody CreateOrderRequest request) {
    return orderService.createOrder(request);
}

@DeleteMapping("/{id}")
@ResponseStatus(HttpStatus.NO_CONTENT) // 204
public void deleteOrder(@PathVariable String id) {
    orderService.deleteOrder(id);
}
```

---

## 🎯 YOUR IMPLEMENTATION TASK

### TASK 1: Implement OrderService

**File:** `order-service/src/main/java/com/example/orderservice/service/OrderService.java`

**Requirements:**
1. Annotate with `@Service`
2. Use constructor injection for dependencies (none yet, we'll add later)
3. Implement `createOrder(CreateOrderRequest request)` method:
   - Generate order ID (use `UUID.randomUUID().toString()`)
   - Map request DTO → Order domain model
   - Validate order (`order.isValid()`)
   - Store in-memory (use a `ConcurrentHashMap<String, Order>` as field)
   - Return the created Order
4. Implement `getOrderById(String id)` method:
   - Retrieve from in-memory map
   - Throw exception if not found
5. Implement `getAllOrders()` method:
   - Return all orders from map

**Hints:**
- Use `private final Map<String, Order> orderStore = new ConcurrentHashMap<>()`
- Map DTO items to domain items using streams
- Don't forget to call `order.setOrderId()` and `order.setCreatedAt()`

---

### TASK 2: Implement OrderController

**File:** `order-service/src/main/java/com/example/orderservice/controller/OrderController.java`

**Requirements:**
1. Annotate with `@RestController`
2. Add `@RequestMapping("/api/orders")`
3. Inject `OrderService` via constructor
4. Implement POST endpoint:
   - Method: `createOrder(@Valid @RequestBody CreateOrderRequest request)`
   - Annotation: `@PostMapping` and `@ResponseStatus(HttpStatus.CREATED)`
   - Call `orderService.createOrder()`
   - Map Order → OrderResponse
   - Return OrderResponse
5. Implement GET by ID endpoint:
   - Method: `getOrder(@PathVariable String id)`
   - Annotation: `@GetMapping("/{id}")`
   - Call `orderService.getOrderById()`
   - Map Order → OrderResponse
6. Implement GET all endpoint:
   - Method: `getAllOrders()`
   - Annotation: `@GetMapping`
   - Return list of OrderResponse

**Hints:**
- Create a private method `mapToResponse(Order order)` to avoid duplication
- Use streams for mapping lists
- Set response DTO fields manually (we'll use MapStruct later)

---

### TASK 3: Test Your Service

After implementation, run:
```bash
cd order-service
mvn clean compile
mvn spring-boot:run
```

**If successful, you'll see:**
```
Started OrderServiceApplication in 2.5 seconds
Tomcat started on port(s): 8081 (http)
```

**Test with curl:**
```bash
# Create an order
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

# Get all orders
curl http://localhost:8081/api/orders
```

---

## 🎓 Review Questions (Answer After Implementation)

1. **What is the difference between `@Service` and `@Component`?**

2. **Why do we use constructor injection instead of field injection?**

3. **What happens if you remove `@Valid` from the controller method parameter?**

4. **Why do we have separate Request and Response DTOs instead of one OrderDTO?**

5. **What is the purpose of `@ResponseStatus(HttpStatus.CREATED)`?**

6. **Where should you calculate `totalAmount` - in DTO, Controller, Service, or Domain Model?**

7. **What happens if you call `orderService.getOrderById("invalid-id")`? How should you handle it?**

---

## 📚 Next Steps (After You Finish)

1. I'll review your code
2. We'll add proper error handling (`@ControllerAdvice`, custom exceptions)
3. We'll add Kafka producer to publish events
4. Build second service (payment-service) that consumes events
5. Learn Kafka patterns and event-driven architecture

---

**START CODING NOW!** Create OrderService.java and OrderController.java. Ask me questions if you get stuck!
