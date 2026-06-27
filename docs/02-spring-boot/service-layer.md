# Service Layer Pattern

> Business logic layer in Spring Boot applications

---

## What is the Service Layer?

The Service Layer sits between the Controller (presentation) and Repository (data access) layers.

**Purpose:** Contains business logic, orchestrates operations, manages transactions.

```
┌──────────────────┐
│   Controller     │  ← HTTP handling, validation
└────────┬─────────┘
         │
┌────────▼─────────┐
│    Service       │  ← Business logic, orchestration
└────────┬─────────┘
         │
┌────────▼─────────┐
│   Repository     │  ← Data access
└──────────────────┘
```

---

## What Goes in the Service Layer?

### ✅ Service Layer Responsibilities

1. **Business Logic**
   - Business rules validation
   - Complex calculations
   - Domain model operations

2. **Orchestration**
   - Coordinating multiple repositories
   - Calling other services
   - Aggregating data from multiple sources

3. **Transaction Management**
   - Defining transaction boundaries
   - Ensuring ACID properties

4. **Event Publishing**
   - Publishing domain events (Kafka, Spring Events)
   - Notifying other services

5. **DTO ↔ Domain Mapping**
   - Converting request DTOs to domain models
   - Converting domain models to response DTOs

### ❌ What DOESN'T Go in Service

1. **HTTP Handling** → Controller
   - Request/response processing
   - HTTP status codes
   - URL mapping

2. **Database Queries** → Repository
   - SQL/JPQL queries
   - Database-specific logic
   - Connection management

3. **Presentation Logic** → Controller/View
   - Formatting for display
   - UI-specific concerns

---

## Service Layer Anatomy

### Basic Service Structure

```java
@Service
public class OrderService {
    
    // ============================================
    // DEPENDENCY MANAGEMENT
    // ============================================
    private final OrderRepository orderRepository;
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;
    private final CustomerService customerService;
    
    // Constructor injection (recommended)
    public OrderService(
        OrderRepository orderRepository,
        KafkaTemplate<String, OrderEvent> kafkaTemplate,
        CustomerService customerService
    ) {
        this.orderRepository = orderRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.customerService = customerService;
    }
    
    // ============================================
    // BUSINESS OPERATIONS
    // ============================================
    
    public Order createOrder(CreateOrderRequest request) {
        // 1. Convert DTO → Domain Model
        Order order = mapToOrder(request);
        
        // 2. Validate business rules
        if (!order.isValid()) {
            throw new InvalidOrderException("Order validation failed");
        }
        
        // 3. Check business constraints
        Customer customer = customerService.getById(request.getCustomerId());
        if (!customer.canPlaceOrder()) {
            throw new BusinessRuleException("Customer cannot place orders");
        }
        
        // 4. Apply business logic
        order.setOrderId(generateOrderId());
        order.setStatus(OrderStatus.PENDING);
        
        // 5. Persist
        Order savedOrder = orderRepository.save(order);
        
        // 6. Publish event
        publishOrderCreatedEvent(savedOrder);
        
        return savedOrder;
    }
    
    // ============================================
    // HELPER METHODS (Private)
    // ============================================
    
    private Order mapToOrder(CreateOrderRequest request) {
        List<OrderItem> items = request.getItems().stream()
            .map(this::mapToOrderItem)
            .toList();
        return new Order(request.getCustomerId(), items);
    }
    
    private OrderItem mapToOrderItem(OrderItemRequest dto) {
        return new OrderItem(
            dto.getProductId(),
            dto.getProductName(),
            dto.getQuantity(),
            dto.getPrice()
        );
    }
    
    private String generateOrderId() {
        return UUID.randomUUID().toString();
    }
    
    private void publishOrderCreatedEvent(Order order) {
        OrderEvent event = new OrderEvent(order.getOrderId(), order.getCustomerId());
        kafkaTemplate.send("order-events", event);
    }
}
```

---

## Service Layer Patterns

### Pattern 1: Simple CRUD Service

```java
@Service
public class OrderService {
    private final OrderRepository orderRepository;
    
    public OrderService(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }
    
    public Order create(CreateOrderRequest request) {
        Order order = mapToOrder(request);
        return orderRepository.save(order);
    }
    
    public Order getById(String id) {
        return orderRepository.findById(id)
            .orElseThrow(() -> new OrderNotFoundException(id));
    }
    
    public List<Order> getAll() {
        return orderRepository.findAll();
    }
    
    public Order update(String id, UpdateOrderRequest request) {
        Order order = getById(id);
        order.updateFrom(request);
        return orderRepository.save(order);
    }
    
    public void delete(String id) {
        orderRepository.deleteById(id);
    }
}
```

### Pattern 2: Service with Business Logic

```java
@Service
public class OrderService {
    private final OrderRepository orderRepository;
    private final CustomerService customerService;
    private final InventoryService inventoryService;
    
    public Order placeOrder(CreateOrderRequest request) {
        // Business Rule 1: Check customer eligibility
        Customer customer = customerService.getById(request.getCustomerId());
        if (customer.hasOutstandingPayments()) {
            throw new BusinessRuleException("Customer has outstanding payments");
        }
        
        // Business Rule 2: Check inventory
        for (OrderItemRequest item : request.getItems()) {
            if (!inventoryService.isAvailable(item.getProductId(), item.getQuantity())) {
                throw new InsufficientStockException(item.getProductId());
            }
        }
        
        // Business Rule 3: Apply pricing rules
        Order order = mapToOrder(request);
        order.applyDiscounts(customer.getDiscountTier());
        
        // Orchestration: Reserve inventory
        inventoryService.reserve(order.getItems());
        
        // Persist
        return orderRepository.save(order);
    }
}
```

### Pattern 3: Service with Event Publishing

```java
@Service
public class OrderService {
    private final OrderRepository orderRepository;
    private final ApplicationEventPublisher eventPublisher; // Spring Events
    
    public Order createOrder(CreateOrderRequest request) {
        Order order = mapToOrder(request);
        Order savedOrder = orderRepository.save(order);
        
        // Publish domain event
        eventPublisher.publishEvent(new OrderCreatedEvent(savedOrder));
        
        return savedOrder;
    }
}

// Event listener in another service
@Service
public class NotificationService {
    
    @EventListener
    public void onOrderCreated(OrderCreatedEvent event) {
        // Send notification
        sendConfirmationEmail(event.getOrder());
    }
}
```

---

## Transaction Management

### What is @Transactional?

`@Transactional` defines a transaction boundary - all database operations within the method execute as a single unit of work.

**ACID Properties:**
- **Atomicity**: All or nothing (rollback on failure)
- **Consistency**: Database remains in valid state
- **Isolation**: Concurrent transactions don't interfere
- **Durability**: Committed changes are permanent

### Basic Transaction

```java
@Service
public class OrderService {
    
    @Transactional
    public Order createOrder(CreateOrderRequest request) {
        // Everything in this method runs in ONE database transaction
        
        Order order = orderRepository.save(new Order(...));
        inventoryService.reserveStock(order.getItems());
        
        // If reserveStock() throws exception, order.save() is rolled back
        
        return order;
    }
}
```

### Transaction Propagation

```java
@Service
public class OrderService {
    
    // REQUIRED (default): Join existing transaction or create new
    @Transactional(propagation = Propagation.REQUIRED)
    public void methodA() {
        // ...
    }
    
    // REQUIRES_NEW: Always create new transaction (suspend existing)
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void methodB() {
        // Runs in separate transaction
        // Won't roll back if methodA fails
    }
    
    // SUPPORTS: Join transaction if exists, run without if not
    @Transactional(propagation = Propagation.SUPPORTS)
    public void methodC() {
        // Read-only operation
    }
}
```

### Transaction Isolation Levels

```java
@Service
public class OrderService {
    
    // READ_COMMITTED: Prevent dirty reads
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public Order getOrder(String id) {
        return orderRepository.findById(id).orElseThrow();
    }
    
    // SERIALIZABLE: Full isolation (slowest, safest)
    @Transactional(isolation = Isolation.SERIALIZABLE)
    public void transferFunds(String from, String to, BigDecimal amount) {
        // Critical financial operation
    }
}
```

### Read-Only Transactions

```java
@Service
public class OrderService {
    
    @Transactional(readOnly = true)
    public List<Order> getOrdersByCustomer(String customerId) {
        // Optimization: Database can skip locking
        return orderRepository.findByCustomerId(customerId);
    }
}
```

### Rollback Rules

```java
@Service
public class OrderService {
    
    // Default: Rollback on RuntimeException and Error
    @Transactional
    public void methodA() {
        throw new RuntimeException(); // ← Rolls back
    }
    
    // Rollback on checked exceptions too
    @Transactional(rollbackFor = Exception.class)
    public void methodB() throws IOException {
        throw new IOException(); // ← Rolls back
    }
    
    // Don't rollback on specific exception
    @Transactional(noRollbackFor = ValidationException.class)
    public void methodC() {
        throw new ValidationException(); // ← Doesn't roll back
    }
}
```

---

## When to Use @Transactional

### ✅ Use @Transactional When

1. **Multiple Database Operations**
   ```java
   @Transactional
   public void transferMoney(String from, String to, BigDecimal amount) {
       accountRepo.debit(from, amount);  // Must succeed together
       accountRepo.credit(to, amount);   // or fail together
   }
   ```

2. **Data Consistency Required**
   ```java
   @Transactional
   public Order completeOrder(String orderId) {
       Order order = orderRepo.findById(orderId).orElseThrow();
       order.setStatus(COMPLETED);
       inventoryService.confirmReservation(order.getItems());
       return orderRepo.save(order);
   }
   ```

3. **Lazy Loading (JPA)**
   ```java
   @Transactional
   public OrderWithItems getOrderDetails(String id) {
       Order order = orderRepo.findById(id).orElseThrow();
       order.getItems().size(); // Lazy load within transaction
       return order;
   }
   ```

### ❌ Don't Use @Transactional When

1. **Single Read-Only Query**
   ```java
   // No need for @Transactional
   public Order getById(String id) {
       return orderRepo.findById(id).orElseThrow();
   }
   ```

2. **No Database Operations**
   ```java
   // No database involved
   public OrderResponse mapToResponse(Order order) {
       return new OrderResponse(...);
   }
   ```

3. **Long-Running Operations**
   ```java
   // ❌ Bad: Transaction held during external API call
   @Transactional
   public void processOrder(String id) {
       Order order = orderRepo.findById(id).orElseThrow();
       externalService.callSlowAPI(order); // ← Holds transaction!
       orderRepo.save(order);
   }
   
   // ✅ Good: Minimize transaction scope
   public void processOrder(String id) {
       Order order = getOrder(id); // Quick read
       externalService.callSlowAPI(order); // No transaction
       saveOrder(order); // Quick write in new transaction
   }
   
   @Transactional
   private Order getOrder(String id) {
       return orderRepo.findById(id).orElseThrow();
   }
   
   @Transactional
   private void saveOrder(Order order) {
       orderRepo.save(order);
   }
   ```

---

## Service Layer Best Practices

### 1. Keep Services Focused

```java
// ❌ Bad: God service (does everything)
@Service
public class OrderService {
    public Order createOrder() { }
    public void sendEmail() { }
    public void generateInvoice() { }
    public void updateInventory() { }
}

// ✅ Good: Focused services
@Service
public class OrderService {
    public Order createOrder() { }
}

@Service
public class NotificationService {
    public void sendEmail() { }
}

@Service
public class InvoiceService {
    public void generateInvoice() { }
}
```

### 2. Use Interfaces for Flexibility

```java
// Interface
public interface OrderService {
    Order createOrder(CreateOrderRequest request);
    Order getById(String id);
}

// Implementation
@Service
public class OrderServiceImpl implements OrderService {
    @Override
    public Order createOrder(CreateOrderRequest request) {
        // ...
    }
    
    @Override
    public Order getById(String id) {
        // ...
    }
}
```

### 3. Don't Return Entities Directly

```java
// ❌ Bad: Exposing JPA entity
@Service
public class OrderService {
    public Order getOrder(String id) {
        return orderRepository.findById(id).orElseThrow();
    }
}

// ✅ Good: Return DTO
@Service
public class OrderService {
    public OrderDTO getOrder(String id) {
        Order order = orderRepository.findById(id).orElseThrow();
        return mapToDTO(order);
    }
}
```

### 4. Handle Exceptions Properly

```java
@Service
public class OrderService {
    
    public Order getById(String id) {
        return orderRepository.findById(id)
            .orElseThrow(() -> new OrderNotFoundException("Order not found: " + id));
    }
    
    public Order createOrder(CreateOrderRequest request) {
        try {
            return orderRepository.save(mapToOrder(request));
        } catch (DataIntegrityViolationException e) {
            throw new DuplicateOrderException("Order already exists", e);
        }
    }
}
```

### 5. Use Separate Mapper Classes

```java
// Mapper component
@Component
public class OrderMapper {
    public Order toEntity(CreateOrderRequest request) {
        // Mapping logic
    }
    
    public OrderResponse toResponse(Order order) {
        // Mapping logic
    }
}

// Service uses mapper
@Service
public class OrderService {
    private final OrderRepository orderRepository;
    private final OrderMapper orderMapper;
    
    public OrderResponse createOrder(CreateOrderRequest request) {
        Order order = orderMapper.toEntity(request);
        Order saved = orderRepository.save(order);
        return orderMapper.toResponse(saved);
    }
}
```

---

## Common Pitfalls

### 1. Business Logic in Controller

```java
// ❌ Bad: Logic in controller
@RestController
public class OrderController {
    @PostMapping("/orders")
    public OrderResponse create(@RequestBody CreateOrderRequest request) {
        if (request.getItems().isEmpty()) {
            throw new ValidationException();
        }
        Order order = new Order();
        order.setOrderId(UUID.randomUUID().toString());
        // ... more logic
    }
}

// ✅ Good: Logic in service
@RestController
public class OrderController {
    private final OrderService orderService;
    
    @PostMapping("/orders")
    public OrderResponse create(@RequestBody CreateOrderRequest request) {
        return orderService.createOrder(request);
    }
}
```

### 2. Calling Repositories from Controllers

```java
// ❌ Bad: Controller directly uses repository
@RestController
public class OrderController {
    private final OrderRepository orderRepository;
    
    @GetMapping("/orders/{id}")
    public Order get(@PathVariable String id) {
        return orderRepository.findById(id).orElseThrow();
    }
}

// ✅ Good: Go through service
@RestController
public class OrderController {
    private final OrderService orderService;
    
    @GetMapping("/orders/{id}")
    public OrderResponse get(@PathVariable String id) {
        return orderService.getById(id);
    }
}
```

### 3. Transaction on Controller Method

```java
// ❌ Bad: Transaction on controller
@RestController
public class OrderController {
    @PostMapping("/orders")
    @Transactional // ← Wrong layer!
    public OrderResponse create(@RequestBody CreateOrderRequest request) {
        return orderService.createOrder(request);
    }
}

// ✅ Good: Transaction on service
@Service
public class OrderService {
    @Transactional
    public Order createOrder(CreateOrderRequest request) {
        // Transaction here
    }
}
```

---

## Related Topics

- [Dependency Injection](./dependency-injection.md)
- [Controller Layer](./controller-layer.md)
- [Repository Layer](./repository-layer.md)
- [Transaction Management](./transactions.md)
- [Testing Services](./testing.md)
