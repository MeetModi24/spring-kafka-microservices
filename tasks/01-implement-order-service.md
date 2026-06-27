# Task 1: Implement OrderService and OrderController

> **Status:** Waiting for approach decision  
> **Prerequisites:** Project structure created, DTOs and models defined  
> **Documentation:** See [docs/02-spring-boot/](../docs/02-spring-boot/)

---

## 📚 Before You Start - Read These

### Required Reading (in order):

1. [Dependency Injection](../docs/02-spring-boot/dependency-injection.md)
   - Understand how Spring DI works
   - Why constructor injection is best
   - How beans are created and managed

2. [Service Layer Pattern](../docs/02-spring-boot/service-layer.md)
   - What goes in the service layer
   - Transaction management basics
   - Best practices

3. [Controller Layer Pattern](../docs/02-spring-boot/controller-layer.md)
   - REST controller anatomy
   - HTTP methods and status codes
   - Request validation

**Estimated reading time:** 45-60 minutes

---

## 🎯 Part 1: Implement OrderService

### Location
`order-service/src/main/java/com/example/orderservice/service/OrderService.java`

### Requirements

1. **Class Setup**
   ```java
   @Service
   public class OrderService {
       // Your code here
   }
   ```

2. **In-Memory Storage**
   ```java
   private final Map<String, Order> orderStore = new ConcurrentHashMap<>();
   ```
   
   **Why ConcurrentHashMap?**
   - Thread-safe for concurrent access
   - Multiple HTTP requests can create orders simultaneously
   - Regular HashMap would cause race conditions

3. **Implement `createOrder()` Method**
   
   **Method signature:**
   ```java
   public Order createOrder(CreateOrderRequest request)
   ```
   
   **Steps to implement:**
   
   a. **Map DTO → Domain Model**
   ```java
   // Convert CreateOrderRequest → Order
   // Convert List<OrderItemRequest> → List<OrderItem>
   // Use streams: request.getItems().stream().map(...).toList()
   ```
   
   b. **Generate Order ID**
   ```java
   // Use: UUID.randomUUID().toString()
   order.setOrderId(generatedId);
   ```
   
   c. **Set System Fields**
   ```java
   order.setCreatedAt(LocalDateTime.now());
   order.setStatus(OrderStatus.PENDING);
   ```
   
   d. **Validate Order**
   ```java
   if (!order.isValid()) {
       throw new IllegalArgumentException("Order validation failed");
   }
   ```
   
   e. **Store in Map**
   ```java
   orderStore.put(order.getOrderId(), order);
   ```
   
   f. **Return Order**
   ```java
   return order;
   ```

4. **Implement `getOrderById()` Method**
   
   **Method signature:**
   ```java
   public Order getOrderById(String id)
   ```
   
   **Implementation:**
   ```java
   // Retrieve from orderStore
   // If not found, throw exception (for now, use IllegalArgumentException)
   // Return order
   ```

5. **Implement `getAllOrders()` Method**
   
   **Method signature:**
   ```java
   public List<Order> getAllOrders()
   ```
   
   **Implementation:**
   ```java
   // Return all values from orderStore as a List
   // Use: new ArrayList<>(orderStore.values())
   ```

### Helper Methods (Private)

Create these private helper methods:

```java
private OrderItem mapToOrderItem(OrderItemRequest dto) {
    return new OrderItem(
        dto.getProductId(),
        dto.getProductName(),
        dto.getQuantity(),
        dto.getPrice()
    );
}
```

### Testing Your Service

After writing OrderService, you can test it standalone:

```java
// In main() or a test
OrderService service = new OrderService();

CreateOrderRequest request = new CreateOrderRequest();
request.setCustomerId("CUST-123");
// ... set items

Order order = service.createOrder(request);
System.out.println("Created order: " + order.getOrderId());
```

---

## 🎯 Part 2: Implement OrderController

### Location
`order-service/src/main/java/com/example/orderservice/controller/OrderController.java`

### Requirements

1. **Class Setup**
   ```java
   @RestController
   @RequestMapping("/api/orders")
   public class OrderController {
       // Your code here
   }
   ```

2. **Dependency Injection**
   ```java
   private final OrderService orderService;
   
   public OrderController(OrderService orderService) {
       this.orderService = orderService;
   }
   ```

3. **Implement POST Endpoint**
   
   **Method signature:**
   ```java
   @PostMapping
   @ResponseStatus(HttpStatus.CREATED)
   public OrderResponse createOrder(@Valid @RequestBody CreateOrderRequest request)
   ```
   
   **Steps:**
   - Call `orderService.createOrder(request)`
   - Get back an `Order` object
   - Map `Order` → `OrderResponse`
   - Return `OrderResponse`

4. **Implement GET by ID Endpoint**
   
   **Method signature:**
   ```java
   @GetMapping("/{id}")
   public OrderResponse getOrder(@PathVariable String id)
   ```
   
   **Steps:**
   - Call `orderService.getOrderById(id)`
   - Map `Order` → `OrderResponse`
   - Return `OrderResponse`

5. **Implement GET All Endpoint**
   
   **Method signature:**
   ```java
   @GetMapping
   public List<OrderResponse> getAllOrders()
   ```
   
   **Steps:**
   - Call `orderService.getAllOrders()`
   - Map `List<Order>` → `List<OrderResponse>`
   - Use streams: `.map(this::mapToResponse).toList()`
   - Return list

### Helper Method (Private)

Create this private mapping method:

```java
private OrderResponse mapToResponse(Order order) {
    OrderResponse response = new OrderResponse();
    response.setOrderId(order.getOrderId());
    response.setCustomerId(order.getCustomerId());
    response.setTotalAmount(order.getTotalAmount());
    response.setStatus(order.getStatus().name()); // Enum → String
    response.setCreatedAt(order.getCreatedAt());
    response.setItems(mapItemsToResponse(order.getItems()));
    return response;
}

private List<OrderItemResponse> mapItemsToResponse(List<OrderItem> items) {
    return items.stream()
        .map(item -> new OrderItemResponse(
            item.getProductId(),
            item.getProductName(),
            item.getQuantity(),
            item.getPrice()
        ))
        .toList();
}
```

---

## 🧪 Testing Your Implementation

### Step 1: Compile

```bash
cd order-service
mvn clean compile
```

**Expected:** No compilation errors

### Step 2: Run Application

```bash
mvn spring-boot:run
```

**Expected output:**
```
Started OrderServiceApplication in 2.5 seconds
Tomcat started on port(s): 8081 (http)
```

### Step 3: Test with curl

**Create an order:**
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
      },
      {
        "productId": "PROD-002",
        "productName": "Mouse",
        "quantity": 2,
        "price": 29.99
      }
    ]
  }'
```

**Expected response:**
```json
{
  "orderId": "some-uuid",
  "customerId": "CUST-123",
  "items": [...],
  "totalAmount": 1059.97,
  "status": "PENDING",
  "createdAt": "2026-06-27T10:30:00"
}
```

**Get all orders:**
```bash
curl http://localhost:8081/api/orders
```

**Get order by ID:**
```bash
curl http://localhost:8081/api/orders/{orderId}
```

### Step 4: Test Validation

**Test with missing customerId:**
```bash
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "",
    "items": []
  }'
```

**Expected:** HTTP 400 with validation errors

---

## 🎓 Review Questions

After implementing, answer these to check your understanding:

1. **What is the difference between `@Service` and `@Component`?**

2. **Why do we use constructor injection instead of field injection?**

3. **What happens if you remove `@Valid` from the controller method parameter?**

4. **Why do we have separate Request and Response DTOs instead of one OrderDTO?**

5. **What is the purpose of `@ResponseStatus(HttpStatus.CREATED)`?**

6. **Where should you calculate `totalAmount` - in DTO, Controller, Service, or Domain Model?**

7. **What happens if you call `orderService.getOrderById("invalid-id")`? How should you handle it?**

---

## ✅ Definition of Done

- [ ] OrderService class created with `@Service` annotation
- [ ] All three methods implemented (create, getById, getAll)
- [ ] OrderController class created with `@RestController` annotation
- [ ] All three endpoints implemented (POST, GET by ID, GET all)
- [ ] Application compiles without errors
- [ ] Application starts successfully
- [ ] Can create an order via curl
- [ ] Can retrieve orders via curl
- [ ] Validation works (400 error for invalid input)
- [ ] You can explain answers to review questions

---

## 🆘 Getting Stuck?

### Common Issues

**1. "Cannot find symbol: OrderService"**
- Make sure OrderService is in `com.example.orderservice.service` package
- Check `@Service` annotation is present
- Try `mvn clean compile` again

**2. "Port 8081 already in use"**
- Check if another service is running
- Kill existing: `lsof -ti:8081 | xargs kill -9`
- Or change port in `application.yml`

**3. "Bean of type OrderService could not be found"**
- Ensure OrderService has `@Service` annotation
- Check package structure (must be under main package)
- Try `mvn clean compile` again

**4. "Validation not working"**
- Check `@Valid` on `@RequestBody` parameter
- Check validation annotations in DTO classes
- Ensure `spring-boot-starter-validation` dependency exists

### Ask for Help

If stuck, share:
1. The specific error message
2. Your code (OrderService or OrderController)
3. What you've tried

---

## 📚 Next Steps

After completing this task:

1. **Code Review**
   - I'll review your implementation
   - Provide feedback and suggestions
   - Discuss design decisions

2. **Add Error Handling**
   - Create custom exceptions
   - Implement `@ControllerAdvice`
   - Return proper error responses

3. **Add Kafka Producer**
   - Publish `OrderCreatedEvent`
   - Learn Kafka basics
   - Test event publishing

4. **Build Second Service**
   - Create payment-service
   - Consume Kafka events
   - Event-driven communication

---

**Ready to code? Start with OrderService.java! 🚀**
