# Controller Layer Pattern

> REST API layer in Spring Boot applications

---

## What is the Controller Layer?

The Controller Layer is the entry point for HTTP requests in a web application.

**Purpose:** Handles HTTP requests, validates input, calls service layer, returns HTTP responses.

```
HTTP Request → Controller → Service → Repository → Database
HTTP Response ← Controller ← Service ← Repository ←
```

---

## What Goes in the Controller Layer?

### ✅ Controller Responsibilities

1. **HTTP Mapping**
   - Map URLs to methods (`@GetMapping`, `@PostMapping`)
   - Extract path variables and query parameters

2. **Request Validation**
   - Validate input using `@Valid`
   - Check request format

3. **Call Service Layer**
   - Delegate business logic to service
   - Never contain business logic itself

4. **DTO Mapping**
   - Convert request DTO → domain model (or pass to service)
   - Convert domain model → response DTO

5. **HTTP Status Codes**
   - Return appropriate status codes (200, 201, 400, 404, etc.)

6. **Error Handling**
   - Handle exceptions
   - Return error responses

### ❌ What DOESN'T Go in Controller

1. **Business Logic** → Service
   - Business rules
   - Complex calculations
   - Orchestration

2. **Database Access** → Repository/Service
   - SQL queries
   - Database connections

3. **Transaction Management** → Service
   - `@Transactional` belongs in service, not controller

---

## REST Controller Anatomy

### Basic Structure

```java
@RestController // = @Controller + @ResponseBody (returns JSON)
@RequestMapping("/api/orders") // Base path for all methods
public class OrderController {
    
    // ==========================================
    // DEPENDENCY INJECTION
    // ==========================================
    private final OrderService orderService;
    
    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }
    
    // ==========================================
    // ENDPOINTS
    // ==========================================
    
    // CREATE: POST /api/orders
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED) // 201
    public OrderResponse createOrder(
        @Valid @RequestBody CreateOrderRequest request
    ) {
        Order order = orderService.createOrder(request);
        return mapToResponse(order);
    }
    
    // READ: GET /api/orders/{id}
    @GetMapping("/{id}")
    public OrderResponse getOrder(@PathVariable String id) {
        Order order = orderService.getById(id);
        return mapToResponse(order);
    }
    
    // READ ALL: GET /api/orders
    @GetMapping
    public List<OrderResponse> getAllOrders() {
        List<Order> orders = orderService.getAllOrders();
        return orders.stream()
            .map(this::mapToResponse)
            .toList();
    }
    
    // UPDATE: PUT /api/orders/{id}
    @PutMapping("/{id}")
    public OrderResponse updateOrder(
        @PathVariable String id,
        @Valid @RequestBody UpdateOrderRequest request
    ) {
        Order order = orderService.update(id, request);
        return mapToResponse(order);
    }
    
    // DELETE: DELETE /api/orders/{id}
    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT) // 204
    public void deleteOrder(@PathVariable String id) {
        orderService.delete(id);
    }
    
    // ==========================================
    // HELPER METHODS (Private)
    // ==========================================
    
    private OrderResponse mapToResponse(Order order) {
        OrderResponse response = new OrderResponse();
        response.setOrderId(order.getOrderId());
        response.setCustomerId(order.getCustomerId());
        response.setTotalAmount(order.getTotalAmount());
        response.setStatus(order.getStatus().name());
        response.setCreatedAt(order.getCreatedAt());
        response.setItems(mapItems(order.getItems()));
        return response;
    }
    
    private List<OrderItemResponse> mapItems(List<OrderItem> items) {
        return items.stream()
            .map(this::mapItemToResponse)
            .toList();
    }
}
```

---

## Controller Annotations

### Class-Level Annotations

```java
@RestController
// Combines @Controller + @ResponseBody
// All methods return JSON by default (not views)

@RequestMapping("/api/orders")
// Base URL for all methods in this controller
// Methods inherit and extend this path
```

### Method-Level HTTP Mapping

```java
@GetMapping       // GET requests (read)
@PostMapping      // POST requests (create)
@PutMapping       // PUT requests (full update)
@PatchMapping     // PATCH requests (partial update)
@DeleteMapping    // DELETE requests (delete)

// With path extension
@GetMapping("/{id}")           // GET /api/orders/123
@PostMapping("/search")        // POST /api/orders/search
@GetMapping("/customer/{id}")  // GET /api/orders/customer/123
```

### Parameter Annotations

```java
@RequestBody
// Deserialize JSON body → Java object
// Example: { "name": "John" } → CreateOrderRequest object

@PathVariable
// Extract value from URL path
// Example: /orders/123 → String id = "123"

@RequestParam
// Extract query parameter
// Example: /orders?status=PENDING → String status = "PENDING"

@RequestHeader
// Extract HTTP header value
// Example: Authorization header

@Valid
// Trigger validation on @RequestBody object
// Uses Bean Validation annotations (@NotNull, @Size, etc.)
```

### Response Annotations

```java
@ResponseStatus(HttpStatus.CREATED)
// Set HTTP status code
// Example: 201 Created for POST

@ResponseBody
// Serialize return value to JSON
// Implicit with @RestController
```

---

## Request Mapping Examples

### Path Variables

```java
// Single path variable
@GetMapping("/orders/{id}")
public OrderResponse getOrder(@PathVariable String id) {
    return orderService.getById(id);
}

// Multiple path variables
@GetMapping("/customers/{customerId}/orders/{orderId}")
public OrderResponse getCustomerOrder(
    @PathVariable String customerId,
    @PathVariable String orderId
) {
    return orderService.getByCustomerAndOrder(customerId, orderId);
}

// Different variable name
@GetMapping("/{id}")
public OrderResponse get(@PathVariable("id") String orderId) {
    return orderService.getById(orderId);
}
```

### Query Parameters

```java
// Single query param
@GetMapping("/search")
public List<OrderResponse> search(@RequestParam String status) {
    // /api/orders/search?status=PENDING
    return orderService.findByStatus(status);
}

// Multiple query params
@GetMapping("/search")
public List<OrderResponse> search(
    @RequestParam String status,
    @RequestParam Integer limit
) {
    // /api/orders/search?status=PENDING&limit=10
    return orderService.findByStatusWithLimit(status, limit);
}

// Optional query param
@GetMapping("/search")
public List<OrderResponse> search(
    @RequestParam(required = false) String status
) {
    // /api/orders/search (status = null)
    // /api/orders/search?status=PENDING (status = "PENDING")
    return orderService.findByStatusOrAll(status);
}

// Default value
@GetMapping("/search")
public List<OrderResponse> search(
    @RequestParam(defaultValue = "PENDING") String status
) {
    return orderService.findByStatus(status);
}
```

### Request Headers

```java
@GetMapping("/orders")
public List<OrderResponse> getOrders(
    @RequestHeader("X-User-Id") String userId,
    @RequestHeader(value = "Authorization", required = false) String auth
) {
    return orderService.getByUser(userId);
}
```

### Request Body

```java
@PostMapping("/orders")
public OrderResponse create(@Valid @RequestBody CreateOrderRequest request) {
    return orderService.createOrder(request);
}
```

---

## Request Validation

### Using @Valid

```java
@PostMapping("/orders")
public OrderResponse create(@Valid @RequestBody CreateOrderRequest request) {
    // @Valid triggers validation based on annotations in CreateOrderRequest
    // If validation fails:
    // - Spring returns HTTP 400 Bad Request
    // - Method is NOT called
    // - Validation errors returned as JSON
}
```

### DTO with Validation Annotations

```java
public class CreateOrderRequest {
    
    @NotBlank(message = "Customer ID is required")
    private String customerId;
    
    @NotEmpty(message = "Order must contain at least one item")
    @Valid // Cascades validation to items
    private List<OrderItemRequest> items;
    
    // Getters/Setters
}

public class OrderItemRequest {
    
    @NotBlank(message = "Product ID is required")
    private String productId;
    
    @Min(value = 1, message = "Quantity must be at least 1")
    private int quantity;
    
    @NotNull(message = "Price is required")
    @DecimalMin(value = "0.01", message = "Price must be greater than 0")
    private BigDecimal price;
    
    // Getters/Setters
}
```

### Validation Error Response

When validation fails, Spring Boot returns:

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

## HTTP Status Codes

### Standard Status Codes

| Code | Meaning | When to Use |
|------|---------|-------------|
| 200 | OK | Successful GET, PUT, PATCH, DELETE (with response body) |
| 201 | Created | Successful POST that creates resource |
| 204 | No Content | Successful DELETE (no response body) |
| 400 | Bad Request | Validation failed, malformed request |
| 401 | Unauthorized | Authentication required |
| 403 | Forbidden | Authenticated but not authorized |
| 404 | Not Found | Resource doesn't exist |
| 409 | Conflict | Business rule violation, duplicate |
| 500 | Internal Server Error | Unexpected server error |

### Setting Status Codes

```java
// Method 1: @ResponseStatus annotation
@PostMapping
@ResponseStatus(HttpStatus.CREATED) // 201
public OrderResponse create(@Valid @RequestBody CreateOrderRequest request) {
    return orderService.createOrder(request);
}

// Method 2: ResponseEntity
@PostMapping
public ResponseEntity<OrderResponse> create(@Valid @RequestBody CreateOrderRequest request) {
    OrderResponse response = orderService.createOrder(request);
    return ResponseEntity.status(HttpStatus.CREATED).body(response);
}

// Method 3: ResponseEntity with location header
@PostMapping
public ResponseEntity<OrderResponse> create(@Valid @RequestBody CreateOrderRequest request) {
    OrderResponse response = orderService.createOrder(request);
    URI location = URI.create("/api/orders/" + response.getOrderId());
    return ResponseEntity.created(location).body(response);
}
```

---

## ResponseEntity for Advanced Responses

### Basic Usage

```java
@GetMapping("/{id}")
public ResponseEntity<OrderResponse> getOrder(@PathVariable String id) {
    try {
        OrderResponse order = orderService.getById(id);
        return ResponseEntity.ok(order); // 200 OK
    } catch (OrderNotFoundException e) {
        return ResponseEntity.notFound().build(); // 404 Not Found
    }
}
```

### With Headers

```java
@PostMapping
public ResponseEntity<OrderResponse> create(@Valid @RequestBody CreateOrderRequest request) {
    OrderResponse response = orderService.createOrder(request);
    
    HttpHeaders headers = new HttpHeaders();
    headers.add("X-Order-Id", response.getOrderId());
    headers.add("Location", "/api/orders/" + response.getOrderId());
    
    return ResponseEntity
        .status(HttpStatus.CREATED)
        .headers(headers)
        .body(response);
}
```

### Conditional Responses

```java
@GetMapping("/{id}")
public ResponseEntity<OrderResponse> getOrder(@PathVariable String id) {
    Optional<OrderResponse> order = orderService.findById(id);
    
    return order
        .map(ResponseEntity::ok)               // 200 if found
        .orElse(ResponseEntity.notFound().build()); // 404 if not found
}
```

---

## Error Handling

### Method-Level Exception Handling

```java
@RestController
@RequestMapping("/api/orders")
public class OrderController {
    
    @GetMapping("/{id}")
    public OrderResponse getOrder(@PathVariable String id) {
        return orderService.getById(id);
        // Throws OrderNotFoundException if not found
    }
    
    // Handle exception in same controller
    @ExceptionHandler(OrderNotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public ErrorResponse handleNotFound(OrderNotFoundException ex) {
        return new ErrorResponse(ex.getMessage());
    }
}
```

### Global Exception Handling (@ControllerAdvice)

```java
@RestControllerAdvice
public class GlobalExceptionHandler {
    
    @ExceptionHandler(OrderNotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public ErrorResponse handleNotFound(OrderNotFoundException ex) {
        return new ErrorResponse(
            HttpStatus.NOT_FOUND.value(),
            ex.getMessage(),
            LocalDateTime.now()
        );
    }
    
    @ExceptionHandler(MethodArgumentNotValidException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ValidationErrorResponse handleValidation(MethodArgumentNotValidException ex) {
        Map<String, String> errors = new HashMap<>();
        ex.getBindingResult().getFieldErrors().forEach(error ->
            errors.put(error.getField(), error.getDefaultMessage())
        );
        return new ValidationErrorResponse(errors);
    }
    
    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ErrorResponse handleGeneric(Exception ex) {
        return new ErrorResponse(
            HttpStatus.INTERNAL_SERVER_ERROR.value(),
            "An unexpected error occurred",
            LocalDateTime.now()
        );
    }
}
```

---

## Best Practices

### 1. Keep Controllers Thin

```java
// ❌ Bad: Business logic in controller
@PostMapping
public OrderResponse create(@Valid @RequestBody CreateOrderRequest request) {
    if (request.getItems().isEmpty()) {
        throw new ValidationException();
    }
    Order order = new Order();
    order.setOrderId(UUID.randomUUID().toString());
    order.setStatus(OrderStatus.PENDING);
    order.calculateTotal();
    // ... more logic
    return mapToResponse(order);
}

// ✅ Good: Delegate to service
@PostMapping
@ResponseStatus(HttpStatus.CREATED)
public OrderResponse create(@Valid @RequestBody CreateOrderRequest request) {
    return orderService.createOrder(request);
}
```

### 2. Use DTOs, Not Domain Models

```java
// ❌ Bad: Exposing domain model
@GetMapping("/{id}")
public Order getOrder(@PathVariable String id) {
    return orderService.getById(id); // Exposes JPA entity
}

// ✅ Good: Return DTO
@GetMapping("/{id}")
public OrderResponse getOrder(@PathVariable String id) {
    Order order = orderService.getById(id);
    return mapToResponse(order);
}
```

### 3. Use Proper HTTP Methods

```java
// ✅ Correct REST semantics
@GetMapping      // Read (safe, idempotent)
@PostMapping     // Create (not idempotent)
@PutMapping      // Full update (idempotent)
@PatchMapping    // Partial update (idempotent)
@DeleteMapping   // Delete (idempotent)

// ❌ Bad: Using POST for everything
@PostMapping("/getOrder")
@PostMapping("/updateOrder")
@PostMapping("/deleteOrder")
```

### 4. Validate All Inputs

```java
@PostMapping
public OrderResponse create(
    @Valid @RequestBody CreateOrderRequest request // Validate body
) {
    return orderService.createOrder(request);
}

@GetMapping("/{id}")
public OrderResponse get(
    @PathVariable @Pattern(regexp = "^[0-9a-f-]+$") String id // Validate path
) {
    return orderService.getById(id);
}
```

### 5. Use Consistent Naming

```java
// ✅ Good: RESTful naming
GET    /api/orders          // List
GET    /api/orders/{id}     // Get by ID
POST   /api/orders          // Create
PUT    /api/orders/{id}     // Update
DELETE /api/orders/{id}     // Delete

// ❌ Bad: RPC-style naming
POST   /api/createOrder
POST   /api/getOrderById
POST   /api/updateOrder
```

---

## Common Pitfalls

### 1. Business Logic in Controller

**Problem:** Controller contains business rules

**Solution:** Move to service layer

### 2. Direct Repository Access

**Problem:** Controller calls repository directly

**Solution:** Always go through service layer

### 3. No Input Validation

**Problem:** Accepting any input without validation

**Solution:** Use `@Valid` and validation annotations

### 4. Wrong HTTP Status Codes

**Problem:** Returning 200 for everything

**Solution:** Use appropriate status codes (201, 204, 404, etc.)

### 5. Exposing Internal Exceptions

**Problem:** Stack traces in response

**Solution:** Use `@ControllerAdvice` for consistent error handling

---

## Related Topics

- [Service Layer](./service-layer.md)
- [Dependency Injection](./dependency-injection.md)
- [DTO Pattern](./dto-pattern.md)
- [Request Validation](./validation.md)
- [Error Handling](./error-handling.md)
- [Testing Controllers](./testing.md)
