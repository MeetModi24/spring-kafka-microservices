# Error Handling in Spring Boot - Deep Dive

> **Status:** ✅ Implemented in order-service  
> **Pattern:** Global Exception Handling with @RestControllerAdvice  
> **Learning Focus:** AOP, Clean Architecture, HTTP Status Codes

---

## Table of Contents

1. [Why Global Exception Handling?](#why-global-exception-handling)
2. [Design Pattern: @RestControllerAdvice](#design-pattern-restcontrolleradvice)
3. [Custom Exceptions](#custom-exceptions)
4. [Error Response DTO](#error-response-dto)
5. [Implementation Details](#implementation-details)
6. [Testing Error Handling](#testing-error-handling)
7. [Interview Questions](#interview-questions)

---

## Why Global Exception Handling?

### Without Global Exception Handler

```java
@RestController
public class OrderController {
    
    @GetMapping("/orders/{id}")
    public ResponseEntity<?> getOrder(@PathVariable String id) {
        try {
            Order order = orderService.getOrderById(id);
            return ResponseEntity.ok(order);
        } catch (OrderNotFoundException ex) {
            ErrorResponse error = new ErrorResponse(404, "Not Found", ex.getMessage());
            return ResponseEntity.status(404).body(error);
        } catch (Exception ex) {
            ErrorResponse error = new ErrorResponse(500, "Error", "Internal error");
            return ResponseEntity.status(500).body(error);
        }
    }
    
    @PostMapping("/orders")
    public ResponseEntity<?> createOrder(@RequestBody CreateOrderRequest request) {
        try {
            Order order = orderService.createOrder(request);
            return ResponseEntity.status(201).body(order);
        } catch (InvalidOrderException ex) {
            ErrorResponse error = new ErrorResponse(400, "Bad Request", ex.getMessage());
            return ResponseEntity.status(400).body(error);
        } catch (Exception ex) {
            ErrorResponse error = new ErrorResponse(500, "Error", "Internal error");
            return ResponseEntity.status(500).body(error);
        }
    }
    
    // ... Every endpoint needs try-catch blocks!
}
```

**Problems:**
- ❌ **Code Duplication**: Every endpoint repeats the same error handling logic
- ❌ **Inconsistent Responses**: Different developers might format errors differently
- ❌ **Hard to Maintain**: Changing error format requires updating all controllers
- ❌ **Violates DRY**: Don't Repeat Yourself principle
- ❌ **Mixes Concerns**: Business logic mixed with error handling

### With Global Exception Handler

```java
@RestController
public class OrderController {
    
    @GetMapping("/orders/{id}")
    public OrderResponse getOrder(@PathVariable String id) {
        Order order = orderService.getOrderById(id);  // If throws, handler catches it
        return mapToResponse(order);
    }
    
    @PostMapping("/orders")
    @ResponseStatus(HttpStatus.CREATED)
    public OrderResponse createOrder(@Valid @RequestBody CreateOrderRequest request) {
        Order order = orderService.createOrder(request);  // Clean!
        return mapToResponse(order);
    }
}
```

**Benefits:**
- ✅ **Clean Controllers**: Focus on business logic only
- ✅ **Consistent Errors**: Single place defines error format
- ✅ **Easy to Maintain**: Change once, applies everywhere
- ✅ **Separation of Concerns**: Error handling is cross-cutting concern
- ✅ **DRY Principle**: Write once, use everywhere

---

## Design Pattern: @RestControllerAdvice

### What is @RestControllerAdvice?

**@RestControllerAdvice** = **@ControllerAdvice** + **@ResponseBody**

It's an **Aspect-Oriented Programming (AOP)** pattern that intercepts exceptions thrown by controllers.

### How It Works

```
1. Controller throws exception
        ↓
2. Spring intercepts exception
        ↓
3. Spring searches for @ExceptionHandler methods
        ↓
4. Matches exception type to handler method
        ↓
5. Handler creates ErrorResponse
        ↓
6. Spring serializes to JSON and returns to client
```

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────┐
│                     Client (Browser/Postman)            │
└────────────────────────┬────────────────────────────────┘
                         │
                         │ HTTP Request
                         ↓
┌─────────────────────────────────────────────────────────┐
│              DispatcherServlet (Spring MVC)             │
└────────────────────────┬────────────────────────────────┘
                         │
                         │ Route to Controller
                         ↓
┌─────────────────────────────────────────────────────────┐
│                  OrderController                         │
│  @GetMapping("/orders/{id}")                            │
│  public OrderResponse getOrder(String id) {             │
│      return orderService.getOrderById(id); ──────┐      │
│  }                                               │      │
└──────────────────────────────────────────────────┼──────┘
                                                   │
                                    Exception Thrown (OrderNotFoundException)
                                                   │
                                                   ↓
┌─────────────────────────────────────────────────────────┐
│           GlobalExceptionHandler                        │
│           @RestControllerAdvice                         │
│                                                          │
│  @ExceptionHandler(OrderNotFoundException.class)        │
│  public ResponseEntity<ErrorResponse> handle(...) {     │
│      ErrorResponse error = new ErrorResponse(...);      │
│      return ResponseEntity.status(404).body(error);     │
│  }                                                       │
└────────────────────────┬────────────────────────────────┘
                         │
                         │ ErrorResponse (JSON)
                         ↓
┌─────────────────────────────────────────────────────────┐
│                     Client                               │
│  {                                                       │
│    "status": 404,                                       │
│    "error": "Not Found",                                │
│    "message": "Order not found: invalid-id"             │
│  }                                                       │
└─────────────────────────────────────────────────────────┘
```

---

## Custom Exceptions

### Why Create Custom Exceptions?

**Instead of this:**
```java
throw new RuntimeException("Order not found: " + id);
```

**We create:**
```java
throw new OrderNotFoundException(id);
```

**Benefits:**
1. **Type Safety**: Can catch specific exception types
2. **Encapsulation**: Exception carries context (orderId)
3. **Clear Intent**: Code clearly shows what went wrong
4. **Specific Handling**: Different HTTP status for different exceptions
5. **Better Debugging**: Stack trace shows meaningful exception name

### Our Custom Exceptions

#### 1. OrderNotFoundException (404 Not Found)

```java
package com.example.orderservice.exception;

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

**When to throw:**
- Order ID doesn't exist in storage
- User tries to access order that doesn't belong to them
- Order was deleted

**HTTP Status:** 404 Not Found

#### 2. InvalidOrderException (400 Bad Request)

```java
package com.example.orderservice.exception;

public class InvalidOrderException extends RuntimeException {
    
    public InvalidOrderException(String message) {
        super(message);
    }
    
    public InvalidOrderException(String message, Throwable cause) {
        super(message, cause);
    }
}
```

**When to throw:**
- Order validation fails (business rules)
- Items have invalid quantities or prices
- Total amount doesn't match items

**HTTP Status:** 400 Bad Request

### Exception Hierarchy

```
RuntimeException (Java)
    ↓
OrderNotFoundException (404)
InvalidOrderException (400)
```

**Why extend RuntimeException?**
- **Unchecked exceptions**: Don't force try-catch everywhere
- **Spring compatibility**: Spring's @Transactional rolls back on RuntimeException
- **Clean code**: Controllers don't need throws declarations

---

## Error Response DTO

### Standard Error Response Format

```java
package com.example.orderservice.dto;

import java.time.LocalDateTime;
import java.util.List;

public class ErrorResponse {
    
    private LocalDateTime timestamp;  // When error occurred
    private int status;                // HTTP status code (404, 400, 500)
    private String error;              // Error type ("Not Found", "Bad Request")
    private String message;            // Human-readable message
    private String path;               // Request path that caused error
    private List<String> details;     // For validation errors (multiple fields)
    
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
    
    // Getters and setters...
}
```

### Why This Structure?

**Consistent Format:**
```json
{
  "timestamp": "2026-07-01T00:34:41.577582",
  "status": 404,
  "error": "Not Found",
  "message": "Order not found: invalid-id",
  "path": "/api/orders/invalid-id",
  "details": null
}
```

**Benefits:**
- ✅ **Predictable**: Frontend knows what to expect
- ✅ **Debuggable**: Timestamp helps correlate with logs
- ✅ **Informative**: Clear message for users/developers
- ✅ **Path Context**: Shows which endpoint failed
- ✅ **Validation Support**: `details` array for multiple errors

---

## Implementation Details

### GlobalExceptionHandler.java

```java
@RestControllerAdvice
public class GlobalExceptionHandler {
    
    // Handle 404 - Order Not Found
    @ExceptionHandler(OrderNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleOrderNotFound(
            OrderNotFoundException ex, 
            WebRequest request) {
        
        ErrorResponse error = new ErrorResponse(
            HttpStatus.NOT_FOUND.value(),      // 404
            "Not Found",
            ex.getMessage(),
            request.getDescription(false).replace("uri=", "")
        );
        
        return new ResponseEntity<>(error, HttpStatus.NOT_FOUND);
    }
    
    // Handle 400 - Invalid Order
    @ExceptionHandler(InvalidOrderException.class)
    public ResponseEntity<ErrorResponse> handleInvalidOrder(
            InvalidOrderException ex, 
            WebRequest request) {
        
        ErrorResponse error = new ErrorResponse(
            HttpStatus.BAD_REQUEST.value(),    // 400
            "Bad Request",
            ex.getMessage(),
            request.getDescription(false).replace("uri=", "")
        );
        
        return new ResponseEntity<>(error, HttpStatus.BAD_REQUEST);
    }
    
    // Handle 400 - Bean Validation Errors
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidationErrors(
            MethodArgumentNotValidException ex,
            WebRequest request) {
        
        // Extract all validation errors
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
    
    // Handle 500 - All Other Exceptions
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGlobalException(
            Exception ex, 
            WebRequest request) {
        
        ErrorResponse error = new ErrorResponse(
            HttpStatus.INTERNAL_SERVER_ERROR.value(),  // 500
            "Internal Server Error",
            "An unexpected error occurred. Please contact support.",
            request.getDescription(false).replace("uri=", "")
        );
        
        // Log full exception for debugging
        ex.printStackTrace();
        
        return new ResponseEntity<>(error, HttpStatus.INTERNAL_SERVER_ERROR);
    }
}
```

### Key Points

1. **@RestControllerAdvice**: Makes this class global for all controllers
2. **@ExceptionHandler**: Maps exception type to handler method
3. **WebRequest**: Provides request context (path, parameters)
4. **ResponseEntity**: Allows setting HTTP status code
5. **Most Specific First**: Order doesn't matter, Spring picks most specific match

---

## Testing Error Handling

### Test 1: Order Not Found (404)

```bash
curl -i http://localhost:8081/api/orders/invalid-id
```

**Response:**
```
HTTP/1.1 404 
Content-Type: application/json

{
  "timestamp": "2026-07-01T00:34:41.577582",
  "status": 404,
  "error": "Not Found",
  "message": "Order not found: invalid-id",
  "path": "/api/orders/invalid-id",
  "details": null
}
```

### Test 2: Validation Errors (400)

```bash
curl -i -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId": "", "items": []}'
```

**Response:**
```
HTTP/1.1 400
Content-Type: application/json

{
  "timestamp": "2026-07-01T00:34:41.6478",
  "status": 400,
  "error": "Validation Failed",
  "message": "Input validation failed. Check 'details' field.",
  "path": "/api/orders",
  "details": [
    "customerId: Customer ID is required",
    "items: Order must contain at least one item"
  ]
}
```

### Test 3: Invalid Order (400)

```bash
# Create order with invalid item (price = 0)
curl -i -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-123",
    "items": [{"productId": "PROD-001", "productName": "Laptop", "quantity": 1, "price": 0}]
  }'
```

**Response:**
```
HTTP/1.1 400
Content-Type: application/json

{
  "timestamp": "2026-07-01T00:35:00.123",
  "status": 400,
  "error": "Bad Request",
  "message": "Order validation failed: Check that all items have valid productId, quantity > 0, and price > 0",
  "path": "/api/orders",
  "details": null
}
```

---

## HTTP Status Codes Reference

### Success Codes (2xx)

| Code | Meaning | When to Use |
|------|---------|-------------|
| 200 | OK | GET request successful |
| 201 | Created | POST created new resource |
| 204 | No Content | DELETE successful, no response body |

### Client Error Codes (4xx)

| Code | Meaning | When to Use |
|------|---------|-------------|
| 400 | Bad Request | Invalid input, validation failure |
| 401 | Unauthorized | Authentication required |
| 403 | Forbidden | Authenticated but not authorized |
| 404 | Not Found | Resource doesn't exist |
| 409 | Conflict | Duplicate resource (e.g., email exists) |
| 422 | Unprocessable Entity | Valid JSON but business rule violation |

### Server Error Codes (5xx)

| Code | Meaning | When to Use |
|------|---------|-------------|
| 500 | Internal Server Error | Unexpected error (bug, database down) |
| 502 | Bad Gateway | Upstream service failed |
| 503 | Service Unavailable | Service temporarily down |
| 504 | Gateway Timeout | Upstream service timeout |

---

## Interview Questions

### Q1: What is the difference between @ControllerAdvice and @RestControllerAdvice?

**Answer:**
- `@ControllerAdvice`: For MVC controllers (returns view names)
- `@RestControllerAdvice`: For REST controllers (returns JSON)
- `@RestControllerAdvice = @ControllerAdvice + @ResponseBody`

### Q2: Why extend RuntimeException instead of Exception?

**Answer:**
- **RuntimeException** = Unchecked exception (no forced try-catch)
- **Exception** = Checked exception (forces try-catch everywhere)
- Benefits:
  - Cleaner code (no throws declarations in method signatures)
  - Spring's `@Transactional` auto-rollback on RuntimeException
  - Controllers don't need try-catch blocks

### Q3: How does @ExceptionHandler match exceptions?

**Answer:**
Spring uses **most specific match**:
1. Exact exception type match
2. Parent exception type match
3. Most specific parent if multiple handlers match

Example:
```java
@ExceptionHandler(OrderNotFoundException.class)  // Most specific
@ExceptionHandler(RuntimeException.class)         // Less specific
@ExceptionHandler(Exception.class)                // Least specific (catch-all)
```

### Q4: What is the benefit of WebRequest parameter?

**Answer:**
`WebRequest` provides request context:
- Request path (`/api/orders/123`)
- Request parameters (`?status=PENDING`)
- Headers
- Session attributes

Used to include request path in error response for debugging.

### Q5: Should you log exceptions in GlobalExceptionHandler?

**Answer:**
**Yes**, but with proper logging levels:
- `log.error()` for 500 errors (server bugs)
- `log.warn()` for 4xx errors (client mistakes)
- Include request ID for correlation

```java
@ExceptionHandler(Exception.class)
public ResponseEntity<ErrorResponse> handleGlobalException(Exception ex, WebRequest request) {
    log.error("Unexpected error at {}: {}", 
        request.getDescription(false), 
        ex.getMessage(), 
        ex);  // Include stack trace
    // ... return error response
}
```

### Q6: How to handle asynchronous exceptions (@Async methods)?

**Answer:**
Use `AsyncUncaughtExceptionHandler`:

```java
@Configuration
@EnableAsync
public class AsyncConfig implements AsyncConfigurer {
    
    @Override
    public AsyncUncaughtExceptionHandler getAsyncUncaughtExceptionHandler() {
        return (ex, method, params) -> {
            log.error("Async exception in {}: {}", method.getName(), ex.getMessage());
        };
    }
}
```

### Q7: How to test GlobalExceptionHandler?

**Answer:**
Two approaches:

**1. Integration Test:**
```java
@SpringBootTest
@AutoConfigureMockMvc
class OrderControllerTest {
    
    @Autowired
    private MockMvc mockMvc;
    
    @Test
    void testOrderNotFound() throws Exception {
        mockMvc.perform(get("/api/orders/invalid-id"))
            .andExpect(status().isNotFound())
            .andExpect(jsonPath("$.status").value(404))
            .andExpect(jsonPath("$.message").value("Order not found: invalid-id"));
    }
}
```

**2. Unit Test:**
```java
class GlobalExceptionHandlerTest {
    
    private GlobalExceptionHandler handler = new GlobalExceptionHandler();
    
    @Test
    void testHandleOrderNotFound() {
        OrderNotFoundException ex = new OrderNotFoundException("123");
        WebRequest request = mock(WebRequest.class);
        when(request.getDescription(false)).thenReturn("uri=/api/orders/123");
        
        ResponseEntity<ErrorResponse> response = handler.handleOrderNotFound(ex, request);
        
        assertEquals(404, response.getStatusCodeValue());
        assertEquals("Order not found: 123", response.getBody().getMessage());
    }
}
```

---

## Best Practices

### 1. Don't Expose Internal Details

❌ **Bad:**
```java
"message": "NullPointerException at OrderService.java:45"
"message": "SQL error: duplicate key value violates unique constraint"
```

✅ **Good:**
```java
"message": "Order not found"
"message": "Customer with this email already exists"
```

### 2. Provide Actionable Messages

❌ **Bad:**
```java
"message": "Invalid input"
```

✅ **Good:**
```java
"message": "Order validation failed: Check that all items have valid productId, quantity > 0, and price > 0"
```

### 3. Use Appropriate HTTP Status Codes

- **404**: Resource doesn't exist
- **400**: Invalid input
- **409**: Duplicate resource
- **422**: Valid format but business rule violation
- **500**: Server bug (never for validation errors)

### 4. Include Request Context

```java
ErrorResponse error = new ErrorResponse(
    404, 
    "Not Found", 
    "Order not found: " + orderId,
    request.getDescription(false)  // ← Include path
);
```

### 5. Log Appropriately

- **4xx errors**: `log.warn()` (client mistake)
- **5xx errors**: `log.error()` with full stack trace (server bug)
- Include correlation ID for distributed tracing

---

## Summary

✅ **What We Learned:**
1. Global exception handling with `@RestControllerAdvice`
2. Custom exceptions for type safety
3. Consistent error response format
4. HTTP status code semantics
5. Separation of concerns (AOP pattern)

✅ **Benefits:**
- Clean controllers focused on business logic
- Consistent error responses across all endpoints
- Easy to maintain (change once, applies everywhere)
- Better debugging with structured errors
- Professional API design

✅ **Next Steps:**
- Add more custom exceptions as needed
- Implement logging with correlation IDs
- Add integration tests for error scenarios
- Consider i18n for error messages

---

**Your error handling is production-ready!** 🎉
