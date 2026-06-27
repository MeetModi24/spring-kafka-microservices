# Dependency Injection Deep Dive

> Understanding Spring's core feature: Inversion of Control and Dependency Injection

---

## What is Dependency Injection (DI)?

Dependency Injection is a design pattern where objects receive their dependencies from external sources rather than creating them internally.

### Bad Approach (Tight Coupling)

```java
public class OrderController {
    private OrderService orderService = new OrderService(); // ❌ Creating dependency manually
    
    public OrderResponse createOrder(CreateOrderRequest request) {
        return orderService.createOrder(request);
    }
}
```

**Problems:**
- Controller creates OrderService → **tight coupling**
- Hard to test (can't mock OrderService)
- Can't swap implementations
- If OrderService constructor changes, Controller breaks
- Violates Single Responsibility Principle

### Good Approach (Dependency Injection)

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
- **Loose coupling** - Controller doesn't know how OrderService is created
- Single Responsibility - Controller doesn't manage OrderService lifecycle
- Easy to swap implementations

---

## How Spring DI Works (IoC Container)

### Visual Representation

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

### How It Works

**Step 1: Component Scanning**
```java
@SpringBootApplication // Contains @ComponentScan
public class App {
    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
    }
}
```

Spring scans packages for:
- `@Component`
- `@Service`
- `@Repository`
- `@Controller`
- `@RestController`

**Step 2: Bean Creation**

Spring creates instances (beans) and stores them in ApplicationContext:

```java
@Service
public class OrderService {
    // Spring creates: new OrderService()
}

@RestController
public class OrderController {
    // Spring creates: new OrderController(orderService)
}
```

**Step 3: Dependency Resolution**

```
1. Spring sees OrderController needs OrderService
2. Looks in ApplicationContext for OrderService bean
3. Finds it (created in Step 2)
4. Passes it to OrderController constructor
5. OrderController is now ready to use
```

**Step 4: You Never Use `new`**

Spring manages the entire lifecycle - you just declare dependencies.

---

## Spring Stereotypes (Annotations)

| Annotation | Purpose | Where to Use | Special Features |
|------------|---------|--------------|------------------|
| `@Component` | Generic Spring-managed bean | Utility classes, helpers | None |
| `@Service` | Business logic layer | Service classes | Semantic only |
| `@Repository` | Data access layer | DAO, JPA repositories | Exception translation |
| `@Controller` | Web controller (returns views) | MVC controllers | View rendering |
| `@RestController` | REST API controller (returns JSON) | REST endpoints | `@Controller` + `@ResponseBody` |

### They're All @Component Under the Hood!

```java
@Service  // = @Component + semantic meaning
@Repository  // = @Component + exception translation
@RestController  // = @Controller + @ResponseBody
```

### Why Semantic Annotations?

1. **Code Readability**
   - You instantly know what layer a class belongs to
   - `@Service` = business logic
   - `@Repository` = data access
   - `@Controller` = web layer

2. **Future Spring Features**
   - Spring might treat them differently in future versions
   - Already: `@Repository` translates database exceptions

3. **AOP Pointcuts**
   - You can target specific stereotypes
   - Example: "Apply logging to all @Service classes"

---

## Three Ways to Inject Dependencies

### 1. Constructor Injection (✅ RECOMMENDED)

```java
@Service
public class OrderService {
    private final OrderRepository repo; // final = immutable after construction
    
    public OrderService(OrderRepository repo) {
        this.repo = repo;
    }
}
```

**Pros:**
- ✅ Immutable (use `final`)
- ✅ Easy to test (pass mock in constructor)
- ✅ Clear dependencies (visible in constructor signature)
- ✅ Required dependencies are obvious
- ✅ Prevents circular dependencies (compile error)
- ✅ Works without Spring (plain Java)

**With Lombok:**
```java
@Service
@RequiredArgsConstructor // Generates constructor for final fields
public class OrderService {
    private final OrderRepository repo;
}
```

### 2. Setter Injection (⚠️ OPTIONAL DEPENDENCIES)

```java
@Service
public class OrderService {
    private OrderRepository repo;
    
    @Autowired
    public void setRepo(OrderRepository repo) {
        this.repo = repo;
    }
}
```

**When to Use:**
- Optional dependencies (can be null)
- Dependencies that can be reconfigured

**Cons:**
- ⚠️ Mutable (can be changed after construction)
- ⚠️ Less clear which dependencies are required
- ⚠️ Can lead to partially constructed objects

### 3. Field Injection (❌ AVOID)

```java
@Service
public class OrderService {
    @Autowired
    private OrderRepository repo;
}
```

**Why It's Bad:**
- ❌ Can't test without Spring (can't pass mock)
- ❌ Hides dependencies (not visible in constructor)
- ❌ Mutable (can be changed)
- ❌ Can't use `final`
- ❌ Violates encapsulation
- ❌ Common but considered **bad practice**

**Why It's Popular:**
- Less boilerplate (no constructor needed)
- Looks cleaner (but isn't better)

---

## Dependency Injection Scenarios

### Single Dependency

```java
@RestController
public class OrderController {
    private final OrderService orderService;
    
    // Spring injects OrderService
    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }
}
```

### Multiple Dependencies

```java
@Service
public class OrderService {
    private final OrderRepository orderRepository;
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;
    private final CustomerService customerService;
    
    // Spring injects all three
    public OrderService(
        OrderRepository orderRepository,
        KafkaTemplate<String, OrderEvent> kafkaTemplate,
        CustomerService customerService
    ) {
        this.orderRepository = orderRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.customerService = customerService;
    }
}
```

### Multiple Implementations (Qualifiers)

```java
// Two implementations of the same interface
@Service("inMemoryOrderRepo")
public class InMemoryOrderRepository implements OrderRepository { }

@Service("jpaOrderRepo")
public class JpaOrderRepository implements OrderRepository { }

// Which one to inject?
@Service
public class OrderService {
    private final OrderRepository orderRepository;
    
    public OrderService(@Qualifier("jpaOrderRepo") OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }
}
```

### Optional Dependencies

```java
@Service
public class OrderService {
    private final OrderRepository orderRepository; // Required
    private NotificationService notificationService; // Optional
    
    // Required dependency via constructor
    public OrderService(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }
    
    // Optional dependency via setter
    @Autowired(required = false)
    public void setNotificationService(NotificationService notificationService) {
        this.notificationService = notificationService;
    }
}
```

---

## Bean Scopes

By default, Spring beans are **singletons** (one instance per container).

### Singleton (Default)

```java
@Service
@Scope("singleton") // Default - can omit
public class OrderService {
}
```

**Characteristics:**
- One instance per Spring container
- Shared by all clients
- Created at startup (or lazy)
- Thread-safe by design (stateless)

### Prototype (New Instance Each Time)

```java
@Service
@Scope("prototype")
public class OrderProcessor {
}
```

**Characteristics:**
- New instance for each injection
- Not shared
- Created on demand
- Spring doesn't manage destruction

### Request (Web Applications)

```java
@Service
@Scope("request")
public class ShoppingCart {
}
```

**Characteristics:**
- One instance per HTTP request
- Destroyed after request completes
- Only in web applications

### Session (Web Applications)

```java
@Service
@Scope("session")
public class UserPreferences {
}
```

**Characteristics:**
- One instance per HTTP session
- Survives across multiple requests
- Destroyed when session expires

---

## Circular Dependencies

### The Problem

```java
@Service
public class A {
    private final B b;
    public A(B b) { this.b = b; }
}

@Service
public class B {
    private final A a;
    public B(A a) { this.a = a; }
}

// Spring fails: "The dependencies of some beans in the application context form a cycle"
```

### Solutions

**1. Refactor (Best)**
```java
// Extract common logic to avoid circular dependency
@Service
public class CommonService { }

@Service
public class A {
    private final CommonService common;
    public A(CommonService common) { this.common = common; }
}

@Service
public class B {
    private final CommonService common;
    public B(CommonService common) { this.common = common; }
}
```

**2. Setter Injection (Workaround)**
```java
@Service
public class A {
    private final B b;
    public A(B b) { this.b = b; }
}

@Service
public class B {
    private A a;
    
    @Autowired
    public void setA(A a) { this.a = a; } // Breaks cycle
}
```

**3. @Lazy (Last Resort)**
```java
@Service
public class A {
    private final B b;
    public A(@Lazy B b) { this.b = b; } // B created lazily
}

@Service
public class B {
    private final A a;
    public B(A a) { this.a = a; }
}
```

---

## Testing with Dependency Injection

### Unit Test (Mock Dependencies)

```java
@ExtendWith(MockitoExtension.class)
class OrderServiceTest {
    
    @Mock
    private OrderRepository orderRepository;
    
    @InjectMocks
    private OrderService orderService;
    
    @Test
    void testCreateOrder() {
        // Given
        CreateOrderRequest request = new CreateOrderRequest(...);
        when(orderRepository.save(any())).thenReturn(savedOrder);
        
        // When
        Order result = orderService.createOrder(request);
        
        // Then
        assertNotNull(result.getOrderId());
        verify(orderRepository).save(any());
    }
}
```

### Integration Test (Real Spring Context)

```java
@SpringBootTest
class OrderServiceIntegrationTest {
    
    @Autowired
    private OrderService orderService; // Real bean, real dependencies
    
    @Test
    void testCreateOrderIntegration() {
        CreateOrderRequest request = new CreateOrderRequest(...);
        Order result = orderService.createOrder(request);
        assertNotNull(result.getOrderId());
    }
}
```

---

## Best Practices

1. **Always use constructor injection**
   - Makes dependencies explicit
   - Enables immutability
   - Easier to test

2. **Use final for dependencies**
   - Prevents accidental reassignment
   - Documents that dependency is required

3. **Prefer interfaces over concrete classes**
   ```java
   // Good
   private final OrderRepository repo;
   
   // Bad
   private final JpaOrderRepository repo;
   ```

4. **Keep constructors simple**
   - Just assignment, no logic
   - Complex initialization → `@PostConstruct`

5. **Avoid circular dependencies**
   - Sign of poor design
   - Refactor to break the cycle

6. **Don't inject too many dependencies**
   - More than 3-4 dependencies = code smell
   - Consider breaking into smaller services

---

## Common Pitfalls

### 1. Field Injection

```java
// ❌ Don't do this
@Autowired
private OrderService orderService;
```

### 2. Using @Autowired on Constructor

```java
// Unnecessary since Spring 4.3
@Service
public class OrderService {
    private final OrderRepository repo;
    
    @Autowired // ← Not needed if there's only one constructor
    public OrderService(OrderRepository repo) {
        this.repo = repo;
    }
}
```

### 3. Injecting Request-Scoped Bean into Singleton

```java
@Service // Singleton
public class OrderService {
    private final ShoppingCart cart; // Request-scoped
    
    // ❌ Will fail - can't inject short-lived bean into long-lived bean
    public OrderService(ShoppingCart cart) {
        this.cart = cart;
    }
}

// ✅ Solution: Use Provider
@Service
public class OrderService {
    private final Provider<ShoppingCart> cartProvider;
    
    public OrderService(Provider<ShoppingCart> cartProvider) {
        this.cartProvider = cartProvider;
    }
    
    public void processOrder() {
        ShoppingCart cart = cartProvider.get(); // Get current request's cart
    }
}
```

---

## Related Topics

- [Spring Framework Fundamentals](./spring-framework-fundamentals.md)
- [Annotations Reference](./annotations-reference.md)
- [Service Layer Pattern](./service-layer.md)
- [Testing Spring Applications](./testing.md)
