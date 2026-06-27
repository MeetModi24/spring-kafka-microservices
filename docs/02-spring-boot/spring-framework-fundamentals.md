# Spring Framework Fundamentals

> **Source**: Consolidated from terminal notes and learning materials

---

## Part 1: Why Spring Boot Exists

Before Spring Boot existed, Java web development looked like this:

```
Browser
   │
HTTP Request
   │
Apache HTTP Server
   │
Tomcat
   │
Servlet
   │
Business Logic
   │
Database
```

A developer had to configure every layer manually.

### Traditional Java Web Setup

For example, to create a REST API, you had to:

1. Install Tomcat separately
2. Configure Tomcat
3. Configure DispatcherServlet
4. Configure Jackson JSON library
5. Configure dependency injection
6. Configure logging
7. Configure transactions
8. Configure database pool
9. Configure XML files

Even a simple "Hello World" required hundreds of lines of configuration.

### Evolution

- **Spring Framework** made this easier using Dependency Injection
- **Spring Boot** made it almost automatic

---

## Understanding the Relationship

Many people think Spring Boot is the framework. **It isn't.**

```
                 Spring Boot
                     │
         -------------------------
         │                       │
 Auto Configuration      Embedded Server
         │
         │
     Spring Framework
         │
         │
 IOC + DI + AOP + MVC + Security + Data
```

**Spring Boot simply sits on top of Spring Framework.**

The real engine is still Spring.

---

## What is Spring Framework?

Spring Framework is basically a huge collection of Java libraries.

It provides solutions for:

- Dependency Injection
- Web Applications
- REST APIs
- Transactions
- Database
- Security
- Scheduling
- Caching
- Messaging
- Testing

Instead of writing everything yourself, Spring provides ready-made implementations.

---

## Core Idea of Spring

Imagine you have `OrderService` which needs `PaymentService` and `InventoryService`.

### Without Spring (Bad):

```java
public class OrderService {
    PaymentService paymentService = new PaymentService();
    InventoryService inventoryService = new InventoryService();
}
```

**Problems:**
- OrderService itself creates the objects
- It becomes tightly coupled
- Hard to test (can't mock dependencies)
- Can't swap implementations

### With Spring (Good):

Spring says:
> "Don't create objects yourself. Instead, tell me what you need. I'll create it."

This is called **Inversion of Control (IoC)**.

```
Normally:
Your code → creates objects

Spring reverses this:
Spring Container → creates objects → gives them to your classes

Hence: Inversion of Control
```

---

## Dependency Injection

Suppose `OrderService` needs `PaymentService`.

### Instead of:

```java
PaymentService payment = new PaymentService();
```

### You write:

```java
@Service
public class OrderService {
    private final PaymentService payment;
    
    public OrderService(PaymentService payment) {
        this.payment = payment;  // Spring injects this
    }
}
```

**Notice:** There is **NO** `new` keyword.

- Spring creates the object
- Spring injects it

Hence: **Dependency Injection**

---

## What is the Spring Container?

This is probably the most important concept.

When your application starts:

```
main() → calls → SpringApplication.run(...)
```

This creates something called **ApplicationContext**.

### ApplicationContext is the Spring Container

Think of it like a **Huge Object Factory**.

Instead of:
```java
new A()
new B()
new C()
```

Spring keeps all objects inside one container:

```
+--------------------------------+
|        Spring Container         |
|--------------------------------|
| UserService                    |
| OrderService                   |
| PaymentService                 |
| KafkaProducer                  |
| KafkaConsumer                  |
| ObjectMapper                   |
| JdbcTemplate                   |
| DataSource                     |
| TransactionManager             |
+--------------------------------+
```

Whenever anyone asks:
> "Give me PaymentService"

Spring returns the **existing object**.

---

## Why Existing Object?

Because objects are expensive.

### Imagine:

**Without Spring:**
- 1000 HTTP requests
- 1000 PaymentService objects created

**With Spring:**
- 1000 HTTP requests
- 1 PaymentService (singleton)
- Shared by everyone

Default scope is **Singleton**.

---

## What is a Bean?

This word confuses everyone.

**A Bean is simply:**
> An object managed by Spring.

That's it. Nothing magical.

```
UserService object
    ↓
Managed by Spring
    ↓
Spring Bean
```

### Example:

```java
@Service
public class UserService {
}
```

Spring sees `@Service`:
1. Creates `new UserService()`
2. Stores it inside `ApplicationContext`

Now `UserService` is a **Bean**.

---

## How Does Spring Know Which Classes to Create?

This is called **Component Scanning**.

### Your Project Structure:

```
com.company
    │
    ├── OrderService
    ├── UserService
    ├── ProductController
    └── KafkaProducer
```

Your main class:

```java
@SpringBootApplication  // Contains @ComponentScan internally
public class App {
}
```

Spring starts scanning packages.

Whenever it finds:
- `@Component`
- `@Service`
- `@Repository`
- `@Controller`
- `@RestController`

It creates objects.

---

## Life of Spring Boot Startup

Let's slow down and see everything that happens:

```java
@SpringBootApplication
public class App {
    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
    }
}
```

### Execution Flow:

```
main()
  ↓
SpringApplication.run()
  ↓
Create ApplicationContext
  ↓
Read application.properties
  ↓
Read dependencies
  ↓
Auto Configuration
  ↓
Scan packages
  ↓
Create Beans
  ↓
Inject dependencies
  ↓
Start Tomcat
  ↓
Application Ready
```

Only after all this, your API becomes available.

---

## What is @SpringBootApplication?

This annotation is actually **three annotations** together:

```
@SpringBootApplication
    =
@Configuration
    +
@EnableAutoConfiguration
    +
@ComponentScan
```

### 1. @Configuration

Allows you to define beans manually:

```java
@Configuration
public class AppConfig {
    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}
```

Spring executes this method and stores `ObjectMapper` inside container.

### 2. @ComponentScan

Scans all packages for:
- `@Component`
- `@Service`
- `@Controller`
- `@Repository`

And creates beans.

### 3. @EnableAutoConfiguration

**This is where the magic begins.**

This is the most powerful feature of Spring Boot.

---

## Auto Configuration

Suppose your `pom.xml` contains:

```xml
<dependency>
    <artifactId>spring-boot-starter-web</artifactId>
</dependency>
```

Spring Boot says:
> "I see Web Starter."

So automatically:
- Configure Tomcat
- Configure DispatcherServlet
- Configure Jackson
- Configure Request Mapping
- Configure Error Pages
- Configure MVC

**No configuration required.**

### Another Example:

Suppose later you add:

```xml
<dependency>
    <artifactId>spring-boot-starter-data-jpa</artifactId>
</dependency>
```

Spring Boot notices and automatically creates:
- DataSource
- EntityManager
- Hibernate
- TransactionManager
- JpaRepositories

**No XML.**

### Kafka Example:

```xml
<dependency>
    <artifactId>spring-kafka</artifactId>
</dependency>
```

Immediately these become Beans:
- KafkaTemplate
- ProducerFactory
- ConsumerFactory
- KafkaListenerContainer

---

## How Does Auto-Configuration Actually Work?

This is one of the most impressive parts of Spring Boot.

Imagine your `pom.xml` contains:

```xml
<dependency>
    <artifactId>spring-boot-starter-web</artifactId>
</dependency>
<dependency>
    <artifactId>spring-kafka</artifactId>
</dependency>
```

### During Startup

Spring Boot checks the classpath (all libraries available to the application).

It asks questions like:

- Is Tomcat present? **Yes.**
- Is Spring MVC present? **Yes.**
- Is Jackson present? **Yes.**
- Is Kafka client present? **Yes.**

Based on those answers, it loads predefined configuration classes:

- WebMvcAutoConfiguration
- JacksonAutoConfiguration
- ServletWebServerFactoryAutoConfiguration
- KafkaAutoConfiguration

### Conditional Logic

Each configuration class contains conditional logic:

```java
@Configuration
@ConditionalOnClass(KafkaTemplate.class)
public class KafkaAutoConfiguration {
    // ... bean definitions
}
```

Meaning:
> "Only create Kafka-related beans if the Kafka library exists."

Similarly:

```java
@ConditionalOnMissingBean
```

Means:
> "Only create this bean if the developer hasn't already created one."

This allows you to override Spring Boot's defaults without modifying its source code.

---

## Embedded Tomcat

### Traditionally:

```
Your WAR File
    │
Deploy into Tomcat
```

### With Spring Boot:

```
Your Application
    │
contains
Tomcat inside
```

Running:

```bash
java -jar app.jar
```

Starts:

```
Application
    │
Embedded Tomcat
    │
HTTP Server
```

**No external installation required.**

---

## Request Lifecycle

Suppose someone visits:

```
GET /orders/123
```

The flow looks like this:

```
Browser
   │
HTTP Request
   │
Embedded Tomcat
   │
DispatcherServlet
   │
Handler Mapping
   │
OrderController
   │
OrderService
   │
Repository
   │
Database
   │
Repository
   │
Controller
   │
Jackson
   │
HTTP Response (JSON)
```

Each layer has a **single responsibility**, making the application modular and easier to maintain.

---

## Why Spring Boot Feels "Magical"

Everything is driven by **convention over configuration**.

Instead of asking you to configure every detail, Spring Boot assumes sensible defaults:

- Port 8080
- Embedded Tomcat
- Jackson for JSON
- Singleton bean scope
- Component scanning from your main package
- Default error handling
- Sensible logging configuration

You only customize what differs from the defaults, usually through `application.properties` or by defining your own beans.

---

## Mental Model to Remember

Think of Spring Boot as three major layers:

```
                 Your Code
         (Controllers, Services, Repositories)
                        │
                        ▼
          Spring Framework (Core Engine)
   IoC • Dependency Injection • MVC • AOP • Data
                        │
                        ▼
         Spring Boot (Automation Layer)
 Auto Configuration • Embedded Server • Starters
```

### Simple Way to Remember:

- **Spring Framework** provides the capabilities
- **Spring Boot** configures those capabilities automatically
- **Your application** focuses on business logic instead of infrastructure

---

## What's Next?

Once these concepts are clear, the next major topics become much easier to understand:

1. IoC Container and ApplicationContext internals
   - Bean creation, lifecycle, scopes
   - Dependency resolution

2. Dependency Injection in depth
   - Constructor injection, field injection
   - Circular dependencies, qualifiers
   - Lazy initialization

3. Spring MVC architecture
   - DispatcherServlet, HandlerMapping
   - HandlerAdapter, ViewResolver
   - Message converters

4. Auto-configuration internals
   - Starters, conditional annotations
   - Configuration metadata

5. Bean lifecycle
   - @PostConstruct, InitializingBean
   - BeanPostProcessor, destruction callbacks

6. AOP (Aspect-Oriented Programming)
   - Proxies, transactions
   - Logging, security

7. Spring Data JPA and transaction management

8. Spring Boot startup sequence and request processing

---

## Related Documents

- [Dependency Injection Deep Dive](./dependency-injection.md)
- [Annotations Reference](./annotations-reference.md)
- [Project Structure](./project-structure.md)
- [Application Configuration](./configuration.md)
