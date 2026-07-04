Part 1: Why Spring Boot Exists

Before Spring Boot existed, Java web development looked like this:

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

A developer had to configure every layer manually.

For example, to create a REST API, you had to:

Install Tomcat separately
Configure Tomcat
Configure DispatcherServlet
Configure Jackson JSON library
Configure dependency injection
Configure logging
Configure transactions
Configure database pool
Configure XML files

Even a simple "Hello World" required hundreds of lines of configuration.

Spring Framework made this easier using Dependency Injection.

Spring Boot made it almost automatic.

First Understand Spring Framework

Many people think Spring Boot is the framework.

It isn't.

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

Spring Boot simply sits on top of Spring Framework.

The real engine is still Spring.

What is Spring Framework?

Spring Framework is basically a huge collection of Java libraries.

It provides solutions for:

Dependency Injection
Web Applications
REST APIs
Transactions
Database
Security
Scheduling
Caching
Messaging
Testing

Instead of writing everything yourself,
Spring provides ready-made implementations.

Core Idea of Spring

Imagine you have

OrderService

which needs

PaymentService

and

InventoryService

Without Spring:

public class OrderService {

    PaymentService paymentService = new PaymentService();

    InventoryService inventoryService = new InventoryService();

}

Problem:

OrderService itself creates the objects.

It becomes tightly coupled.

Spring says:

Don't create objects yourself.

Instead

Tell me what you need.

I'll create it.

This is called

Inversion of Control (IoC)

Normally

Your code
      │
creates objects

Spring reverses this.

Spring Container
        │
creates objects
        │
gives them to your classes

Hence

Inversion of Control

Dependency Injection

Suppose

OrderService

needs

PaymentService

Instead of

PaymentService payment = new PaymentService();

you write

@Service
public class OrderService {

    private final PaymentService payment;

    public OrderService(PaymentService payment){
        this.payment = payment;
    }

}

Notice something.

There is

NO new keyword.

Spring creates the object.

Spring injects it.

Hence

Dependency Injection.

What is the Spring Container?

This is probably the most important concept.

When your application starts

main()

calls

SpringApplication.run(...)

This creates something called

ApplicationContext

ApplicationContext is the Spring Container.

Think of it like

Huge Object Factory

Instead of

new A()
new B()
new C()

Spring keeps

A
B
C
D
E
...

inside one container.

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

Whenever anyone asks

Give me PaymentService

Spring returns the existing object.

Why Existing Object?

Because objects are expensive.

Imagine

1000 HTTP requests

Without Spring

1000 PaymentService objects

With Spring

1 PaymentService

shared by everyone.

Default scope is

Singleton.

What is a Bean?

This word confuses everyone.

A Bean is simply

An object managed by Spring.

That's it.

Nothing magical.

UserService object

↓

Managed by Spring

↓

Spring Bean

Example

@Service
public class UserService{
}

Spring sees

@Service

creates

new UserService()

stores it inside

ApplicationContext.

Now

UserService

is a Bean.

How does Spring know which classes to create?

This is called

Component Scanning.

Suppose your project

com.company

      │

      ├── OrderService

      ├── UserService

      ├── ProductController

      ├── KafkaProducer

Your main class

@SpringBootApplication
public class App{
}

contains

@ComponentScan

internally.

Spring starts scanning packages.

Whenever it finds

@Component
@Service
@Repository
@Controller
@RestController

it creates objects.

Life of Spring Boot Startup

Let's slow down and see everything that happens.

Suppose

@SpringBootApplication
public class App {

    public static void main(String args[]){

        SpringApplication.run(App.class,args);

    }

}

Execution:

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

Only after all this

your API becomes available.

What is @SpringBootApplication?

This annotation is actually

three annotations together.

@SpringBootApplication

=

@Configuration

+

@EnableAutoConfiguration

+

@ComponentScan

Let's understand each.

1. @Configuration

Suppose you want Spring to create an object manually.

@Configuration
public class AppConfig {

    @Bean
    public ObjectMapper objectMapper(){

        return new ObjectMapper();

    }

}

Spring executes this method.

Stores

ObjectMapper

inside container.

2. @ComponentScan

Scans all packages

@Component

@Service

@Controller

@Repository

and creates beans.

3. @EnableAutoConfiguration

This is where the magic begins.

This is the most powerful feature of Spring Boot.

Auto Configuration

Suppose your pom.xml contains

<dependency>

spring-boot-starter-web

</dependency>

Spring Boot says

"I see Web Starter."

So automatically

configure Tomcat
configure DispatcherServlet
configure Jackson
configure Request Mapping
configure Error Pages
configure MVC

No configuration required.

Suppose later you add

spring-boot-starter-data-jpa

Spring Boot notices.

Automatically creates

DataSource

EntityManager

Hibernate

TransactionManager

JpaRepositories

No XML.

Suppose you add

spring-kafka

Immediately

KafkaTemplate

ProducerFactory

ConsumerFactory

KafkaListenerContainer

become Beans.

Notice something.

You never told Spring

Create KafkaTemplate.

It figured it out.

How?

Because

@EnableAutoConfiguration

looks at dependencies.

How Does Auto-Configuration Actually Work?

This is one of the most impressive parts of Spring Boot.

Imagine your pom.xml contains:

<dependency>
    <artifactId>spring-boot-starter-web</artifactId>
</dependency>

<dependency>
    <artifactId>spring-kafka</artifactId>
</dependency>

During startup, Spring Boot checks the classpath (all libraries available to the application).

It asks questions like:

Is Tomcat present?
Yes.

Is Spring MVC present?
Yes.

Is Jackson present?
Yes.

Is Kafka client present?
Yes.

Based on those answers, it loads predefined configuration classes.

For example:

WebMvcAutoConfiguration
JacksonAutoConfiguration
ServletWebServerFactoryAutoConfiguration
KafkaAutoConfiguration

Each configuration class contains conditional logic such as:

@Configuration
@ConditionalOnClass(KafkaTemplate.class)
public class KafkaAutoConfiguration {
    ...
}

Meaning:

"Only create Kafka-related beans if the Kafka library exists."

Similarly:

@ConditionalOnMissingBean

means:

"Only create this bean if the developer hasn't already created one."

This allows you to override Spring Boot's defaults without modifying its source code.

Embedded Tomcat

Traditionally:

Your WAR File
      │
Deploy into Tomcat

With Spring Boot:

Your Application
       │
contains
Tomcat inside

Running:

java -jar app.jar

starts:

Application
     │
Embedded Tomcat
     │
HTTP Server

No external installation required.

Request Lifecycle

Suppose someone visits:

GET /orders/123

The flow looks like this:

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

Each layer has a single responsibility, making the application modular and easier to maintain.

Why Spring Boot Feels "Magical"

Everything is driven by convention over configuration.

Instead of asking you to configure every detail, Spring Boot assumes sensible defaults:

Port 8080
Embedded Tomcat
Jackson for JSON
Singleton bean scope
Component scanning from your main package
Default error handling
Sensible logging configuration

You only customize what differs from the defaults, usually through application.properties or by defining your own beans.

Mental Model to Remember

Think of Spring Boot as three major layers:

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

A simple way to remember the responsibilities is:

Spring Framework provides the capabilities.
Spring Boot configures those capabilities automatically.
Your application focuses on business logic instead of infrastructure.

This is only the foundation. Once these concepts are clear, the next major topics become much easier to understand:

IoC Container and ApplicationContext internals (bean creation, lifecycle, scopes, dependency resolution).
Dependency Injection in depth (constructor injection, field injection, circular dependencies, qualifiers, lazy initialization).
Spring MVC architecture (DispatcherServlet, HandlerMapping, HandlerAdapter, ViewResolver, message converters).
Auto-configuration internals (starters, conditional annotations, configuration metadata).
Bean lifecycle (@PostConstruct, InitializingBean, BeanPostProcessor, destruction callbacks).
AOP (Aspect-Oriented Programming) (proxies, transactions, logging, security).
Spring Data JPA and transaction management.
Spring Boot startup sequence and request processing.


---
  📘 Maven Deep Dive

  What is Maven?

  Maven is a build automation tool that manages:
  1. Dependencies (downloading JARs you need)
  2. Building (compiling .java → .class files)
  3. Testing (running unit tests)
  4. Packaging (creating executable .jar files)

  Maven Coordinates (GAV)

  Every Maven artifact has 3 coordinates:
  <groupId>com.example</groupId>        <!-- Organization/company -->
  <artifactId>order-service</artifactId> <!-- Project name -->
  <version>1.0.0-SNAPSHOT</version>      <!-- Version -->

  Combined, they create a unique identifier: com.example:order-service:1.0.0-SNAPSHOT

  SNAPSHOT means "in development". When you release, change to 1.0.0 (no SNAPSHOT).

  ---
  Parent POM: Why It Matters

  <parent>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-parent</artifactId>
      <version>3.2.0</version>
  </parent>

  This parent POM provides:
  - Dependency versions: You don't specify versions for Spring dependencies
  - Plugin configurations: Maven compiler plugin configured for Java 17
  - Resource filtering: Replaces @project.version@ in files with actual version

  Example: When you add spring-boot-starter-web, you don't specify version - parent decides.

  ---
  Dependency Scopes
   
  <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-test</artifactId>
      <scope>test</scope>  <!-- Only available during testing -->
  </dependency>

  Scopes:
  - compile (default) - Available everywhere (main code + tests)
  - test - Only in src/test, not packaged in final JAR
  - provided - Needed for compilation but provided by runtime (like servlet API in Tomcat)
  - runtime - Not needed for compilation but needed at runtime (like JDBC drivers)

  ---
  Spring Boot Starters: Convention over Configuration
   
  Instead of declaring 15 dependencies:
  <!-- DON'T DO THIS -->
  <dependency><artifactId>spring-web</artifactId></dependency>
  <dependency><artifactId>spring-webmvc</artifactId></dependency>
  <dependency><artifactId>jackson-databind</artifactId></dependency>
  <dependency><artifactId>tomcat-embed-core</artifactId></dependency>
  <!-- ... 11 more -->

  Use a starter:
  <!-- DO THIS -->
  <dependency>
      <artifactId>spring-boot-starter-web</artifactId>
  </dependency>

  ---
  Maven Build Lifecycle

  When you run mvn clean package, Maven executes these phases in order:

  clean → validate → compile → test → package → install → deploy

  Common commands:
  - mvn clean - Deletes target/ folder
  - mvn compile - Compiles src/main/java → target/classes
  - mvn test - Runs unit tests
  - mvn package - Creates target/order-service-1.0.0-SNAPSHOT.jar
  - mvn spring-boot:run - Runs the application (Spring Boot plugin goal)
  - mvn install - Installs JAR to local Maven repo (~/.m2/repository)

