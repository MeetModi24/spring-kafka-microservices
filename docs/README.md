# Documentation Structure

This directory contains comprehensive learning materials organized by topic.

## 📂 Directory Structure

```
docs/
├── 01-fundamentals/          # Java, Maven, Environment Setup
├── 02-spring-boot/           # Spring Boot concepts, annotations, patterns
├── 03-kafka/                 # Kafka deep dive, patterns, best practices
├── 04-design-patterns/       # GoF patterns, architectural patterns
├── 05-architecture/          # HLD, LLD, microservices architecture
└── LEARNING-GUIDE.md         # Main learning path and task guide
```

## 📚 What Goes Where

### 01-fundamentals/
- Java basics needed for Spring Boot
- Maven deep dive (POM structure, lifecycle, commands)
- Environment setup guides
- IDE configuration
- Git workflow

### 02-spring-boot/
- Spring Framework vs Spring Boot
- Dependency Injection (IoC container)
- Annotations reference (@SpringBootApplication, @Service, @RestController, etc.)
- Configuration (application.yml, profiles)
- Layered architecture (Controller → Service → Repository)
- DTO pattern
- Validation
- Error handling
- Testing

### 03-kafka/
- Kafka architecture (brokers, topics, partitions)
- Producers and consumers
- Consumer groups
- Message serialization
- Kafka Streams
- Idempotency patterns
- Exactly-once semantics
- Dead letter topics
- Best practices

### 04-design-patterns/
- Creational patterns (Factory, Builder, Singleton)
- Structural patterns (Adapter, Decorator, Proxy)
- Behavioral patterns (Strategy, Observer, Command)
- Spring-specific patterns
- When to use each pattern

### 05-architecture/
- Microservices principles
- Event-driven architecture
- SAGA pattern
- CQRS
- API Gateway
- Service discovery
- Circuit breaker
- Distributed tracing
- High-level design examples
- Low-level design examples

## 🎯 How to Use This Documentation

1. **Sequential Learning**: Start with 01-fundamentals, progress through each directory
2. **Reference**: Jump to specific topics when needed
3. **Code Examples**: Each concept includes working code snippets
4. **Deep Dives**: Detailed explanations of "why" behind patterns

## 📖 Main Documents

- **LEARNING-GUIDE.md** - Your weekly learning path with hands-on tasks
- **tasks/** - Step-by-step implementation tasks
- **Fundamentals.md** (root) - Quick reference notes

## ✅ Completion Tracking

Each directory has a `README.md` with:
- [ ] Topics covered
- [ ] Prerequisites
- [ ] Learning objectives
- [ ] Practical exercises
- [ ] Review questions
