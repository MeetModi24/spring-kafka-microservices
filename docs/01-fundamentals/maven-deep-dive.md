# Maven Deep Dive

## What is Maven?

Maven is a **build automation tool** that manages:
1. **Dependencies** (downloading JARs you need)
2. **Building** (compiling `.java` → `.class` files)
3. **Testing** (running unit tests)
4. **Packaging** (creating executable `.jar` files)

---

## Maven Coordinates (GAV)

Every Maven artifact has 3 coordinates that uniquely identify it:

```xml
<groupId>com.example</groupId>        <!-- Organization/company -->
<artifactId>order-service</artifactId> <!-- Project name -->
<version>1.0.0-SNAPSHOT</version>      <!-- Version -->
```

Combined, they create: `com.example:order-service:1.0.0-SNAPSHOT`

### Version Conventions

- `1.0.0` - Released version (stable)
- `1.0.0-SNAPSHOT` - In development (changes frequently)
- `1.0.1` - Patch release (bug fixes)
- `1.1.0` - Minor release (new features, backward compatible)
- `2.0.0` - Major release (breaking changes)

---

## Parent POM: Inheritance

```xml
<parent>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-parent</artifactId>
    <version>3.5.16</version>
</parent>
```

### What the Parent Provides

1. **Dependency Management**
   - Versions for all Spring dependencies
   - You don't specify versions for Spring libraries

2. **Plugin Configuration**
   - Maven compiler configured for Java 17
   - Surefire plugin for testing
   - Spring Boot plugin for packaging

3. **Resource Filtering**
   - Replaces `@project.version@` in files
   - Useful for application.yml properties

### Example: No Version Needed

```xml
<!-- Because parent manages versions -->
<dependency>
    <artifactId>spring-boot-starter-web</artifactId>
    <!-- No <version> tag needed -->
</dependency>
```

---

## Dependency Scopes

Scopes control when dependencies are available:

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-test</artifactId>
    <scope>test</scope>  <!-- Only available in tests -->
</dependency>
```

| Scope | Available When | Packaged in JAR? |
|-------|----------------|------------------|
| `compile` (default) | Compilation + runtime + test | Yes |
| `test` | Test compilation + test runtime | No |
| `provided` | Compilation + test (not runtime) | No |
| `runtime` | Runtime + test (not compilation) | Yes |

### When to Use Each Scope

**compile** (default):
```xml
<dependency>
    <artifactId>spring-boot-starter-web</artifactId>
    <!-- Used everywhere, included in JAR -->
</dependency>
```

**test**:
```xml
<dependency>
    <artifactId>spring-boot-starter-test</artifactId>
    <scope>test</scope>
    <!-- Only for unit tests, not in production JAR -->
</dependency>
```

**provided**:
```xml
<dependency>
    <groupId>javax.servlet</groupId>
    <artifactId>javax.servlet-api</artifactId>
    <scope>provided</scope>
    <!-- Tomcat provides this at runtime -->
</dependency>
```

**runtime**:
```xml
<dependency>
    <groupId>com.h2database</groupId>
    <artifactId>h2</artifactId>
    <scope>runtime</scope>
    <!-- Needed at runtime, not compilation -->
</dependency>
```

---

## Spring Boot Starters

Starters are **opinionated dependency bundles**.

### Without Starters (tedious):

```xml
<dependency><artifactId>spring-web</artifactId></dependency>
<dependency><artifactId>spring-webmvc</artifactId></dependency>
<dependency><artifactId>jackson-databind</artifactId></dependency>
<dependency><artifactId>tomcat-embed-core</artifactId></dependency>
<dependency><artifactId>tomcat-embed-el</artifactId></dependency>
<dependency><artifactId>tomcat-embed-websocket</artifactId></dependency>
<!-- ... 10 more dependencies -->
```

### With Starters (simple):

```xml
<dependency>
    <artifactId>spring-boot-starter-web</artifactId>
</dependency>
```

### Common Starters

| Starter | Includes |
|---------|----------|
| `spring-boot-starter-web` | Tomcat, Spring MVC, Jackson, Validation |
| `spring-boot-starter-data-jpa` | Hibernate, JPA, Transaction management |
| `spring-boot-starter-test` | JUnit, Mockito, AssertJ, Spring Test |
| `spring-kafka` | Kafka clients, Spring Kafka |
| `spring-boot-starter-actuator` | Health checks, metrics |
| `spring-boot-starter-validation` | Bean Validation (JSR-380) |

---

## Maven Build Lifecycle

Maven executes commands in phases:

```
clean → validate → compile → test → package → install → deploy
```

### Phase Descriptions

| Phase | What It Does |
|-------|--------------|
| `clean` | Deletes `target/` directory |
| `validate` | Validates project structure |
| `compile` | Compiles `src/main/java` → `target/classes` |
| `test` | Runs unit tests in `src/test/java` |
| `package` | Creates JAR/WAR in `target/` |
| `install` | Installs JAR to local repo (`~/.m2/repository`) |
| `deploy` | Uploads JAR to remote repository (Nexus, Artifactory) |

### Common Commands

```bash
# Clean build artifacts
mvn clean

# Compile main code
mvn compile

# Run tests
mvn test

# Create JAR (runs compile → test → package)
mvn package

# Install to local Maven repo
mvn install

# Combined: clean then package
mvn clean package

# Run Spring Boot application
mvn spring-boot:run

# Skip tests during package
mvn package -DskipTests

# Run specific test class
mvn test -Dtest=OrderServiceTest

# Verbose output
mvn -X package
```

---

## POM Structure Explained

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
    <modelVersion>4.0.0</modelVersion>

    <!-- PROJECT IDENTITY -->
    <groupId>com.example</groupId>
    <artifactId>order-service</artifactId>
    <version>1.0.0-SNAPSHOT</version>
    <packaging>jar</packaging>

    <!-- PARENT (inheritance) -->
    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>3.5.16</version>
    </parent>

    <!-- PROPERTIES (variables) -->
    <properties>
        <java.version>17</java.version>
        <spring-kafka.version>3.1.0</spring-kafka.version>
    </properties>

    <!-- DEPENDENCIES (libraries) -->
    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
    </dependencies>

    <!-- BUILD (plugins) -->
    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>
```

---

## Maven Repository

Maven downloads dependencies from repositories:

### Repository Hierarchy

```
1. Local (~/.m2/repository)
   ↓ (if not found)
2. Maven Central (repo.maven.apache.org)
   ↓ (if not found)
3. Custom repos (Nexus, Artifactory)
```

### Local Repository

```bash
# Location
~/.m2/repository/

# Structure
~/.m2/repository/
└── org/
    └── springframework/
        └── boot/
            └── spring-boot-starter-web/
                └── 3.5.16/
                    └── spring-boot-starter-web-3.5.16.jar
```

### Clearing Cache

```bash
# Delete entire local repo (will re-download everything)
rm -rf ~/.m2/repository

# Delete specific artifact
rm -rf ~/.m2/repository/org/springframework/boot
```

---

## Spring Boot Maven Plugin

```xml
<plugin>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-maven-plugin</artifactId>
</plugin>
```

### What It Does

1. **Creates executable JAR**
   - Includes all dependencies
   - Includes embedded Tomcat
   - Single file deployment

2. **Repackages application**
   - Original: `order-service-1.0.0-SNAPSHOT.jar` (app code only)
   - Repackaged: `order-service-1.0.0-SNAPSHOT.jar` (app + dependencies + Tomcat)

3. **Provides goals**
   - `mvn spring-boot:run` - Run application
   - `mvn spring-boot:build-image` - Create Docker image

### Running the Application

```bash
# During development
mvn spring-boot:run

# Production (after mvn package)
java -jar target/order-service-1.0.0-SNAPSHOT.jar
```

---

## Dependency Tree

See all dependencies (including transitive):

```bash
mvn dependency:tree
```

Example output:
```
com.example:order-service:1.0.0-SNAPSHOT
├── org.springframework.boot:spring-boot-starter-web:3.5.16
│   ├── org.springframework.boot:spring-boot-starter:3.5.16
│   │   ├── org.springframework.boot:spring-boot:3.5.16
│   │   └── org.springframework:spring-core:6.2.10
│   ├── org.springframework:spring-web:6.2.10
│   └── com.fasterxml.jackson.core:jackson-databind:2.18.2
└── org.springframework.kafka:spring-kafka:3.1.0
    └── org.apache.kafka:kafka-clients:3.8.0
```

### Resolving Dependency Conflicts

```bash
# Find why a dependency is included
mvn dependency:tree -Dincludes=com.fasterxml.jackson.core:jackson-databind

# Exclude transitive dependency
<dependency>
    <artifactId>spring-boot-starter-web</artifactId>
    <exclusions>
        <exclusion>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
        </exclusion>
    </exclusions>
</dependency>
```

---

## Multi-Module Projects

For multiple services in one repo:

```
spring-kafka-microservices/
├── pom.xml (parent)
├── order-service/
│   └── pom.xml (child)
├── payment-service/
│   └── pom.xml (child)
└── stock-service/
    └── pom.xml (child)
```

Parent `pom.xml`:
```xml
<packaging>pom</packaging>
<modules>
    <module>order-service</module>
    <module>payment-service</module>
    <module>stock-service</module>
</modules>
```

Child `pom.xml`:
```xml
<parent>
    <groupId>com.example</groupId>
    <artifactId>spring-kafka-microservices</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</parent>
```

Build all modules:
```bash
mvn clean install
```

---

## Best Practices

1. **Use Spring Boot parent POM**
   - Manages versions automatically
   - Reduces conflicts

2. **Don't specify versions for Spring dependencies**
   - Parent POM handles this
   - Only specify for non-Spring libraries

3. **Use starters over individual dependencies**
   - Less boilerplate
   - Tested combinations

4. **Keep dependencies up to date**
   - Security patches
   - Bug fixes

5. **Use appropriate scopes**
   - `test` for test-only dependencies
   - Keeps production JAR small

6. **Clean regularly**
   - `mvn clean` before important builds
   - Prevents stale artifacts

---

## Next Steps

- [Spring Boot Fundamentals](../02-spring-boot/README.md)
- [Project Structure](../02-spring-boot/project-structure.md)
- [Dependency Injection](../02-spring-boot/dependency-injection.md)
