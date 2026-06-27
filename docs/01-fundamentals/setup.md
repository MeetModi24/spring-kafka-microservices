# Environment Setup Guide

## Prerequisites

- macOS (Darwin)
- Homebrew installed
- Basic terminal knowledge

## Step 1: Java Installation

### What is Java?
Java is both a programming language and a runtime platform (JVM - Java Virtual Machine). Spring Boot applications compile to Java bytecode and run on the JVM.

### Installation

```bash
# Install Java via Homebrew
brew install maven  # This automatically installs OpenJDK as a dependency
```

### Verification

```bash
java --version
# Expected: openjdk 26.0.1 or later

mvn -version
# Expected: Apache Maven 3.9.16 or later
```

### Configuration

Java PATH and JAVA_HOME are automatically configured in `~/.zshrc` by the setup:

```bash
export PATH="/opt/homebrew/opt/openjdk/bin:$PATH"
export JAVA_HOME="/opt/homebrew/opt/openjdk/libexec/openjdk.jdk/Contents/Home"
```

Reload shell config:
```bash
source ~/.zshrc
```

---

## Step 2: Maven Installation

Maven is installed automatically with Java (see Step 1).

### What is Maven?
Maven is a build automation and dependency management tool. Think of it as:
- `npm` for Node.js
- `pip` + build tools for Python
- `cargo` for Rust

It handles:
- Downloading dependencies (JAR files)
- Compiling Java code
- Running tests
- Packaging applications

---

## Step 3: Docker & Kafka Setup

### Docker Installation

Docker should already be installed on Salesforce Macs.

```bash
docker --version
# Expected: Docker version 29.x or later

docker compose version
# Expected: Docker Compose version v5.x or later
```

### Kafka Environment

Navigate to project directory:
```bash
cd ~/spring-kafka-microservices
```

Start Kafka infrastructure:
```bash
docker compose up -d
```

This starts:
- **Zookeeper** (port 2181) - Kafka coordinator
- **Kafka Broker** (port 9092) - Message broker
- **Kafka UI** (port 8080) - Web interface

### Verification

```bash
# Check containers are running
docker compose ps

# Expected output:
# NAME        STATUS
# kafka       Up
# kafka-ui    Up
# zookeeper   Up
```

Access Kafka UI: http://localhost:8080

### Stop Kafka

```bash
docker compose down
```

---

## Step 4: VS Code Setup

### Required Extensions

Install these extensions in VS Code (`Cmd+Shift+X`):

| Extension ID | Purpose |
|--------------|---------|
| `vscjava.vscode-java-pack` | Java development (IntelliSense, debugging) |
| `vmware.vscode-boot-dev-pack` | Spring Boot support (config, dashboard) |
| `redhat.vscode-yaml` | YAML syntax validation |
| `ms-azuretools.vscode-docker` | Docker container management |
| `jeppeandersen.vscode-kafka` | Kafka topic browser |
| `humao.rest-client` | Test REST APIs |

### Post-Installation

1. Reload VS Code: `Cmd+Shift+P` → `Developer: Reload Window`
2. Open any `.java` file to trigger Java language server initialization (takes 1-2 min first time)

---

## Step 5: Project Structure

```bash
cd ~/spring-kafka-microservices
```

Directory structure:
```
spring-kafka-microservices/
├── docker-compose.yml       # Kafka infrastructure
├── docs/                    # Documentation (this file)
├── tasks/                   # Implementation tasks
├── order-service/           # First microservice
├── payment-service/         # Second microservice (future)
└── stock-service/           # Third microservice (future)
```

---

## Troubleshooting

### Java command not found after installation

```bash
# Check if Java is installed
ls /opt/homebrew/opt/openjdk/bin/java

# If exists, reload shell config
source ~/.zshrc

# Or open new terminal window
```

### Maven dependencies not downloading

```bash
# Check internet connection
# Try clearing Maven cache
rm -rf ~/.m2/repository

# Re-download dependencies
cd order-service
mvn clean install
```

### Kafka containers won't start

```bash
# Check Docker is running
docker ps

# Check for port conflicts
lsof -i :9092  # Kafka port
lsof -i :8080  # Kafka UI port

# Restart Docker Desktop if needed
# Then retry: docker compose up -d
```

### VS Code Java extension not working

```bash
# Check Java is accessible
java -version

# Reload VS Code window
# Cmd+Shift+P → Developer: Reload Window

# Check Java extension status
# Lower right corner should show Java version
```

---

## Next Steps

Once setup is complete:
1. Read [Maven Deep Dive](maven-deep-dive.md)
2. Start [Spring Boot Fundamentals](../02-spring-boot/README.md)
3. Begin [Task 1: Create First Service](../../tasks/01-create-order-service.md)
