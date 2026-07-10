# Documentation Index

Complete guide to all documentation files in this project.

---

## 📚 Quick Navigation

### Getting Started
1. **[README.md](README.md)** - Start here! Quick start guide and overview
2. **[TESTING-GUIDE.md](TESTING-GUIDE.md)** - Step-by-step testing instructions

### Understanding the System
3. **[ARCHITECTURE.md](ARCHITECTURE.md)** - Complete architecture, flow diagrams, and design decisions
4. **[KAFKA-STREAMS-IMPLEMENTATION.md](KAFKA-STREAMS-IMPLEMENTATION.md)** - Deep dive into Kafka Streams orchestration
5. **[CURRENT-STATUS.md](CURRENT-STATUS.md)** - Current system state and verified functionality

---

## Document Descriptions

### [README.md](README.md)
**Purpose:** Quick start guide  
**Read Time:** 2 minutes  
**Contains:**
- Quick start commands
- Architecture diagram
- Service overview
- Links to detailed docs

**Start here if:** You want to get the system running quickly

---

### [TESTING-GUIDE.md](TESTING-GUIDE.md)
**Purpose:** Comprehensive testing instructions  
**Read Time:** 15 minutes  
**Contains:**
- 6 test scenarios with expected outputs
- How to verify Kafka topics
- How to monitor Kafka Streams
- Troubleshooting guide
- Complete test script

**Read this if:** You want to test the system thoroughly

**Quick Test:**
```bash
# Create order
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-001",
    "items": [{"productId": "PROD-001", "productName": "Laptop", "quantity": 2, "price": 1000.00}]
  }'

# Wait 5 seconds, check status (should be ROLLBACK)
```

---

### [ARCHITECTURE.md](ARCHITECTURE.md)
**Purpose:** Complete system architecture  
**Read Time:** 20 minutes  
**Contains:**
- Step-by-step request flow
- Kafka topics table
- SAGA decision matrix
- File structure
- Key implementation details
- Testing scenarios
- Architecture diagrams

**Read this if:** You want to understand how the system works internally

**Key Sections:**
- **Step 4:** Kafka Streams join logic (most important)
- **SAGA Decision Matrix:** When to CONFIRM/REJECT/ROLLBACK
- **Kafka Topics:** All 4 topics explained

---

### [KAFKA-STREAMS-IMPLEMENTATION.md](KAFKA-STREAMS-IMPLEMENTATION.md)
**Purpose:** Kafka Streams technical deep dive  
**Read Time:** 25 minutes  
**Contains:**
- Kafka Streams configuration
- KStream-KStream join explanation
- Time windowing details
- Exactly-once semantics
- State management (RocksDB)
- Performance characteristics
- Troubleshooting guide
- Comparison: Manual join vs Kafka Streams

**Read this if:** You want to understand Kafka Streams in detail

**Key Concepts:**
- **Join Window:** 10-second time window
- **Exactly-once:** No duplicate decisions
- **State Store:** RocksDB for buffering events
- **Fault Tolerance:** Changelog topics backup state

---

### [CURRENT-STATUS.md](CURRENT-STATUS.md)
**Purpose:** System status snapshot  
**Read Time:** 5 minutes  
**Contains:**
- Current configuration
- Tested scenarios
- Fixed issues
- Known limitations
- Quick commands

**Read this if:** You want to know what's working right now

**Quick Status:**
```bash
# Check all services
curl -s http://localhost:8081/actuator/health | jq .status
curl -s http://localhost:8082/actuator/health | jq .status
curl -s http://localhost:8083/actuator/health | jq .status

# All should return: "UP"
```

---

## Learning Path

### For First-Time Users

1. **[README.md](README.md)** - Get system running (5 min)
2. **[TESTING-GUIDE.md](TESTING-GUIDE.md)** - Test Scenario 1 (5 min)
3. **[ARCHITECTURE.md](ARCHITECTURE.md)** - Read "Complete Request Flow" section (10 min)
4. **Experiment!** - Create more orders, check logs

### For Developers

1. **[README.md](README.md)** - Quick overview
2. **[ARCHITECTURE.md](ARCHITECTURE.md)** - Full read (20 min)
3. **[KAFKA-STREAMS-IMPLEMENTATION.md](KAFKA-STREAMS-IMPLEMENTATION.md)** - Full read (25 min)
4. **Code Review** - Read OrderStreamProcessor.java
5. **[TESTING-GUIDE.md](TESTING-GUIDE.md)** - Run all scenarios

### For Troubleshooting

1. **[CURRENT-STATUS.md](CURRENT-STATUS.md)** - Check what should be working
2. **[TESTING-GUIDE.md](TESTING-GUIDE.md)** - Troubleshooting section
3. **[KAFKA-STREAMS-IMPLEMENTATION.md](KAFKA-STREAMS-IMPLEMENTATION.md)** - Troubleshooting section
4. Check logs: `tail -f logs/order-service.log`

---

## Quick Reference

### Start System
```bash
./start.sh
```

### Test System
```bash
# Quick test
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"CUST-001","items":[{"productId":"PROD-001","productName":"Test","quantity":1,"price":100}]}'

# Full test suite
./test-saga.sh  # See TESTING-GUIDE.md for script
```

### Check Status
```bash
# Services
curl http://localhost:8081/actuator/health

# Kafka Streams
grep "State transition.*RUNNING" logs/order-service.log

# Topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

### View Logs
```bash
tail -f logs/order-service.log     # Kafka Streams
tail -f logs/payment-service.log
tail -f logs/stock-service.log
```

### Kafka UI
```
http://localhost:8080
```

---

## File Sizes

| File | Lines | Size | Read Time |
|------|-------|------|-----------|
| README.md | 91 | 3KB | 2 min |
| TESTING-GUIDE.md | 460 | 15KB | 15 min |
| ARCHITECTURE.md | 473 | 20KB | 20 min |
| KAFKA-STREAMS-IMPLEMENTATION.md | 650 | 25KB | 25 min |
| CURRENT-STATUS.md | 350 | 12KB | 5 min |

**Total Reading Time:** ~67 minutes for complete understanding

---

## Additional Files

- **docker-compose.yml** - Kafka + Zookeeper setup
- **start.sh** - Service startup script
- **test-orders.sh** - Automated test suite (if exists)
- **pom.xml** - Maven dependencies (per service)
- **application.yml** - Configuration (per service)

---

## Support

If you're stuck:

1. Check **[CURRENT-STATUS.md](CURRENT-STATUS.md)** - Is everything working as expected?
2. Read **[TESTING-GUIDE.md](TESTING-GUIDE.md)** Troubleshooting section
3. Check logs: `tail -f logs/*.log`
4. View Kafka UI: http://localhost:8080

---

## Summary

- 5 comprehensive documentation files
- Step-by-step testing guide
- Complete architecture explanation
- Kafka Streams deep dive
- Current system status
- ~60 minutes total reading time

**Start with README.md, then explore based on your needs!**
