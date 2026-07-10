# Testing Guide - Spring Kafka Microservices

Complete guide to test the Kafka Streams SAGA orchestration system.

---

## Prerequisites

Ensure all services are running:

```bash
# Check service health
curl http://localhost:8081/actuator/health  # order-service
curl http://localhost:8082/actuator/health  # payment-service  
curl http://localhost:8083/actuator/health  # stock-service

# All should return: {"status":"UP"}
```

---

## Test Scenario 1: ROLLBACK (Payment Fails, Stock Succeeds)

**Expected:** Order status = ROLLBACK, stock gets released

```bash
# Create order
ORDER_ID=$(curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-001",
    "items": [
      {"productId": "PROD-001", "productName": "Laptop", "quantity": 2, "price": 1000.00}
    ]
  }' | jq -r '.orderId')

echo "Order created: $ORDER_ID"

# Wait for Kafka Streams to process (10-second window)
sleep 5

# Check final status
curl -s "http://localhost:8081/api/orders/$ORDER_ID" | jq '{orderId, status, totalAmount}'
```

**Expected Output:**
```json
{
  "orderId": "xxx-xxx-xxx",
  "status": "ROLLBACK",
  "totalAmount": 2000.00
}
```

**Why ROLLBACK?**
- Customer "CUST-001" doesn't exist → Payment **REJECTS**
- Product "PROD-001" exists with stock → Stock **ACCEPTS**
- Result: ROLLBACK (payment failed, need to release reserved stock)

---

## Test Scenario 2: Verify Kafka Topics

Check that all 4 topics exist and contain messages:

```bash
# List all topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 | grep -v "^__"
```

**Expected Topics:**
```
order-created
order-events
payment-events
stock-events
```

### Check topic contents:

```bash
# 1. Check order-created topic
echo "=== ORDER-CREATED ===="
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic order-created \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 3000 2>&1 | grep -v "Processed"

# 2. Check payment-events topic  
echo -e "\n=== PAYMENT-EVENTS ===="
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic payment-events \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 3000 2>&1 | grep -v "Processed"

# 3. Check stock-events topic
echo -e "\n=== STOCK-EVENTS ===="
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic stock-events \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 3000 2>&1 | grep -v "Processed"

# 4. Check order-events (final decisions)
echo -e "\n=== ORDER-EVENTS ===="
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic order-events \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 3000 2>&1 | grep -v "Processed"
```

---

## Test Scenario 3: Check Kafka Streams Processing

Monitor Kafka Streams logs to see the join happening:

```bash
# Watch Kafka Streams join in real-time
tail -f logs/order-service.log | grep -E "Final decision|State transition"
```

**Then create an order in another terminal:**
```bash
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-001",
    "items": [{"productId": "PROD-001", "productName": "Laptop", "quantity": 1, "price": 1000.00}]
  }'
```

**Expected log output:**
```
Final decision made: orderId=xxx, status=ROLLBACK, reason=Payment service rejected...
```

---

## Test Scenario 4: Service Logs Analysis

Check each service's activity:

### Order Service (Kafka Streams orchestrator)
```bash
tail -50 logs/order-service.log | grep -E "Publishing|Final decision|State transition"
```

**What to look for:**
- "Publishing OrderCreatedEvent" → Order published to order-created
- "Final decision made" → Kafka Streams made SAGA decision
- "State transition.*RUNNING" → Kafka Streams is healthy

### Payment Service
```bash
tail -50 logs/payment-service.log | grep -E "Received|Published|Processing"
```

**What to look for:**
- "Received OrderCreatedEvent" → Got order from order-created
- "Processing payment for order" → Processing
- "Published PaymentProcessedEvent: REJECT" → Sent result to payment-events

### Stock Service
```bash
tail -50 logs/stock-service.log | grep -E "Received|Published|Processing"
```

**What to look for:**
- "Received OrderCreatedEvent" → Got order from order-created
- "Processing stock for order" → Processing
- "Published StockProcessedEvent: ACCEPT" → Sent result to stock-events

---

## Test Scenario 5: Multi-Item Order

Test with multiple products:

```bash
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-001",
    "items": [
      {"productId": "PROD-001", "productName": "Laptop", "quantity": 1, "price": 1000.00},
      {"productId": "PROD-002", "productName": "Mouse", "quantity": 5, "price": 25.00}
    ]
  }' | jq
```

**Expected:** Still ROLLBACK because customer doesn't exist (payment fails)

---

## Test Scenario 6: Check Kafka UI

Open Kafka UI in browser to visualize topics:

```
http://localhost:8080
```

**What to explore:**
- **Topics tab** → See all 4 topics
- **Messages tab** → View actual event payloads
- **Consumers tab** → See consumer groups (payment-service-group, stock-service-group, etc.)

---

## Troubleshooting

### Services not starting?

```bash
# Check if ports are in use
lsof -ti:8081,8082,8083

# Check Docker containers
docker ps

# Restart everything
./start.sh
```

### Kafka Streams not working?

```bash
# Check Kafka Streams state
grep "State transition" logs/order-service.log | tail -5

# Should see: State transition from REBALANCING to RUNNING
```

### Messages not appearing in topics?

```bash
# Verify topic creation
docker exec kafka kafka-topics --describe --topic payment-events --bootstrap-server localhost:9092

# Should show: Leader: 1, Replicas: 1, Isr: 1
```

### Order stuck in PENDING?

**Possible causes:**
1. Kafka Streams not running → Check logs
2. Payment/Stock not publishing → Check service logs
3. 10-second window expired → Events arrived > 10s apart

```bash
# Check when events arrived
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic payment-events \
  --from-beginning \
  --property print.timestamp=true \
  --max-messages 1 \
  --timeout-ms 3000
```

---

## Complete Test Script

Run all tests automatically:

```bash
#!/bin/bash

echo "=== COMPREHENSIVE SAGA TEST ==="
echo

# Test 1: Create order
echo "1. Creating test order..."
ORDER_ID=$(curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-001",
    "items": [{"productId": "PROD-001", "productName": "Laptop", "quantity": 2, "price": 1000.00}]
  }' | jq -r '.orderId')

echo "   Order ID: $ORDER_ID"
echo

# Test 2: Wait for processing
echo "2. Waiting for Kafka Streams to process (5 seconds)..."
sleep 5
echo

# Test 3: Check final status
echo "3. Checking order status..."
STATUS=$(curl -s "http://localhost:8081/api/orders/$ORDER_ID" | jq -r '.status')
echo "   Status: $STATUS"
echo

# Test 4: Verify topics
echo "4. Verifying Kafka topics exist..."
TOPICS=$(docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 2>&1 | grep -v "^__" | wc -l)
echo "   Found $TOPICS topics"
echo

# Test 5: Check Kafka Streams state
echo "5. Checking Kafka Streams state..."
STREAMS_STATE=$(grep "State transition.*RUNNING" logs/order-service.log | tail -1)
if [ -n "$STREAMS_STATE" ]; then
  echo "   ✅ Kafka Streams is RUNNING"
else
  echo "   ❌ Kafka Streams not running"
fi
echo

# Summary
echo "=== TEST SUMMARY ==="
if [ "$STATUS" == "ROLLBACK" ]; then
  echo "✅ SAGA orchestration working correctly!"
  echo "   - Payment service REJECTED (customer not found)"
  echo "   - Stock service ACCEPTED (product available)"
  echo "   - Kafka Streams made ROLLBACK decision"
  echo "   - Order status updated to ROLLBACK"
else
  echo "❌ Expected status: ROLLBACK, Got: $STATUS"
fi
```

Save this as `test-saga.sh` and run:

```bash
chmod +x test-saga.sh
./test-saga.sh
```

---

## Understanding the Results

### Status: ROLLBACK
- **Meaning:** One service succeeded, one failed
- **Action:** Compensation transaction executed
- **Example:** Payment failed → Release reserved stock

### Status: CONFIRMED  
- **Meaning:** Both services succeeded
- **Action:** Commit all changes
- **Example:** Payment + Stock both OK → Complete order

### Status: REJECTED
- **Meaning:** Both services failed
- **Action:** Nothing to rollback
- **Example:** Payment failed + Stock unavailable → Cancel order

### Status: PENDING
- **Meaning:** Still waiting for decision OR Kafka Streams failed
- **Action:** Check logs, ensure Kafka Streams is running

---

## Performance Testing

Test the 10-second join window:

```bash
# Create order
ORDER_ID=$(curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"CUST-001","items":[{"productId":"PROD-001","productName":"Test","quantity":1,"price":100}]}' \
  | jq -r '.orderId')

# Check immediately (should be PENDING)
echo "T+0s: $(curl -s http://localhost:8081/api/orders/$ORDER_ID | jq -r .status)"
sleep 2
echo "T+2s: $(curl -s http://localhost:8081/api/orders/$ORDER_ID | jq -r .status)"
sleep 3
echo "T+5s: $(curl -s http://localhost:8081/api/orders/$ORDER_ID | jq -r .status)"
```

**Expected:** Status changes from PENDING → ROLLBACK within 5 seconds

---

## Next Steps

After testing:

1. **Monitor in production:** Use Kafka UI to monitor topic lag
2. **Add custom test data:** Create customers and products in H2 databases
3. **Load testing:** Use Apache JMeter or k6 for concurrent orders
4. **Distributed tracing:** Add Sleuth/Zipkin for request tracing
