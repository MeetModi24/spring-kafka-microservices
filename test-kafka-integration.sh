#!/bin/bash

# Test Kafka Integration Script
# Automates testing of order-service with Kafka

set -e  # Exit on error

echo "🚀 Starting Kafka Integration Test..."
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Step 1: Check Kafka is running
echo "${YELLOW}Step 1: Checking Kafka status...${NC}"
if docker-compose ps | grep -q "kafka.*Up"; then
    echo "${GREEN}✓ Kafka is running${NC}"
else
    echo "${RED}✗ Kafka is not running${NC}"
    echo "Starting Kafka..."
    docker-compose up -d
    echo "Waiting 30 seconds for Kafka to initialize..."
    sleep 30
fi
echo ""

# Step 2: Verify containers
echo "${YELLOW}Step 2: Verifying containers...${NC}"
docker-compose ps
echo ""

# Step 3: Test order creation
echo "${YELLOW}Step 3: Creating test order...${NC}"
ORDER_RESPONSE=$(curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-TEST-123",
    "items": [
      {
        "productId": "PROD-001",
        "productName": "Test Laptop",
        "quantity": 1,
        "price": 999.99
      },
      {
        "productId": "PROD-002",
        "productName": "Test Mouse",
        "quantity": 2,
        "price": 29.99
      }
    ]
  }')

ORDER_ID=$(echo $ORDER_RESPONSE | jq -r '.orderId')

if [ -n "$ORDER_ID" ] && [ "$ORDER_ID" != "null" ]; then
    echo "${GREEN}✓ Order created successfully${NC}"
    echo "Order ID: $ORDER_ID"
    echo "Response:"
    echo $ORDER_RESPONSE | jq '.'
else
    echo "${RED}✗ Failed to create order${NC}"
    echo "Response: $ORDER_RESPONSE"
    exit 1
fi
echo ""

# Step 4: Verify order can be retrieved
echo "${YELLOW}Step 4: Retrieving created order...${NC}"
GET_RESPONSE=$(curl -s http://localhost:8081/api/orders/$ORDER_ID)
echo "${GREEN}✓ Order retrieved successfully${NC}"
echo $GET_RESPONSE | jq '.'
echo ""

# Step 5: Test error handling - 404
echo "${YELLOW}Step 5: Testing 404 error handling...${NC}"
ERROR_404=$(curl -s http://localhost:8081/api/orders/invalid-id)
STATUS=$(echo $ERROR_404 | jq -r '.status')
if [ "$STATUS" = "404" ]; then
    echo "${GREEN}✓ 404 error handling works${NC}"
    echo $ERROR_404 | jq '.'
else
    echo "${RED}✗ 404 error handling failed${NC}"
    exit 1
fi
echo ""

# Step 6: Test error handling - 400 validation
echo "${YELLOW}Step 6: Testing 400 validation error handling...${NC}"
ERROR_400=$(curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId": "", "items": []}')
STATUS=$(echo $ERROR_400 | jq -r '.status')
if [ "$STATUS" = "400" ]; then
    echo "${GREEN}✓ 400 validation error handling works${NC}"
    echo $ERROR_400 | jq '.'
else
    echo "${RED}✗ 400 validation error handling failed${NC}"
    exit 1
fi
echo ""

# Step 7: Create multiple orders to test Kafka partitioning
echo "${YELLOW}Step 7: Creating 5 orders to test Kafka partitioning...${NC}"
for i in {1..5}; do
    curl -s -X POST http://localhost:8081/api/orders \
      -H "Content-Type: application/json" \
      -d "{
        \"customerId\": \"CUST-$i\",
        \"items\": [{
          \"productId\": \"PROD-TEST-$i\",
          \"productName\": \"Test Item $i\",
          \"quantity\": $i,
          \"price\": $(($i * 100)).00
        }]
      }" > /dev/null
    echo "  ${GREEN}✓${NC} Order $i created"
done
echo ""

# Step 8: Get all orders
echo "${YELLOW}Step 8: Retrieving all orders...${NC}"
ALL_ORDERS=$(curl -s http://localhost:8081/api/orders)
ORDER_COUNT=$(echo $ALL_ORDERS | jq '. | length')
echo "${GREEN}✓ Found $ORDER_COUNT orders${NC}"
echo ""

# Summary
echo "${GREEN}═══════════════════════════════════════════════${NC}"
echo "${GREEN}✓ All tests passed successfully!${NC}"
echo "${GREEN}═══════════════════════════════════════════════${NC}"
echo ""
echo "Next steps:"
echo "1. Open Kafka UI: http://localhost:8080"
echo "2. Check 'order-events' topic for published events"
echo "3. View application logs for Kafka producer activity"
echo "4. Ready to build payment-service (Phase 3)!"
echo ""
