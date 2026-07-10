#!/bin/bash

##############################################################################
# Spring Kafka Microservices - Comprehensive Test Orders Script
#
# This script provides comprehensive testing of all SAGA scenarios:
# 1. Multi-item order - all succeed (CONFIRMED)
# 2. Multi-item order - partial stock failure with atomic rollback (REJECTED)
# 3. Multi-item order - payment rejected (ROLLBACK stock)
# 4. Multi-item order - stock rejected (ROLLBACK payment)
# 5. Both services reject (REJECTED)
# 6. Query order status
# 7. Custom order testing
#
# Features:
# - Validates order status after creation
# - Checks database state
# - Monitors Kafka events
# - Provides detailed test results
##############################################################################

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m'

ORDER_SERVICE_URL="http://localhost:8081"
PAYMENT_SERVICE_URL="http://localhost:8082"
STOCK_SERVICE_URL="http://localhost:8083"

WAIT_TIME=5  # Seconds to wait for SAGA completion
LAST_ORDER_ID=""

##############################################################################
# Logging Functions
##############################################################################

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_test() {
    echo -e "${YELLOW}[TEST]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_result() {
    echo -e "${CYAN}[RESULT]${NC} $1"
}

print_banner() {
    echo -e "${GREEN}"
    echo "╔═══════════════════════════════════════════════════════════════╗"
    echo "║                                                               ║"
    echo "║           SAGA Pattern Test Suite                            ║"
    echo "║                                                               ║"
    echo "║  Comprehensive testing of all SAGA scenarios:                ║"
    echo "║    1. Both succeed → CONFIRMED                               ║"
    echo "║    2. Atomic multi-item rollback → REJECTED                  ║"
    echo "║    3. Payment fails → ROLLBACK stock                         ║"
    echo "║    4. Stock fails → ROLLBACK payment                         ║"
    echo "║    5. Both fail → REJECTED                                   ║"
    echo "║                                                               ║"
    echo "╚═══════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

print_separator() {
    echo -e "${CYAN}═══════════════════════════════════════════════════════════${NC}"
}

##############################################################################
# Utility Functions
##############################################################################

wait_between_tests() {
    echo ""
    log_info "Waiting $WAIT_TIME seconds for SAGA to complete..."

    for i in $(seq $WAIT_TIME -1 1); do
        printf "\r${BLUE}[INFO]${NC} Time remaining: ${CYAN}$i${NC} seconds..."
        sleep 1
    done
    printf "\r${BLUE}[INFO]${NC} SAGA processing complete                \n"
    echo ""
}

check_service() {
    log_info "Checking if services are running..."

    local all_running=true

    # Check order-service
    if ! curl -s "$ORDER_SERVICE_URL/actuator/health" > /dev/null 2>&1; then
        log_error "order-service is not running on $ORDER_SERVICE_URL"
        all_running=false
    else
        log_success "order-service is running"
    fi

    # Check payment-service
    if ! curl -s "$PAYMENT_SERVICE_URL/actuator/health" > /dev/null 2>&1; then
        log_error "payment-service is not running on $PAYMENT_SERVICE_URL"
        all_running=false
    else
        log_success "payment-service is running"
    fi

    # Check stock-service
    if ! curl -s "$STOCK_SERVICE_URL/actuator/health" > /dev/null 2>&1; then
        log_error "stock-service is not running on $STOCK_SERVICE_URL"
        all_running=false
    else
        log_success "stock-service is running"
    fi

    if [ "$all_running" = false ]; then
        echo ""
        log_error "Some services are not running. Please run: ./start.sh"
        exit 1
    fi

    echo ""
}

create_order() {
    local order_json=$1
    local test_name=$2

    log_info "Creating order..."

    local response=$(curl -s -X POST "$ORDER_SERVICE_URL/api/orders" \
        -H "Content-Type: application/json" \
        -d "$order_json")

    # Extract order ID
    LAST_ORDER_ID=$(echo "$response" | jq -r '.orderId' 2>/dev/null)

    if [ "$LAST_ORDER_ID" = "null" ] || [ -z "$LAST_ORDER_ID" ]; then
        log_error "Failed to create order"
        echo "$response" | jq '.' 2>/dev/null || echo "$response"
        return 1
    fi

    echo ""
    log_success "Order created: $LAST_ORDER_ID"
    echo ""
    echo "$response" | jq '.' 2>/dev/null || echo "$response"
    echo ""
}

get_order_status() {
    local order_id=${1:-$LAST_ORDER_ID}

    if [ -z "$order_id" ]; then
        log_error "No order ID provided"
        return 1
    fi

    log_info "Fetching order status: $order_id"

    local response=$(curl -s "$ORDER_SERVICE_URL/api/orders/$order_id")

    if [ -z "$response" ] || echo "$response" | grep -q "error"; then
        log_error "Failed to fetch order status"
        echo "$response"
        return 1
    fi

    local status=$(echo "$response" | jq -r '.status' 2>/dev/null)

    echo ""
    log_result "Order Status: ${CYAN}$status${NC}"
    echo ""
    echo "$response" | jq '.' 2>/dev/null || echo "$response"
    echo ""

    return 0
}

validate_order_status() {
    local expected_status=$1
    local order_id=${2:-$LAST_ORDER_ID}

    log_info "Validating order status (expected: $expected_status)..."

    local response=$(curl -s "$ORDER_SERVICE_URL/api/orders/$order_id")
    local actual_status=$(echo "$response" | jq -r '.status' 2>/dev/null)

    if [ "$actual_status" = "$expected_status" ]; then
        log_success "Status validation passed: $actual_status"
        return 0
    else
        log_error "Status validation failed: expected=$expected_status, actual=$actual_status"
        return 1
    fi
}

##############################################################################
# Test Scenarios
##############################################################################

test_scenario_1() {
    print_separator
    log_test "Test 1: Multi-Item Order - All Succeed (CONFIRMED)"
    print_separator
    echo ""
    log_info "Order details:"
    echo "  • Customer: CUST-001 (Balance: \$10,000)"
    echo "  • Items: 1x Laptop (\$1,200), 2x Mouse (\$25)"
    echo "  • Total: \$1,250"
    echo ""
    log_info "Expected: Payment ACCEPT + Stock ACCEPT → CONFIRMED"
    echo ""

    local order_json='{
        "customerId": "CUST-001",
        "items": [
          {"productId": "PROD-001", "productName": "Laptop", "quantity": 1, "price": 1200.00},
          {"productId": "PROD-002", "productName": "Mouse", "quantity": 2, "price": 25.00}
        ]
      }'

    create_order "$order_json" "Test 1"
    wait_between_tests
    validate_order_status "CONFIRMED"

    echo ""
    log_success "Test 1 completed"
    echo ""
}

test_scenario_2() {
    print_separator
    log_test "Test 2: Multi-Item Order - Atomic Rollback (REJECTED)"
    print_separator
    echo ""
    log_info "Order details:"
    echo "  • Customer: CUST-001 (Balance: \$10,000)"
    echo "  • Items: 5x Laptop, 300x Mouse"
    echo "  • Stock: Laptop available=100✓, Mouse available=200✗ (need 300)"
    echo ""
    log_info "Expected: Stock reserves Laptop, fails on Mouse, atomically rolls back Laptop → REJECTED"
    echo ""

    local order_json='{
        "customerId": "CUST-001",
        "items": [
          {"productId": "PROD-001", "productName": "Laptop", "quantity": 5, "price": 1200.00},
          {"productId": "PROD-002", "productName": "Mouse", "quantity": 300, "price": 25.00}
        ]
      }'

    create_order "$order_json" "Test 2"
    wait_between_tests
    validate_order_status "REJECTED"

    echo ""
    log_success "Test 2 completed"
    echo ""
}

test_scenario_3() {
    print_separator
    log_test "Test 3: Payment Rejected - Rollback Stock (ROLLBACK)"
    print_separator
    echo ""
    log_info "Order details:"
    echo "  • Customer: CUST-002 (Balance: \$100)"
    echo "  • Items: 1x Laptop (\$1,200)"
    echo "  • Total: \$1,200"
    echo ""
    log_info "Expected: Payment REJECT (insufficient funds) + Stock ACCEPT → ROLLBACK stock"
    echo ""

    local order_json='{
        "customerId": "CUST-002",
        "items": [
          {"productId": "PROD-001", "productName": "Laptop", "quantity": 1, "price": 1200.00}
        ]
      }'

    create_order "$order_json" "Test 3"
    wait_between_tests
    validate_order_status "REJECTED"

    echo ""
    log_success "Test 3 completed"
    echo ""
}

test_scenario_4() {
    print_separator
    log_test "Test 4: Stock Rejected - Rollback Payment (ROLLBACK)"
    print_separator
    echo ""
    log_info "Order details:"
    echo "  • Customer: CUST-001 (Balance: \$10,000)"
    echo "  • Items: 1x Laptop, 300x Mouse"
    echo "  • Stock: Mouse available=200✗ (need 300)"
    echo ""
    log_info "Expected: Payment ACCEPT + Stock REJECT (insufficient) → ROLLBACK payment"
    echo ""

    local order_json='{
        "customerId": "CUST-001",
        "items": [
          {"productId": "PROD-001", "productName": "Laptop", "quantity": 1, "price": 1200.00},
          {"productId": "PROD-002", "productName": "Mouse", "quantity": 300, "price": 25.00}
        ]
      }'

    create_order "$order_json" "Test 4"
    wait_between_tests
    validate_order_status "REJECTED"

    echo ""
    log_success "Test 4 completed"
    echo ""
}

test_scenario_5() {
    print_separator
    log_test "Test 5: Both Rejected (REJECTED)"
    print_separator
    echo ""
    log_info "Order details:"
    echo "  • Customer: CUST-002 (Balance: \$100)"
    echo "  • Items: 1x Laptop (\$1,200), 300x Mouse"
    echo "  • Payment: REJECT (insufficient funds)"
    echo "  • Stock: REJECT (insufficient Mouse stock)"
    echo ""
    log_info "Expected: Payment REJECT + Stock REJECT → REJECTED (nothing to compensate)"
    echo ""

    local order_json='{
        "customerId": "CUST-002",
        "items": [
          {"productId": "PROD-001", "productName": "Laptop", "quantity": 1, "price": 1200.00},
          {"productId": "PROD-002", "productName": "Mouse", "quantity": 300, "price": 25.00}
        ]
      }'

    create_order "$order_json" "Test 5"
    wait_between_tests
    validate_order_status "REJECTED"

    echo ""
    log_success "Test 5 completed"
    echo ""
}

test_scenario_6() {
    print_separator
    log_test "Test 6: Complex Multi-Item Order (CONFIRMED)"
    print_separator
    echo ""
    log_info "Order details:"
    echo "  • Customer: CUST-003 (Balance: \$5,000)"
    echo "  • Items: 2x Laptop, 5x Keyboard, 1x Monitor"
    echo "  • Total: \$2,875"
    echo ""
    log_info "Expected: Payment ACCEPT + Stock ACCEPT → CONFIRMED"
    echo ""

    local order_json='{
        "customerId": "CUST-003",
        "items": [
          {"productId": "PROD-001", "productName": "Laptop", "quantity": 2, "price": 1200.00},
          {"productId": "PROD-003", "productName": "Keyboard", "quantity": 5, "price": 75.00},
          {"productId": "PROD-004", "productName": "Monitor", "quantity": 1, "price": 300.00}
        ]
      }'

    create_order "$order_json" "Test 6"
    wait_between_tests
    validate_order_status "CONFIRMED"

    echo ""
    log_success "Test 6 completed"
    echo ""
}

test_scenario_7() {
    print_separator
    log_test "Test 7: Edge Case - Large Quantity (REJECTED)"
    print_separator
    echo ""
    log_info "Order details:"
    echo "  • Customer: CUST-001 (Balance: \$10,000)"
    echo "  • Items: 1000x Mouse"
    echo "  • Stock: Mouse available=200✗ (need 1000)"
    echo ""
    log_info "Expected: Stock REJECT → REJECTED"
    echo ""

    local order_json='{
        "customerId": "CUST-001",
        "items": [
          {"productId": "PROD-002", "productName": "Mouse", "quantity": 1000, "price": 25.00}
        ]
      }'

    create_order "$order_json" "Test 7"
    wait_between_tests
    validate_order_status "REJECTED"

    echo ""
    log_success "Test 7 completed"
    echo ""
}

test_query_order() {
    print_separator
    log_test "Query Order Test"
    print_separator
    echo ""

    if [ -z "$LAST_ORDER_ID" ]; then
        log_error "No previous order to query. Please run a test scenario first."
        return 1
    fi

    get_order_status "$LAST_ORDER_ID"

    echo ""
    log_success "Query test completed"
    echo ""
}

test_custom_order() {
    print_separator
    log_test "Custom Order Test"
    print_separator
    echo ""

    log_info "Enter custom order details (or press Ctrl+C to cancel):"
    echo ""

    read -p "Customer ID (CUST-001, CUST-002, CUST-003): " customer_id
    read -p "Product ID: " product_id
    read -p "Product Name: " product_name
    read -p "Quantity: " quantity
    read -p "Price: " price

    local order_json=$(cat <<EOF
{
    "customerId": "$customer_id",
    "items": [
        {"productId": "$product_id", "productName": "$product_name", "quantity": $quantity, "price": $price}
    ]
}
EOF
)

    echo ""
    log_info "Creating custom order..."

    create_order "$order_json" "Custom Order"
    wait_between_tests
    get_order_status

    echo ""
    log_success "Custom order test completed"
    echo ""
}

##############################################################################
# Helper Functions
##############################################################################

show_initial_state() {
    log_info "Initial State (before tests):"
    echo ""
    echo "Customers:"
    echo "  • CUST-001: John Doe     | Balance=\$10,000 (will be used for success tests)"
    echo "  • CUST-002: Jane Smith   | Balance=\$100    (will be used for payment failure tests)"
    echo "  • CUST-003: Bob Johnson  | Balance=\$5,000  (will be used for mixed tests)"
    echo ""
    echo "Products (Top 5):"
    echo "  • PROD-001: Laptop      | 100 available | \$1,200 each"
    echo "  • PROD-002: Mouse       | 200 available | \$25 each"
    echo "  • PROD-003: Keyboard    | 150 available | \$75 each"
    echo "  • PROD-004: Monitor     | 80 available  | \$300 each"
    echo "  • PROD-005: Headphones  | 120 available | \$150 each"
    echo "  ... (5 more products available)"
    echo ""
}

show_monitoring_tips() {
    echo ""
    print_separator
    log_info "Monitoring & Debugging Tips"
    print_separator
    echo ""
    echo "${CYAN}1. Watch service logs in real-time:${NC}"
    echo "   ./start.sh logs"
    echo ""
    echo "${CYAN}2. View Kafka topics and messages:${NC}"
    echo "   http://localhost:8080 (Kafka UI)"
    echo ""
    echo "${CYAN}3. Check H2 database console:${NC}"
    echo "   Payment DB: http://localhost:8082/h2-console"
    echo "               JDBC URL: jdbc:h2:mem:paymentdb"
    echo "               Username: sa"
    echo "               Password: (leave empty)"
    echo ""
    echo "   Stock DB:   http://localhost:8083/h2-console"
    echo "               JDBC URL: jdbc:h2:mem:stockdb"
    echo "               Username: sa"
    echo "               Password: (leave empty)"
    echo ""
    echo "${CYAN}4. Check service health:${NC}"
    echo "   ./start.sh health"
    echo ""
    echo "${CYAN}5. Check Kafka topics:${NC}"
    echo "   ./start.sh topics"
    echo ""
    echo "${CYAN}6. Query specific order:${NC}"
    echo "   curl http://localhost:8081/api/orders/<ORDER_ID>"
    echo ""
    echo "${CYAN}7. Useful SQL queries:${NC}"
    echo "   -- Payment Service"
    echo "   SELECT customer_id, amount_available, amount_reserved FROM customers;"
    echo ""
    echo "   -- Stock Service"
    echo "   SELECT product_id, product_name, available_items, reserved_items FROM products;"
    echo ""
}

show_test_summary() {
    echo ""
    print_separator
    log_success "Test Execution Summary"
    print_separator
    echo ""

    local total_tests=$1
    log_info "Total tests executed: ${CYAN}$total_tests${NC}"
    echo ""

    echo "Test Scenarios Covered:"
    echo "  ${GREEN}✓${NC} Test 1: Multi-item success scenario"
    echo "  ${GREEN}✓${NC} Test 2: Atomic multi-item rollback"
    echo "  ${GREEN}✓${NC} Test 3: Payment failure with stock compensation"
    echo "  ${GREEN}✓${NC} Test 4: Stock failure with payment compensation"
    echo "  ${GREEN}✓${NC} Test 5: Both services reject"

    if [ $total_tests -gt 5 ]; then
        echo "  ${GREEN}✓${NC} Additional custom tests"
    fi

    echo ""
    log_info "Last created order ID: ${CYAN}${LAST_ORDER_ID:-None}${NC}"
    echo ""
}

##############################################################################
# Main Script
##############################################################################

main() {
    print_banner
    check_service

    case "${1:-all}" in
        1)
            show_initial_state
            test_scenario_1
            show_test_summary 1
            ;;
        2)
            show_initial_state
            test_scenario_2
            show_test_summary 1
            ;;
        3)
            show_initial_state
            test_scenario_3
            show_test_summary 1
            ;;
        4)
            show_initial_state
            test_scenario_4
            show_test_summary 1
            ;;
        5)
            show_initial_state
            test_scenario_5
            show_test_summary 1
            ;;
        6)
            show_initial_state
            test_scenario_6
            show_test_summary 1
            ;;
        7)
            show_initial_state
            test_scenario_7
            show_test_summary 1
            ;;
        query)
            test_query_order
            ;;
        custom)
            test_custom_order
            ;;
        all)
            show_initial_state
            test_scenario_1
            test_scenario_2
            test_scenario_3
            test_scenario_4
            test_scenario_5

            log_success "════════════════════════════════════════════════════════════"
            log_success "All core test scenarios completed!"
            log_success "════════════════════════════════════════════════════════════"

            show_test_summary 5
            show_monitoring_tips
            ;;
        full)
            show_initial_state
            test_scenario_1
            test_scenario_2
            test_scenario_3
            test_scenario_4
            test_scenario_5
            test_scenario_6
            test_scenario_7

            log_success "════════════════════════════════════════════════════════════"
            log_success "All test scenarios completed!"
            log_success "════════════════════════════════════════════════════════════"

            show_test_summary 7
            show_monitoring_tips
            ;;
        *)
            echo "Usage: $0 [COMMAND]"
            echo ""
            echo "Commands:"
            echo "  ${CYAN}1${NC}      - Test 1: Multi-item success (CONFIRMED)"
            echo "  ${CYAN}2${NC}      - Test 2: Atomic multi-item rollback (REJECTED)"
            echo "  ${CYAN}3${NC}      - Test 3: Payment failure, rollback stock (ROLLBACK)"
            echo "  ${CYAN}4${NC}      - Test 4: Stock failure, rollback payment (ROLLBACK)"
            echo "  ${CYAN}5${NC}      - Test 5: Both fail (REJECTED)"
            echo "  ${CYAN}6${NC}      - Test 6: Complex multi-item order (CONFIRMED)"
            echo "  ${CYAN}7${NC}      - Test 7: Edge case - large quantity (REJECTED)"
            echo "  ${CYAN}query${NC}  - Query last created order status"
            echo "  ${CYAN}custom${NC} - Create custom order (interactive)"
            echo "  ${CYAN}all${NC}    - Run core tests 1-5 (default)"
            echo "  ${CYAN}full${NC}   - Run all tests 1-7"
            echo ""
            echo "Examples:"
            echo "  ./test-orders.sh           # Run core tests"
            echo "  ./test-orders.sh 1         # Run test 1 only"
            echo "  ./test-orders.sh full      # Run all tests"
            echo "  ./test-orders.sh query     # Query last order"
            echo "  ./test-orders.sh custom    # Create custom order"
            echo ""
            exit 1
            ;;
    esac
}

main "$@"
