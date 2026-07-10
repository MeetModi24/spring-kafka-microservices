#!/bin/bash

##############################################################################
# Spring Kafka Microservices - Comprehensive Startup Script
#
# This script provides complete lifecycle management for the microservices:
# 1. Starts Docker (Kafka, Zookeeper, Kafka UI)
# 2. Validates Kafka is ready with health checks
# 3. Compiles and starts all 3 microservices
# 4. Validates service health via actuator endpoints
# 5. Provides comprehensive status checks and monitoring
#
# Usage:
#   ./start.sh              # Start everything
#   ./start.sh --skip-build # Start without recompiling
#   ./start.sh stop         # Stop everything
#   ./start.sh restart      # Restart everything
#   ./start.sh status       # Check detailed status
#   ./start.sh logs         # Tail all logs
#   ./start.sh health       # Check service health endpoints
#   ./start.sh clean        # Stop and clean logs
##############################################################################

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m' # No Color

# Directories
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOGS_DIR="$SCRIPT_DIR/logs"
PIDS_FILE="$SCRIPT_DIR/.service-pids"

# Service ports
ORDER_PORT=8081
PAYMENT_PORT=8082
STOCK_PORT=8083
KAFKA_UI_PORT=8080
KAFKA_PORT=9092
ZOOKEEPER_PORT=2181

# Configuration
SKIP_BUILD=false
MAX_WAIT_TIME=120  # 2 minutes max wait for services

##############################################################################
# Utility Functions
##############################################################################

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_debug() {
    echo -e "${CYAN}[DEBUG]${NC} $1"
}

print_banner() {
    echo -e "${GREEN}"
    echo "╔═══════════════════════════════════════════════════════════════╗"
    echo "║                                                               ║"
    echo "║     Spring Kafka Microservices - SAGA Pattern Demo           ║"
    echo "║                                                               ║"
    echo "║  Services:                                                    ║"
    echo "║    - order-service   (Port $ORDER_PORT)                             ║"
    echo "║    - payment-service (Port $PAYMENT_PORT)                             ║"
    echo "║    - stock-service   (Port $STOCK_PORT)                             ║"
    echo "║                                                               ║"
    echo "║  Infrastructure:                                              ║"
    echo "║    - Kafka Broker    (Port $KAFKA_PORT)                              ║"
    echo "║    - Zookeeper       (Port $ZOOKEEPER_PORT)                             ║"
    echo "║    - Kafka UI        (Port $KAFKA_UI_PORT - http://localhost:$KAFKA_UI_PORT)     ║"
    echo "║                                                               ║"
    echo "╚═══════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

check_command() {
    if ! command -v $1 &> /dev/null; then
        log_error "$1 is not installed. Please install it first."

        case $1 in
            docker)
                echo "Install Docker: https://docs.docker.com/get-docker/"
                ;;
            docker-compose)
                echo "Install Docker Compose: https://docs.docker.com/compose/install/"
                ;;
            mvn)
                echo "Install Maven: https://maven.apache.org/install.html"
                ;;
            nc)
                echo "Install netcat (usually pre-installed on Unix systems)"
                ;;
            curl)
                echo "Install curl: https://curl.se/download.html"
                ;;
        esac
        exit 1
    fi
}

wait_for_port() {
    local port=$1
    local service=$2
    local max_attempts=${3:-$MAX_WAIT_TIME}
    local attempt=1

    log_info "Waiting for $service (port $port) to be ready..."

    while ! nc -z localhost $port &> /dev/null; do
        if [ $attempt -gt $max_attempts ]; then
            log_error "$service failed to start on port $port after $max_attempts seconds"
            log_error "Check logs at: $LOGS_DIR/$service.log"
            return 1
        fi
        printf "."
        sleep 1
        ((attempt++))
    done

    echo ""
    log_success "$service is ready on port $port"
    return 0
}

wait_for_kafka() {
    log_info "Waiting for Kafka to be ready..."
    local max_attempts=60
    local attempt=1

    # First, wait for port
    while ! nc -z localhost $KAFKA_PORT &> /dev/null; do
        if [ $attempt -gt $max_attempts ]; then
            log_error "Kafka port $KAFKA_PORT is not available after $max_attempts seconds"
            return 1
        fi
        printf "."
        sleep 1
        ((attempt++))
    done

    echo ""
    log_info "Kafka port is up, verifying broker availability..."

    # Then verify Kafka broker is functional
    attempt=1
    while ! docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list &> /dev/null; do
        if [ $attempt -gt $max_attempts ]; then
            log_error "Kafka broker failed to become ready after $max_attempts seconds"
            log_error "Check Docker logs: docker logs kafka"
            return 1
        fi
        printf "."
        sleep 1
        ((attempt++))
    done

    echo ""
    log_success "Kafka broker is ready and accepting connections"
    return 0
}

wait_for_service_health() {
    local service=$1
    local port=$2
    local max_attempts=60
    local attempt=1

    log_info "Checking $service health endpoint..."

    while ! curl -s "http://localhost:$port/actuator/health" | grep -q '"status":"UP"' 2>/dev/null; do
        if [ $attempt -gt $max_attempts ]; then
            log_warn "$service health check failed after $max_attempts seconds (may still be starting)"
            return 1
        fi
        printf "."
        sleep 1
        ((attempt++))
    done

    echo ""
    log_success "$service health check passed"
    return 0
}

create_logs_dir() {
    if [ ! -d "$LOGS_DIR" ]; then
        mkdir -p "$LOGS_DIR"
        log_info "Created logs directory: $LOGS_DIR"
    fi
}

##############################################################################
# Docker Functions
##############################################################################

check_docker_running() {
    if ! docker info &> /dev/null; then
        log_error "Docker is not running. Please start Docker first."
        exit 1
    fi
}

start_docker() {
    log_info "Starting Docker containers (Kafka, Zookeeper, Kafka UI)..."

    cd "$SCRIPT_DIR"

    # Check if containers are already running
    if docker-compose ps | grep -q "Up"; then
        log_warn "Some containers are already running"
        log_info "Stopping existing containers first..."
        docker-compose down
        sleep 2
    fi

    docker-compose up -d

    if [ $? -eq 0 ]; then
        log_success "Docker containers started"
    else
        log_error "Failed to start Docker containers"
        log_error "Check Docker Compose logs: docker-compose logs"
        exit 1
    fi

    # Wait for Kafka to be ready
    wait_for_kafka

    # Give Kafka extra time to stabilize
    log_info "Allowing Kafka to stabilize..."
    sleep 5
    log_success "Kafka is stable and ready"
}

stop_docker() {
    log_info "Stopping Docker containers..."

    cd "$SCRIPT_DIR"
    docker-compose down

    if [ $? -eq 0 ]; then
        log_success "Docker containers stopped"
    else
        log_warn "Failed to stop Docker containers (they may not be running)"
    fi
}

check_docker_status() {
    log_info "Docker Containers Status:"
    echo ""

    local containers=("kafka" "zookeeper" "kafka-ui")
    for container in "${containers[@]}"; do
        if docker ps --format '{{.Names}}' | grep -q "^${container}$"; then
            local status=$(docker inspect --format='{{.State.Status}}' $container)
            local uptime=$(docker inspect --format='{{.State.StartedAt}}' $container | cut -d'.' -f1)
            echo -e "  ${GREEN}✓${NC} $container: ${GREEN}$status${NC} (started: $uptime)"
        else
            echo -e "  ${RED}✗${NC} $container: ${RED}not running${NC}"
        fi
    done
    echo ""
}

##############################################################################
# Service Functions
##############################################################################

compile_services() {
    if [ "$SKIP_BUILD" = true ]; then
        log_warn "Skipping compilation (--skip-build flag set)"
        return 0
    fi

    log_info "Compiling all services..."

    local services=("order-service" "payment-service" "stock-service")

    for service in "${services[@]}"; do
        log_info "Compiling $service..."
        cd "$SCRIPT_DIR/$service"

        # Use clean package to ensure fresh build
        if mvn clean compile -q > "$LOGS_DIR/$service-compile.log" 2>&1; then
            log_success "$service compiled successfully"
        else
            log_error "$service compilation failed"
            log_error "Check logs: $LOGS_DIR/$service-compile.log"
            echo ""
            echo "Last 20 lines of compilation log:"
            tail -20 "$LOGS_DIR/$service-compile.log"
            exit 1
        fi
    done

    cd "$SCRIPT_DIR"
    log_success "All services compiled successfully"
}

start_service() {
    local service=$1
    local port=$2

    log_info "Starting $service on port $port..."

    cd "$SCRIPT_DIR/$service"

    # Check if port is already in use
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
        log_warn "Port $port is already in use. Attempting to kill existing process..."
        kill_by_port $port $service
        sleep 2
    fi

    # Start service in background and redirect output to log file
    nohup mvn spring-boot:run \
        > "$LOGS_DIR/$service.log" 2>&1 &

    local pid=$!
    echo "$service:$pid:$port" >> "$PIDS_FILE"

    log_info "$service started with PID $pid"

    # Wait for port to be available
    if wait_for_port $port $service; then
        # Additional health check via actuator
        sleep 2  # Give service time to fully initialize
        wait_for_service_health $service $port
    else
        log_error "$service failed to start"
        log_error "Check logs: $LOGS_DIR/$service.log"
        return 1
    fi
}

start_all_services() {
    log_info "Starting all microservices..."

    create_logs_dir

    # Remove old PIDs file
    rm -f "$PIDS_FILE"

    # Start services in order
    # Order service first (orchestrator)
    start_service "order-service" $ORDER_PORT

    # Payment and stock services can start in parallel
    log_info "Starting payment and stock services..."
    start_service "payment-service" $PAYMENT_PORT
    start_service "stock-service" $STOCK_PORT

    echo ""
    log_success "All services started successfully"
}

stop_services() {
    log_info "Stopping all services..."

    if [ ! -f "$PIDS_FILE" ]; then
        log_warn "No PIDs file found. Attempting to stop services by port..."
        kill_by_port $ORDER_PORT "order-service"
        kill_by_port $PAYMENT_PORT "payment-service"
        kill_by_port $STOCK_PORT "stock-service"
        return
    fi

    while IFS=: read -r service pid port; do
        if [ -z "$service" ]; then
            continue
        fi

        if ps -p $pid > /dev/null 2>&1; then
            log_info "Stopping $service (PID: $pid)..."
            kill $pid 2>/dev/null

            # Wait up to 10 seconds for graceful shutdown
            local wait_count=0
            while ps -p $pid > /dev/null 2>&1 && [ $wait_count -lt 10 ]; do
                sleep 1
                ((wait_count++))
            done

            # Force kill if still running
            if ps -p $pid > /dev/null 2>&1; then
                log_warn "Force killing $service (PID: $pid)..."
                kill -9 $pid 2>/dev/null
            fi

            log_success "$service stopped"
        else
            log_warn "$service (PID: $pid) is not running"
        fi
    done < "$PIDS_FILE"

    rm -f "$PIDS_FILE"
    log_success "All services stopped"
}

kill_by_port() {
    local port=$1
    local service=$2

    local pid=$(lsof -ti:$port 2>/dev/null)
    if [ ! -z "$pid" ]; then
        log_info "Found process on port $port (PID: $pid), stopping..."
        kill $pid 2>/dev/null
        sleep 2
        if ps -p $pid > /dev/null 2>&1; then
            kill -9 $pid 2>/dev/null
        fi
        log_success "Process on port $port stopped"
    fi
}

##############################################################################
# Status & Monitoring Functions
##############################################################################

check_status() {
    print_banner

    echo ""
    check_docker_status

    log_info "Microservices Status:"
    echo ""

    check_service_status "order-service" $ORDER_PORT
    check_service_status "payment-service" $PAYMENT_PORT
    check_service_status "stock-service" $STOCK_PORT

    echo ""
    log_info "Access Points:"
    echo "  ${CYAN}•${NC} Kafka UI:        http://localhost:$KAFKA_UI_PORT"
    echo "  ${CYAN}•${NC} Order Service:   http://localhost:$ORDER_PORT/actuator/health"
    echo "  ${CYAN}•${NC} Payment Service: http://localhost:$PAYMENT_PORT/actuator/health"
    echo "  ${CYAN}•${NC} Stock Service:   http://localhost:$STOCK_PORT/actuator/health"
    echo ""
    echo "  ${CYAN}•${NC} Payment H2:      http://localhost:$PAYMENT_PORT/h2-console"
    echo "  ${CYAN}•${NC} Stock H2:        http://localhost:$STOCK_PORT/h2-console"
    echo ""
}

check_service_status() {
    local service=$1
    local port=$2

    if nc -z localhost $port &> /dev/null; then
        # Check health endpoint
        local health_status=$(curl -s "http://localhost:$port/actuator/health" 2>/dev/null | grep -o '"status":"[^"]*"' | cut -d'"' -f4)

        if [ "$health_status" = "UP" ]; then
            echo -e "  ${GREEN}✓${NC} $service: ${GREEN}RUNNING${NC} (port $port, health: ${GREEN}UP${NC})"
        else
            echo -e "  ${YELLOW}△${NC} $service: ${YELLOW}STARTING${NC} (port $port, health: ${YELLOW}${health_status:-UNKNOWN}${NC})"
        fi
    else
        echo -e "  ${RED}✗${NC} $service: ${RED}NOT RUNNING${NC} (port $port)"
    fi
}

check_health_endpoints() {
    log_info "Checking health endpoints..."
    echo ""

    local services=("order-service:$ORDER_PORT" "payment-service:$PAYMENT_PORT" "stock-service:$STOCK_PORT")

    for service_info in "${services[@]}"; do
        IFS=: read -r service port <<< "$service_info"

        echo -e "${CYAN}$service (http://localhost:$port/actuator/health):${NC}"
        local response=$(curl -s "http://localhost:$port/actuator/health" 2>/dev/null)

        if [ ! -z "$response" ]; then
            echo "$response" | jq '.' 2>/dev/null || echo "$response"
        else
            echo -e "${RED}  Service not responding${NC}"
        fi
        echo ""
    done
}

tail_logs() {
    log_info "Tailing logs (Ctrl+C to stop)..."
    log_info "Log files location: $LOGS_DIR"
    echo ""

    if [ ! -d "$LOGS_DIR" ]; then
        log_error "Logs directory not found: $LOGS_DIR"
        exit 1
    fi

    if ! ls "$LOGS_DIR"/*.log 1> /dev/null 2>&1; then
        log_error "No log files found in $LOGS_DIR"
        exit 1
    fi

    # Use multitail if available, otherwise fall back to tail
    if command -v multitail &> /dev/null; then
        multitail "$LOGS_DIR"/*.log
    else
        tail -f "$LOGS_DIR"/*.log 2>/dev/null
    fi
}

##############################################################################
# Kafka Functions
##############################################################################

list_kafka_topics() {
    log_info "Kafka Topics:"
    echo ""
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
    echo ""
}

describe_kafka_topics() {
    log_info "Describing Kafka topics..."
    echo ""

    local topics=$(docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null)

    for topic in $topics; do
        echo -e "${CYAN}Topic: $topic${NC}"
        docker exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic $topic
        echo ""
    done
}

##############################################################################
# Main Script
##############################################################################

parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --skip-build)
                SKIP_BUILD=true
                shift
                ;;
            *)
                COMMAND=$1
                shift
                ;;
        esac
    done
}

main() {
    parse_args "$@"

    case "${COMMAND:-start}" in
        start)
            print_banner
            check_command docker
            check_command mvn
            check_command nc
            check_command curl
            check_docker_running

            start_docker
            compile_services
            start_all_services

            echo ""
            log_success "════════════════════════════════════════════════════════════"
            log_success "All systems operational!"
            log_success "════════════════════════════════════════════════════════════"
            echo ""
            echo "Next steps:"
            echo "  ${CYAN}1.${NC} Check status:    ./start.sh status"
            echo "  ${CYAN}2.${NC} View logs:       ./start.sh logs"
            echo "  ${CYAN}3.${NC} Run tests:       ./test-orders.sh"
            echo "  ${CYAN}4.${NC} Kafka UI:        http://localhost:$KAFKA_UI_PORT"
            echo "  ${CYAN}5.${NC} Health check:    ./start.sh health"
            echo ""
            ;;

        stop)
            print_banner
            stop_services
            stop_docker
            log_success "All systems stopped"
            ;;

        restart)
            print_banner
            log_info "Restarting all services..."
            $0 stop
            sleep 3
            $0 start
            ;;

        status)
            check_status
            ;;

        health)
            print_banner
            check_health_endpoints
            ;;

        logs)
            tail_logs
            ;;

        topics)
            print_banner
            list_kafka_topics
            describe_kafka_topics
            ;;

        clean)
            log_info "Cleaning up..."
            stop_services
            stop_docker
            rm -rf "$LOGS_DIR"
            rm -f "$PIDS_FILE"
            log_success "Cleanup complete"
            ;;

        *)
            echo "Usage: $0 [OPTIONS] {COMMAND}"
            echo ""
            echo "Commands:"
            echo "  start   - Start Docker and all microservices"
            echo "  stop    - Stop all microservices and Docker"
            echo "  restart - Restart everything"
            echo "  status  - Check status of all services"
            echo "  health  - Check health endpoints of all services"
            echo "  logs    - Tail all service logs"
            echo "  topics  - List and describe Kafka topics"
            echo "  clean   - Stop everything and clean up logs"
            echo ""
            echo "Options:"
            echo "  --skip-build  Skip Maven compilation (faster restart)"
            echo ""
            echo "Examples:"
            echo "  ./start.sh                  # Full start with compilation"
            echo "  ./start.sh --skip-build     # Start without recompiling"
            echo "  ./start.sh status           # Check status"
            echo "  ./start.sh health           # Detailed health check"
            exit 1
            ;;
    esac
}

main "$@"
