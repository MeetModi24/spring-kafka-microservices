package com.example.orderservice.repository;

import com.example.orderservice.model.Order;
import com.example.orderservice.model.OrderStatus;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ORDER REPOSITORY (Data Access Layer)
 * =====================================
 *
 * PATTERN: Repository Pattern
 * - Abstracts data storage implementation from business logic
 * - Provides a collection-like interface for domain objects
 * - Isolates domain layer from persistence concerns
 *
 * CURRENT IMPLEMENTATION: In-Memory Storage
 * - Uses ConcurrentHashMap for thread-safe operations
 * - Data lost on restart (not production-ready)
 * - Good for learning, prototyping, and testing
 *
 * FUTURE MIGRATION PATH TO DATABASE:
 *
 * Option 1: Spring Data JPA (Most common)
 * ----------------------------------------
 * 1. Add dependency:
 *    <dependency>
 *        <groupId>org.springframework.boot</groupId>
 *        <artifactId>spring-boot-starter-data-jpa</artifactId>
 *    </dependency>
 *    <dependency>
 *        <groupId>com.h2database</groupId>
 *        <artifactId>h2</artifactId>
 *        <scope>runtime</scope>
 *    </dependency>
 *
 * 2. Convert to interface:
 *    public interface OrderRepository extends JpaRepository<Order, String> {
 *        List<Order> findByCustomerId(String customerId);
 *        List<Order> findByStatus(OrderStatus status);
 *    }
 *
 * 3. Add JPA annotations to Order model:
 *    @Entity
 *    @Table(name = "orders")
 *    public class Order {
 *        @Id
 *        private String orderId;
 *        ...
 *    }
 *
 * 4. Configure datasource in application.yml:
 *    spring:
 *      datasource:
 *        url: jdbc:h2:mem:orderdb
 *      jpa:
 *        hibernate:
 *          ddl-auto: update
 *
 * Option 2: Spring Data MongoDB (NoSQL)
 * --------------------------------------
 * 1. Add dependency:
 *    <dependency>
 *        <groupId>org.springframework.boot</groupId>
 *        <artifactId>spring-boot-starter-data-mongodb</artifactId>
 *    </dependency>
 *
 * 2. Convert to interface:
 *    public interface OrderRepository extends MongoRepository<Order, String> {
 *        List<Order> findByCustomerId(String customerId);
 *    }
 *
 * 3. Add @Document annotation:
 *    @Document(collection = "orders")
 *    public class Order { ... }
 *
 * WHY USE REPOSITORY PATTERN?
 * - Testability: Easy to mock in unit tests
 * - Flexibility: Swap storage implementation without changing service code
 * - Separation of Concerns: Business logic doesn't know about storage details
 * - Query Encapsulation: Complex queries hidden behind method names
 */
@Repository
public class OrderRepository {

    /**
     * In-memory storage
     * Thread-safe concurrent map for multi-threaded access
     */
    private final Map<String, Order> orderStore = new ConcurrentHashMap<>();

    /**
     * Save or update an order.
     *
     * SEMANTICS:
     * - If orderId doesn't exist: creates new order
     * - If orderId exists: updates existing order
     *
     * In Spring Data JPA, this would be:
     *   orderRepository.save(order);
     *
     * @param order The order to save
     * @return The saved order
     */
    public Order save(Order order) {
        if (order == null || order.getOrderId() == null) {
            throw new IllegalArgumentException("Order and orderId cannot be null");
        }
        orderStore.put(order.getOrderId(), order);
        return order;
    }

    /**
     * Find order by ID.
     *
     * DESIGN CHOICE: Optional vs null
     * - Optional.empty() explicitly signals "not found"
     * - Avoids NullPointerException pitfalls
     * - Forces caller to handle missing case
     *
     * In Spring Data JPA, this would be:
     *   orderRepository.findById(orderId);
     *
     * @param orderId The order ID
     * @return Optional containing order if found, empty otherwise
     */
    public Optional<Order> findById(String orderId) {
        return Optional.ofNullable(orderStore.get(orderId));
    }

    /**
     * Find all orders.
     *
     * In Spring Data JPA, this would be:
     *   orderRepository.findAll();
     *
     * @return List of all orders
     */
    public List<Order> findAll() {
        return new ArrayList<>(orderStore.values());
    }

    /**
     * Find orders by customer ID.
     *
     * CUSTOM QUERY: Not provided by default JpaRepository
     * In Spring Data JPA, you would just declare:
     *   List<Order> findByCustomerId(String customerId);
     * Spring generates the query automatically based on method name!
     *
     * @param customerId The customer ID
     * @return List of orders for this customer
     */
    public List<Order> findByCustomerId(String customerId) {
        return orderStore.values().stream()
                .filter(order -> customerId.equals(order.getCustomerId()))
                .toList();
    }

    /**
     * Find orders by status.
     *
     * In Spring Data JPA, you would declare:
     *   List<Order> findByStatus(OrderStatus status);
     *
     * @param status The order status
     * @return List of orders with this status
     */
    public List<Order> findByStatus(OrderStatus status) {
        return orderStore.values().stream()
                .filter(order -> status == order.getStatus())
                .toList();
    }

    /**
     * Check if order exists.
     *
     * In Spring Data JPA:
     *   orderRepository.existsById(orderId);
     *
     * @param orderId The order ID
     * @return true if order exists, false otherwise
     */
    public boolean existsById(String orderId) {
        return orderStore.containsKey(orderId);
    }

    /**
     * Delete order by ID.
     *
     * In Spring Data JPA:
     *   orderRepository.deleteById(orderId);
     *
     * @param orderId The order ID
     */
    public void deleteById(String orderId) {
        orderStore.remove(orderId);
    }

    /**
     * Count total orders.
     *
     * In Spring Data JPA:
     *   orderRepository.count();
     *
     * @return Total number of orders
     */
    public long count() {
        return orderStore.size();
    }

    /**
     * Delete all orders.
     * WARNING: Use with caution!
     *
     * In Spring Data JPA:
     *   orderRepository.deleteAll();
     */
    public void deleteAll() {
        orderStore.clear();
    }
}
