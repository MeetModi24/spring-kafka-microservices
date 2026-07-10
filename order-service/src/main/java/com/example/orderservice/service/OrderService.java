package com.example.orderservice.service;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;

import com.example.orderservice.dto.CreateOrderRequest;
import com.example.orderservice.event.OrderCreatedEvent;
import com.example.orderservice.exception.InvalidOrderException;
import com.example.orderservice.exception.OrderNotFoundException;
import com.example.orderservice.model.Order;
import com.example.orderservice.model.OrderItem;
import com.example.orderservice.repository.OrderRepository;

/**
 * ORDER SERVICE (Business Logic Layer)
 * =====================================
 *
 * RESPONSIBILITIES:
 * - Validate business rules
 * - Coordinate operations across multiple components
 * - Transform between DTOs and domain models
 * - Publish domain events
 *
 * PATTERN: Service Layer
 * - Sits between Controller (API) and Repository (Data)
 * - Contains business logic, not just CRUD operations
 * - Transaction boundary (when using @Transactional with database)
 *
 * DEPENDENCIES:
 * - OrderRepository: Data access
 * - OrderEventProducer: Event publishing
 */
@Service
public class OrderService {
    private final OrderRepository orderRepository;
    private final OrderEventProducer eventProducer;

    /**
     * Constructor injection (recommended over field injection)
     * Spring automatically injects required beans
     *
     * WHY CONSTRUCTOR INJECTION?
     * - Makes dependencies explicit
     * - Enables immutability (final fields)
     * - Easier to test (can pass mocks in constructor)
     * - Required dependencies are clear at compile time
     */
    public OrderService(OrderRepository orderRepository, OrderEventProducer eventProducer) {
        this.orderRepository = orderRepository;
        this.eventProducer = eventProducer;
    }

    /**
     * Create a new order.
     *
     * BUSINESS FLOW:
     * 1. Validate request and transform to domain model
     * 2. Validate business rules
     * 3. Persist to repository
     * 4. Publish domain event
     *
     * DESIGN PATTERN: Command Handler
     * - Receives command (CreateOrderRequest)
     * - Validates and executes
     * - Returns result (Order)
     *
     * @param request The create order request DTO
     * @return Created order
     * @throws InvalidOrderException if validation fails
     */
    public Order createOrder(CreateOrderRequest request) {
        // Map DTO items -> Domain items
        List<OrderItem> orderItems = request.getItems().stream()
            .map(dto -> new OrderItem(
                dto.getProductId(),
                dto.getProductName(),
                dto.getQuantity(),
                dto.getPrice()
            ))
            .toList();

        // Create Order with domain items
        Order order = new Order(request.getCustomerId(), orderItems);
        order.setOrderId(UUID.randomUUID().toString());

        // Validate business rules
        if (!order.isValid()) {
            throw new InvalidOrderException("Order validation failed: " +
                "Check that all items have valid productId, quantity > 0, and price > 0");
        }

        // Persist to repository (replaces direct map access)
        order = orderRepository.save(order);

        // Publish event to Kafka (asynchronous)
        // This happens AFTER order is persisted to ensure consistency
        OrderCreatedEvent event = mapToEvent(order);
        eventProducer.publishOrderCreated(event);

        return order;
    }

    /**
     * Helper method to map Order domain model → OrderCreatedEvent
     *
     * WHY SEPARATE MAPPING?
     * - Domain model (Order) has internal fields we don't want to publish
     * - Event schema is a contract with consumers, should be stable
     * - Allows domain model to evolve without breaking consumers
     */
    private OrderCreatedEvent mapToEvent(Order order) {
        // Map OrderItem → OrderItemEvent
        List<OrderCreatedEvent.OrderItemEvent> itemEvents = order.getItems().stream()
            .map(item -> new OrderCreatedEvent.OrderItemEvent(
                item.getProductId(),
                item.getProductName(),
                item.getQuantity(),
                item.getPrice()
            ))
            .collect(Collectors.toList());

        // Create event with selected fields
        return new OrderCreatedEvent(
            order.getOrderId(),
            order.getCustomerId(),
            itemEvents,
            order.getTotalAmount(),
            order.getCreatedAt()
        );
    }
    
    /**
     * Retrieve order by ID.
     *
     * PATTERN: Query Handler
     * - Simple read operation
     * - Delegates to repository
     * - Throws exception if not found
     *
     * @param orderId The order ID
     * @return The order
     * @throws OrderNotFoundException if order doesn't exist
     */
    public Order getOrderById(String orderId) {
        return orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));
    }

    /**
     * Get all orders.
     *
     * NOTE: In production, this should be paginated to avoid
     * loading huge datasets into memory.
     *
     * Better signature would be:
     *   Page<Order> getAllOrders(Pageable pageable)
     *
     * @return List of all orders
     */
    public List<Order> getAllOrders() {
        return orderRepository.findAll();
    }

}
