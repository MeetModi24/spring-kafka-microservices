package com.example.orderservice.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;

import com.example.orderservice.dto.CreateOrderRequest;
import com.example.orderservice.event.OrderCreatedEvent;
import com.example.orderservice.exception.InvalidOrderException;
import com.example.orderservice.exception.OrderNotFoundException;
import com.example.orderservice.model.Order;
import com.example.orderservice.model.OrderItem;

@Service
public class OrderService {
    private final Map<String, Order> orderStore = new ConcurrentHashMap<>();
    private final OrderEventProducer eventProducer;

    /**
     * Constructor injection
     * Spring automatically injects OrderEventProducer bean
     */
    public OrderService(OrderEventProducer eventProducer) {
        this.eventProducer = eventProducer;
    }

    public Order createOrder(CreateOrderRequest request){ // customerId, items
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

        if (!order.isValid()) {
            throw new InvalidOrderException("Order validation failed: " +
                "Check that all items have valid productId, quantity > 0, and price > 0");
        }

        // Store order in memory
        orderStore.put(order.getOrderId(), order);

        // Publish event to Kafka (asynchronous)
        // This happens AFTER order is stored to ensure consistency
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
    
    public Order getOrderById(String Id){
        Order order = orderStore.get(Id);
        if (order == null) {
            throw new OrderNotFoundException(Id);
        }
        return order;
    }

    public List<Order> getAllOrders(){
        return new ArrayList<>(orderStore.values());
    }

}
