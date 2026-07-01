package com.example.orderservice.event;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Event published when an order is created
 *
 * DESIGN PATTERN: Event-Driven Architecture
 * - Decouples order-service from payment-service/stock-service
 * - Services communicate via events, not direct API calls
 * - Enables asynchronous processing
 *
 * WHY A SEPARATE EVENT CLASS (not reuse Order)?
 * - Domain model (Order) may have fields we don't want to publish
 * - Event schema should be stable (versioned contract)
 * - Domain model can evolve independently
 *
 * IMMUTABILITY:
 * - Events represent facts that happened
 * - Should not be modified after creation
 * - No setters (only getters) would be better, but Jackson needs them
 */
public class OrderCreatedEvent {

    private String orderId;
    private String customerId;
    private List<OrderItemEvent> items;
    private BigDecimal totalAmount;
    private LocalDateTime createdAt;

    /**
     * Default constructor (required by Jackson for deserialization)
     */
    public OrderCreatedEvent() {
    }

    /**
     * Constructor for creating events
     */
    public OrderCreatedEvent(String orderId, String customerId,
                            List<OrderItemEvent> items, BigDecimal totalAmount,
                            LocalDateTime createdAt) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.items = items;
        this.totalAmount = totalAmount;
        this.createdAt = createdAt;
    }

    // Getters and Setters
    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public List<OrderItemEvent> getItems() {
        return items;
    }

    public void setItems(List<OrderItemEvent> items) {
        this.items = items;
    }

    public BigDecimal getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(BigDecimal totalAmount) {
        this.totalAmount = totalAmount;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    @Override
    public String toString() {
        return "OrderCreatedEvent{" +
                "orderId='" + orderId + '\'' +
                ", customerId='" + customerId + '\'' +
                ", items=" + items +
                ", totalAmount=" + totalAmount +
                ", createdAt=" + createdAt +
                '}';
    }

    /**
     * Inner class for order item events
     * Nested inside OrderCreatedEvent to keep event structure cohesive
     */
    public static class OrderItemEvent {
        private String productId;
        private String productName;
        private int quantity;
        private BigDecimal price;

        /**
         * Default constructor (for Jackson)
         */
        public OrderItemEvent() {
        }

        /**
         * Constructor for creating item events
         */
        public OrderItemEvent(String productId, String productName,
                             int quantity, BigDecimal price) {
            this.productId = productId;
            this.productName = productName;
            this.quantity = quantity;
            this.price = price;
        }

        // Getters and Setters
        public String getProductId() {
            return productId;
        }

        public void setProductId(String productId) {
            this.productId = productId;
        }

        public String getProductName() {
            return productName;
        }

        public void setProductName(String productName) {
            this.productName = productName;
        }

        public int getQuantity() {
            return quantity;
        }

        public void setQuantity(int quantity) {
            this.quantity = quantity;
        }

        public BigDecimal getPrice() {
            return price;
        }

        public void setPrice(BigDecimal price) {
            this.price = price;
        }

        @Override
        public String toString() {
            return "OrderItemEvent{" +
                    "productId='" + productId + '\'' +
                    ", productName='" + productName + '\'' +
                    ", quantity=" + quantity +
                    ", price=" + price +
                    '}';
        }
    }
}
