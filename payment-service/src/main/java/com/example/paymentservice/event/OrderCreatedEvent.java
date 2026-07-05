package com.example.paymentservice.event;

import lombok.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Event received from order-service via Kafka
 * 
 * IMPORTANT: Must match order-service's OrderCreatedEvent structure exactly
 * for JSON deserialization to work
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderCreatedEvent {
    
    private String orderId;
    private String customerId;
    private List<OrderItemEvent> items;
    private BigDecimal totalAmount;
    private LocalDateTime createdAt;
    
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OrderItemEvent {
        private String productId;
        private String productName;
        private int quantity;
        private BigDecimal price;
    }
}