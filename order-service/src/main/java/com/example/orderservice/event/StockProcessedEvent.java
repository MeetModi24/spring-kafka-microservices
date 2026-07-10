package com.example.orderservice.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Event published by stock-service after processing stock reservation.
 * Consumed by order-service orchestrator.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class StockProcessedEvent {
    private String orderId;
    private String customerId;
    private List<StockItemResult> items;  // Results for ALL items
    private StockStatus status;
    private String reason;  // Rejection reason (if status = REJECT)

    public enum StockStatus {
        ACCEPT,   // All items reserved successfully
        REJECT    // At least one item unavailable
    }

    /**
     * Inner class for individual item results.
     * Useful for tracking which specific items failed.
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class StockItemResult {
        private String productId;
        private String productName;
        private int quantity;
        private boolean available;  // Was this specific item available?
    }
}
