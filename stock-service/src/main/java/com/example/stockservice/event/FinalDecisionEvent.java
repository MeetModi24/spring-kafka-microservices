package com.example.stockservice.event;

import lombok.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Final decision event sent to all SAGA participants (Phase 5 version with source tracking)
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FinalDecisionEvent {

    private String orderId;
    private String customerId;
    private List<OrderItemDTO> items;  // All order items
    private DecisionStatus status;
    private String reason;
    private String source;  // "PAYMENT" or "STOCK" - tracks which service caused ROLLBACK
    private LocalDateTime decidedAt;

    /**
     * Decision status enum
     */
    public enum DecisionStatus {
        /**
         * Order confirmed - commit all reservations
         */
        CONFIRMED,

        /**
         * Order rejected - both services failed, nothing to compensate
         */
        REJECTED,

        /**
         * Partial success - rollback the successful service
         * source field indicates which service failed
         */
        ROLLBACK
    }

    /**
     * DTO for order items in decision event
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class OrderItemDTO {
        private String productId;
        private String productName;
        private int quantity;
        private BigDecimal price;
    }
}
