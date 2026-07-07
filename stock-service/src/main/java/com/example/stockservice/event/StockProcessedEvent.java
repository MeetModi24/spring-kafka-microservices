package com.example.stockservice.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class StockProcessedEvent {
    private String orderId;
    private String productId;
    private int quantity;
    private StockStatus status;
    private String reason;  // Rejection reason (if status = REJECT)
    
    public enum StockStatus {
        ACCEPT,   // Stock reserved successfully
        REJECT    // Insufficient stock
    }
}