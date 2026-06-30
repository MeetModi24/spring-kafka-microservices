package com.example.orderservice.exception;

/**
 * Thrown when an order is not found in the system
 * HTTP Status: 404 NOT FOUND
 */
public class OrderNotFoundException extends RuntimeException {
    
    private final String orderId;
    
    public OrderNotFoundException(String orderId) {
        super("Order not found: " + orderId);
        this.orderId = orderId;
    }
    
    public String getOrderId() {
        return orderId;
    }
}