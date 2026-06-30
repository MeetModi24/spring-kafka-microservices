package com.example.orderservice.exception;

/**
 * Thrown when order validation fails
 * HTTP Status: 400 BAD REQUEST
 */
public class InvalidOrderException extends RuntimeException {
    
    public InvalidOrderException(String message) {
        super(message);
    }
    
    public InvalidOrderException(String message, Throwable cause) {
        super(message, cause);
    }
}