package com.example.orderservice.exception;

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;

import com.example.orderservice.dto.ErrorResponse;

/**
 * Global exception handler using @ControllerAdvice
 * 
 * DESIGN PATTERN: Aspect-Oriented Programming (AOP)
 * - Separates cross-cutting concerns (error handling) from business logic
 * - Single place to handle all exceptions
 * - Consistent error response format
 * 
 * HOW IT WORKS:
 * 1. Spring intercepts exceptions thrown by controllers
 * 2. Matches exception type to @ExceptionHandler method
 * 3. Returns ResponseEntity with appropriate HTTP status
 */
@RestControllerAdvice
public class GlobalExceptionHandler {
    
    /**
     * Handle OrderNotFoundException
     * HTTP 404 Not Found
     */
    @ExceptionHandler(OrderNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleOrderNotFound(
            OrderNotFoundException ex, 
            WebRequest request) {
        
        ErrorResponse error = new ErrorResponse(
            HttpStatus.NOT_FOUND.value(),
            "Not Found",
            ex.getMessage(),
            request.getDescription(false).replace("uri=", "")
        );
        
        return new ResponseEntity<>(error, HttpStatus.NOT_FOUND);
    }
    
    /**
     * Handle InvalidOrderException
     * HTTP 400 Bad Request
     */
    @ExceptionHandler(InvalidOrderException.class)
    public ResponseEntity<ErrorResponse> handleInvalidOrder(
            InvalidOrderException ex, 
            WebRequest request) {
        
        ErrorResponse error = new ErrorResponse(
            HttpStatus.BAD_REQUEST.value(),
            "Bad Request",
            ex.getMessage(),
            request.getDescription(false).replace("uri=", "")
        );
        
        return new ResponseEntity<>(error, HttpStatus.BAD_REQUEST);
    }
    
    /**
     * Handle Bean Validation errors (@Valid)
     * HTTP 400 Bad Request
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidationErrors(
            MethodArgumentNotValidException ex,
            WebRequest request) {
        
        // Extract validation error messages
        List<String> details = ex.getBindingResult()
            .getFieldErrors()
            .stream()
            .map(error -> error.getField() + ": " + error.getDefaultMessage())
            .collect(Collectors.toList());
        
        ErrorResponse error = new ErrorResponse(
            HttpStatus.BAD_REQUEST.value(),
            "Validation Failed",
            "Input validation failed. Check 'details' field.",
            request.getDescription(false).replace("uri=", "")
        );
        error.setDetails(details);
        
        return new ResponseEntity<>(error, HttpStatus.BAD_REQUEST);
    }
    
    /**
     * Handle all other exceptions
     * HTTP 500 Internal Server Error
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGlobalException(
            Exception ex, 
            WebRequest request) {
        
        ErrorResponse error = new ErrorResponse(
            HttpStatus.INTERNAL_SERVER_ERROR.value(),
            "Internal Server Error",
            "An unexpected error occurred. Please contact support.",
            request.getDescription(false).replace("uri=", "")
        );
        
        // Log the full exception (in real app, use proper logger)
        ex.printStackTrace();
        
        return new ResponseEntity<>(error, HttpStatus.INTERNAL_SERVER_ERROR);
    }
}