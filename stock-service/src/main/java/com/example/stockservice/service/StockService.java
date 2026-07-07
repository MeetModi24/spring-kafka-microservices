package com.example.stockservice.service;

import com.example.stockservice.event.StockProcessedEvent;
import com.example.stockservice.event.StockProcessedEvent.StockStatus;
import com.example.stockservice.model.Product;
import com.example.stockservice.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
@RequiredArgsConstructor
public class StockService {
    
    private final ProductRepository productRepository;
    
    // Idempotency tracking
    private final Set<String> processedReservations = ConcurrentHashMap.newKeySet();
    private final Set<String> processedDecisions = ConcurrentHashMap.newKeySet();
    
    /**
     * Reserve stock for an order (Phase 1 of SAGA).
     */
    @Transactional
    public StockProcessedEvent processOrderStock(String orderId, String productId, int quantity) {
        // Idempotency check
        if (processedReservations.contains(orderId)) {
            log.warn("Duplicate stock reservation request for order: {}", orderId);
            return buildLastResponse(orderId);
        }
        
        log.info("Processing stock for order: {} | Product: {} | Quantity: {}", 
                 orderId, productId, quantity);
        
        Product product = productRepository.findById(productId).orElse(null);
        
        if (product == null) {
            log.warn("Product not found: {}", productId);
            processedReservations.add(orderId);
            return StockProcessedEvent.builder()
                .orderId(orderId)
                .productId(productId)
                .quantity(quantity)
                .status(StockStatus.REJECT)
                .reason("Product not found")
                .build();
        }
        
        log.info("Product {} | Available: {} | Reserved: {} | Required: {}", 
                 productId, product.getAvailableItems(), product.getReservedItems(), quantity);
        
        boolean reserved = product.reserve(quantity);
        
        if (reserved) {
            productRepository.save(product);
            processedReservations.add(orderId);
            
            log.info("Stock RESERVED for order: {} | Product: {} | Quantity: {} | Remaining: {}", 
                     orderId, productId, quantity, product.getAvailableItems());
            
            return StockProcessedEvent.builder()
                .orderId(orderId)
                .productId(productId)
                .quantity(quantity)
                .status(StockStatus.ACCEPT)
                .build();
        } else {
            processedReservations.add(orderId);
            
            log.warn("Stock REJECTED for order: {} | Insufficient stock | Available: {} | Required: {}", 
                     orderId, product.getAvailableItems(), quantity);
            
            return StockProcessedEvent.builder()
                .orderId(orderId)
                .productId(productId)
                .quantity(quantity)
                .status(StockStatus.REJECT)
                .reason("Insufficient stock. Available: " + product.getAvailableItems())
                .build();
        }
    }
    
    /**
     * Confirm stock reservation (Phase 2 of SAGA - CONFIRMED path).
     */
    @Transactional
    public void handleConfirm(String orderId, String productId, int quantity) {
        if (processedDecisions.contains(orderId)) {
            log.warn("Duplicate confirm for order: {}", orderId);
            return;
        }
        
        log.info("Received CONFIRM decision for order: {}", orderId);
        
        Product product = productRepository.findById(productId).orElse(null);
        if (product != null) {
            product.confirm(quantity);
            productRepository.save(product);
            processedDecisions.add(orderId);
            
            log.info("Stock CONFIRMED for order: {} | Product: {} | Deducted: {} | Reserved now: {}", 
                     orderId, productId, quantity, product.getReservedItems());
        }
    }
    
    /**
     * Rollback stock reservation (Phase 2 of SAGA - REJECTED/ROLLBACK path).
     */
    @Transactional
    public void handleRollback(String orderId, String productId, int quantity) {
        if (processedDecisions.contains(orderId)) {
            log.warn("Duplicate rollback for order: {}", orderId);
            return;
        }
        
        log.info("Received ROLLBACK decision for order: {}", orderId);
        
        Product product = productRepository.findById(productId).orElse(null);
        if (product != null) {
            product.rollback(quantity);
            productRepository.save(product);
            processedDecisions.add(orderId);
            
            log.info("Stock ROLLED BACK for order: {} | Product: {} | Returned: {} | Available now: {}", 
                     orderId, productId, quantity, product.getAvailableItems());
        }
    }
    
    private StockProcessedEvent buildLastResponse(String orderId) {
        // Return cached response (implementation detail - simplified)
        return StockProcessedEvent.builder()
            .orderId(orderId)
            .status(StockStatus.ACCEPT)
            .build();
    }
}