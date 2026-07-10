package com.example.stockservice.service;

import com.example.stockservice.event.OrderCreatedEvent;
import com.example.stockservice.event.StockProcessedEvent;
import com.example.stockservice.event.StockProcessedEvent.StockItemResult;
import com.example.stockservice.event.StockProcessedEvent.StockStatus;
import com.example.stockservice.model.Product;
import com.example.stockservice.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
@RequiredArgsConstructor
public class StockService {

    private final ProductRepository productRepository;

    // Idempotency tracking
    private final Set<String> processedReservations = ConcurrentHashMap.newKeySet();
    private final Set<String> processedDecisions = ConcurrentHashMap.newKeySet();

    // Cache last response for idempotency
    private final Map<String, StockProcessedEvent> responseCache = new ConcurrentHashMap<>();

    /**
     * Reserve stock for ALL items in an order (Phase 1 of SAGA).
     *
     * ATOMIC OPERATION:
     * - Either ALL items are reserved, or NONE are reserved
     * - If any item fails, rollback all previously reserved items in this order
     *
     * This ensures consistency: we never end up with partial reservations.
     */
    @Transactional
    public StockProcessedEvent processOrderStock(OrderCreatedEvent event) {
        String orderId = event.getOrderId();

        // Idempotency check
        if (processedReservations.contains(orderId)) {
            log.warn("Duplicate stock reservation request for order: {}", orderId);
            return responseCache.getOrDefault(orderId, buildDefaultResponse(orderId));
        }

        log.info("Processing stock for order: {} | Items: {}", orderId, event.getItems().size());

        List<StockItemResult> itemResults = new ArrayList<>();
        List<Product> reservedProducts = new ArrayList<>();  // Track for rollback
        boolean allAvailable = true;
        StringBuilder failureReason = new StringBuilder();

        // Try to reserve ALL items
        for (OrderCreatedEvent.OrderItemEvent item : event.getItems()) {
            Product product = productRepository.findById(item.getProductId()).orElse(null);

            if (product == null) {
                log.warn("Product not found: {}", item.getProductId());
                allAvailable = false;
                failureReason.append("Product ").append(item.getProductId()).append(" not found; ");

                itemResults.add(StockItemResult.builder()
                    .productId(item.getProductId())
                    .productName(item.getProductName())
                    .quantity(item.getQuantity())
                    .available(false)
                    .build());
                break;  // Stop processing, will rollback
            }

            log.info("Product {} | Available: {} | Reserved: {} | Required: {}",
                     item.getProductId(), product.getAvailableItems(),
                     product.getReservedItems(), item.getQuantity());

            boolean reserved = product.reserve(item.getQuantity());

            if (!reserved) {
                log.warn("Insufficient stock for product: {} | Available: {} | Required: {}",
                         item.getProductId(), product.getAvailableItems(), item.getQuantity());
                allAvailable = false;
                failureReason.append("Product ").append(item.getProductId())
                             .append(" insufficient (available: ").append(product.getAvailableItems())
                             .append(", required: ").append(item.getQuantity()).append("); ");

                itemResults.add(StockItemResult.builder()
                    .productId(item.getProductId())
                    .productName(item.getProductName())
                    .quantity(item.getQuantity())
                    .available(false)
                    .build());
                break;  // Stop processing, will rollback
            }

            // Successfully reserved this item
            reservedProducts.add(product);
            itemResults.add(StockItemResult.builder()
                .productId(item.getProductId())
                .productName(item.getProductName())
                .quantity(item.getQuantity())
                .available(true)
                .build());

            log.info("Reserved {} units of product {}", item.getQuantity(), item.getProductId());
        }

        StockProcessedEvent response;

        if (allAvailable) {
            // All items reserved successfully - commit to database
            productRepository.saveAll(reservedProducts);
            productRepository.flush();
            processedReservations.add(orderId);

            log.info("Stock RESERVED for order: {} | All {} items available",
                     orderId, event.getItems().size());

            response = StockProcessedEvent.builder()
                .orderId(orderId)
                .customerId(event.getCustomerId())
                .items(itemResults)
                .status(StockStatus.ACCEPT)
                .build();

        } else {
            // At least one item failed - rollback ALL reserved items
            log.warn("Stock REJECTED for order: {} | Reason: {}", orderId, failureReason);

            for (Product product : reservedProducts) {
                // Find the quantity we reserved for this product
                int quantityToRollback = event.getItems().stream()
                    .filter(item -> item.getProductId().equals(product.getProductId()))
                    .findFirst()
                    .map(OrderCreatedEvent.OrderItemEvent::getQuantity)
                    .orElse(0);

                product.rollback(quantityToRollback);
                log.info("Rolled back {} units of product {}", quantityToRollback, product.getProductId());
            }

            productRepository.saveAll(reservedProducts);
            productRepository.flush();
            processedReservations.add(orderId);

            response = StockProcessedEvent.builder()
                .orderId(orderId)
                .customerId(event.getCustomerId())
                .items(itemResults)
                .status(StockStatus.REJECT)
                .reason(failureReason.toString().trim())
                .build();
        }

        // Cache response for idempotency
        responseCache.put(orderId, response);
        return response;
    }

    /**
     * Confirm stock reservation for ALL items (Phase 2 of SAGA - CONFIRMED path).
     * Moves items from reserved to permanently deducted.
     */
    @Transactional
    public void handleConfirm(OrderCreatedEvent event) {
        String orderId = event.getOrderId();

        if (processedDecisions.contains(orderId)) {
            log.warn("Duplicate confirm for order: {}", orderId);
            return;
        }

        log.info("Received CONFIRM decision for order: {} | Items: {}",
                 orderId, event.getItems().size());

        List<Product> productsToUpdate = new ArrayList<>();

        for (OrderCreatedEvent.OrderItemEvent item : event.getItems()) {
            Product product = productRepository.findById(item.getProductId()).orElse(null);
            if (product != null) {
                product.confirm(item.getQuantity());
                productsToUpdate.add(product);

                log.info("Confirmed {} units of product {} | Reserved now: {}",
                         item.getQuantity(), item.getProductId(), product.getReservedItems());
            }
        }

        productRepository.saveAll(productsToUpdate);
        productRepository.flush();
        processedDecisions.add(orderId);

        log.info("Stock CONFIRMED for order: {} | All {} items deducted",
                 orderId, event.getItems().size());
    }

    /**
     * Rollback stock reservation for ALL items (Phase 2 of SAGA - REJECTED/ROLLBACK path).
     * Returns items from reserved back to available pool.
     */
    @Transactional
    public void handleRollback(OrderCreatedEvent event) {
        String orderId = event.getOrderId();

        if (processedDecisions.contains(orderId)) {
            log.warn("Duplicate rollback for order: {}", orderId);
            return;
        }

        log.info("Received ROLLBACK decision for order: {} | Items: {}",
                 orderId, event.getItems().size());

        List<Product> productsToUpdate = new ArrayList<>();

        for (OrderCreatedEvent.OrderItemEvent item : event.getItems()) {
            Product product = productRepository.findById(item.getProductId()).orElse(null);
            if (product != null) {
                product.rollback(item.getQuantity());
                productsToUpdate.add(product);

                log.info("Rolled back {} units of product {} | Available now: {}",
                         item.getQuantity(), item.getProductId(), product.getAvailableItems());
            }
        }

        productRepository.saveAll(productsToUpdate);
        productRepository.flush();
        processedDecisions.add(orderId);

        log.info("Stock ROLLED BACK for order: {} | All {} items returned to inventory",
                 orderId, event.getItems().size());
    }

    private StockProcessedEvent buildDefaultResponse(String orderId) {
        return StockProcessedEvent.builder()
            .orderId(orderId)
            .status(StockStatus.ACCEPT)
            .items(Collections.emptyList())
            .build();
    }
}
