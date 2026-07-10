package com.example.stockservice.config;

import com.example.stockservice.model.Product;
import com.example.stockservice.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.List;

/**
 * Initialize test product data on application startup
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class DataInitializer implements CommandLineRunner {

    private final ProductRepository productRepository;

    @Override
    public void run(String... args) {
        log.info("Initializing test product data...");

        List<Product> products = List.of(
            new Product("PROD-001", "Laptop", 100, 0, new BigDecimal("1200.00")),
            new Product("PROD-002", "Mouse", 200, 0, new BigDecimal("25.00")),
            new Product("PROD-003", "Keyboard", 150, 0, new BigDecimal("75.00")),
            new Product("PROD-004", "Monitor", 80, 0, new BigDecimal("300.00")),
            new Product("PROD-005", "Headphones", 120, 0, new BigDecimal("150.00")),
            new Product("PROD-006", "USB Cable", 500, 0, new BigDecimal("10.00")),
            new Product("PROD-007", "Webcam", 60, 0, new BigDecimal("90.00")),
            new Product("PROD-008", "Desk Lamp", 75, 0, new BigDecimal("45.00")),
            new Product("PROD-009", "Phone Charger", 300, 0, new BigDecimal("20.00")),
            new Product("PROD-010", "External HDD", 50, 0, new BigDecimal("120.00"))
        );

        productRepository.saveAll(products);
        log.info("Successfully initialized {} products", products.size());

        // Log product inventory
        products.forEach(p ->
            log.info("Product: {} | Name: {} | Price: {} | Available: {}",
                     p.getProductId(), p.getProductName(), p.getPrice(), p.getAvailableItems())
        );
    }
}
