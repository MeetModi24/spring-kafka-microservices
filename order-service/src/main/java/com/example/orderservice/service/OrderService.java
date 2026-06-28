package com.example.orderservice.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Service;

import com.example.orderservice.dto.CreateOrderRequest;
import com.example.orderservice.model.Order;
import com.example.orderservice.model.OrderItem;

@Service
public class OrderService {
    private final Map<String, Order> orderStore = new ConcurrentHashMap<>();

    public Order createOrder(CreateOrderRequest request){ // customerId, items
        // Map DTO items -> Domain items
        List<OrderItem> orderItems = request.getItems().stream()
            .map(dto -> new OrderItem(
                dto.getProductId(),
                dto.getProductName(),
                dto.getQuantity(),
                dto.getPrice()
            ))
            .toList();

        // Create Order with domain items
        Order order = new Order(request.getCustomerId(), orderItems);
        order.setOrderId(UUID.randomUUID().toString());

        if(!order.isValid()){
            throw new IllegalArgumentException("Order validation failed");
        }
        orderStore.put(order.getOrderId(), order);
        return order;
    }
    
    public Order getOrderById(String Id){
        Order order = orderStore.get(Id);
        if(order == null){
            throw new IllegalArgumentException("Id not found"); 
        }
        return order;
    }

    public List<Order> getAllOrders(){
        return new ArrayList<>(orderStore.values());
    }

}
