package com.example.orderservice.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.example.orderservice.dto.CreateOrderRequest;
import com.example.orderservice.dto.OrderItemResponse;
import com.example.orderservice.dto.OrderResponse;
import com.example.orderservice.model.Order;
import com.example.orderservice.model.OrderItem;
import com.example.orderservice.service.OrderService;

import jakarta.validation.Valid;

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;



@RestController
@RequestMapping("api/orders")
public class OrderController {
    private final OrderService orderService;
    public OrderController(OrderService orderService){
        this.orderService = orderService;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public OrderResponse createOrder(@Valid @RequestBody CreateOrderRequest request){
        Order order = orderService.createOrder(request);
        return mapToResponse(order);
    } 
    
    private OrderResponse mapToResponse(Order order) {
        OrderResponse response = new OrderResponse();
        response.setOrderId(order.getOrderId());
        response.setCustomerId(order.getCustomerId());
        response.setTotalAmount(order.getTotalAmount());
        response.setStatus(order.getStatus().name()); // Enum → String
        response.setCreatedAt(order.getCreatedAt());
        response.setItems(mapItemsToResponse(order.getItems()));
        return response;
    }   

    @GetMapping("/{id}")
    public OrderResponse getOrder(@PathVariable String id){
        Order order = orderService.getOrderById(id);
        return mapToResponse(order);
    }

    @GetMapping
    public List<OrderResponse> getAllOrders() {
        List<Order> orderList = orderService.getAllOrders();
        return orderList.stream()
            .map(this::mapToResponse)
            .collect(Collectors.toList());
    }

    private List<OrderItemResponse> mapItemsToResponse(List<OrderItem> items) {
        return items.stream()
            .map(item -> new OrderItemResponse(
                item.getProductId(),
                item.getProductName(),
                item.getQuantity(),
                item.getPrice()
            ))
            .collect(Collectors.toList());
    }
    


}
