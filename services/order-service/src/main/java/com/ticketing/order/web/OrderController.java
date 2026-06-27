package com.ticketing.order.web;

import com.ticketing.order.app.OrderService;
import com.ticketing.order.app.PlaceOrderRequest;
import com.ticketing.order.domain.OrderEntity;
import com.ticketing.order.domain.OrderRepository;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * REST API for orders. Thin: place an order, and read orders back (the latter
 * feeds the read-only debugging dashboard). Status changes happen via the saga,
 * not via the API.
 */
@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderService orderService;
    private final OrderRepository orders;

    public OrderController(OrderService orderService, OrderRepository orders) {
        this.orderService = orderService;
        this.orders = orders;
    }

    /** POST /api/orders — place an order. Returns 202 (the saga runs async). */
    @PostMapping
    public ResponseEntity<Map<String, String>> place(@RequestBody PlaceOrderRequest req) {
        String orderId = orderService.placeOrder(req);
        // 202 Accepted: we've recorded the order + queued the event; the rest of
        // the saga (reserve seat, pay, confirm) happens asynchronously.
        return ResponseEntity.accepted().body(Map.of("orderId", orderId));
    }

    /** GET /api/orders/{id} — one order (for the dashboard / debugging). */
    @GetMapping("/{id}")
    public ResponseEntity<OrderEntity> get(@PathVariable String id) {
        return orders.findById(id)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND).build());
    }

    /** GET /api/orders — list all orders. */
    @GetMapping
    public List<OrderEntity> list() {
        return orders.findAll();
    }
}
