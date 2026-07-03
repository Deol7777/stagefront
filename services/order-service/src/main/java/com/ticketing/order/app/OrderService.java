package com.ticketing.order.app;

import com.ticketing.contracts.Money;
import com.ticketing.contracts.events.EventType;
import com.ticketing.contracts.events.OrderPlaced;
import com.ticketing.order.domain.OrderEntity;
import com.ticketing.order.domain.OrderRepository;
import com.ticketing.order.domain.OrderStatus;
import com.ticketing.order.outbox.OutboxWriter;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

/**
 * Application service for placing orders. Home of the transactional outbox write.
 */
@Service
public class OrderService {

    private final OrderRepository orders;
    private final OutboxWriter outbox;

    public OrderService(OrderRepository orders, OutboxWriter outbox) {
        this.orders = orders;
        this.outbox = outbox;
    }

    /**
     * Place an order. The whole method is ONE transaction: the order row and the
     * outbox row commit together or not at all. We never write to Kafka here —
     * publishing is the relay's job, reading from the outbox. That separation is
     * exactly what prevents the dual-write problem.
     *
     * @return the new order's id
     */
    @Transactional
    public String placeOrder(PlaceOrderRequest req) {
        String orderId = UUID.randomUUID().toString();

        var order = new OrderEntity(
                orderId, req.userId(), req.seatId(), req.eventScheduleId(),
                req.amount(), req.currency(), OrderStatus.PENDING, Instant.now());
        orders.save(order);

        var payload = new OrderPlaced(
                orderId, req.userId(), req.requestId(),
                req.seatId(), req.eventScheduleId(),
                new Money(req.amount(), req.currency()));
        outbox.append(EventType.ORDER_PLACED, "Order", orderId, payload, /* traceId */ null);

        return orderId;
    }
}
