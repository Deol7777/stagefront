package com.ticketing.order.app;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.contracts.Money;
import com.ticketing.contracts.events.EventType;
import com.ticketing.contracts.events.OrderPlaced;
import com.ticketing.order.domain.OrderEntity;
import com.ticketing.order.domain.OrderRepository;
import com.ticketing.order.domain.OrderStatus;
import com.ticketing.order.outbox.OutboxEntity;
import com.ticketing.order.outbox.OutboxRepository;
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
    private final OutboxRepository outbox;
    private final ObjectMapper mapper;   // Boot-provided, already has Java-time support

    public OrderService(OrderRepository orders, OutboxRepository outbox, ObjectMapper mapper) {
        this.orders = orders;
        this.outbox = outbox;
        this.mapper = mapper;
    }

    /**
     * Place an order. The whole method is ONE transaction: the order row and the
     * outbox row commit together or not at all. We never write to Kafka here —
     * publishing is the relay's job, reading from the outbox. That separation is
     * exactly what prevents the dual-write problem (a DB commit with no event, or
     * an event with no DB commit).
     *
     * @return the new order's id
     */
    @Transactional
    public String placeOrder(PlaceOrderRequest req) {
        String orderId = UUID.randomUUID().toString();

        // 1. Business write: the order, in PENDING (saga starts now).
        var order = new OrderEntity(
                orderId, req.userId(), req.seatId(), req.eventScheduleId(),
                req.amount(), req.currency(), OrderStatus.PENDING, Instant.now());
        orders.save(order);

        // 2. Build the event and record it in the outbox (same transaction).
        var payload = new OrderPlaced(
                orderId, req.userId(), req.requestId(),
                req.seatId(), req.eventScheduleId(),
                new Money(req.amount(), req.currency()));
        var envelope = EventType.ORDER_PLACED.newEnvelope(/* traceId */ null, payload);

        var record = new OutboxEntity(
                UUID.randomUUID(),
                "Order",
                orderId,
                EventType.ORDER_PLACED.eventName(),
                EventType.ORDER_PLACED.topic(),
                payload.partitionKey(),
                toJson(envelope),
                EventType.ORDER_PLACED.schemaVersion(),
                Instant.now());
        outbox.save(record);

        return orderId;
    }

    /** Serialize the envelope to the JSON we store (and later publish verbatim). */
    private String toJson(Object envelope) {
        try {
            return mapper.writeValueAsString(envelope);
        } catch (JsonProcessingException e) {
            // Serialization failing here is a programming/contract bug, not a
            // transient error — fail the transaction loudly.
            throw new IllegalStateException("Failed to serialize event for outbox", e);
        }
    }
}
