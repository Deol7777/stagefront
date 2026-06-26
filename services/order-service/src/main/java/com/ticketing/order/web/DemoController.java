package com.ticketing.order.web;

import com.ticketing.contracts.EventEnvelope;
import com.ticketing.contracts.Money;
import com.ticketing.contracts.events.EventType;
import com.ticketing.contracts.events.OrderPlaced;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;

/**
 * TEMPORARY debug endpoint — lets you SEE the event contract serialized over a
 * live HTTP call, before any Kafka/DB wiring exists. It builds a sample
 * {@code OrderPlaced} envelope exactly as a producer would and returns it as
 * JSON (Spring uses Jackson, the same serialization that will hit Kafka).
 *
 * <p>This is throwaway scaffolding to prove the wiring end-to-end. It will be
 * replaced by the real "place order" endpoint (which persists an order + outbox
 * row) when the outbox step lands.
 */
@RestController
public class DemoController {

    /**
     * GET /api/demo/sample-event — returns a freshly-built OrderPlaced envelope.
     * Spring serializes the record to JSON; Instant renders as ISO-8601 because
     * Spring Boot auto-configures the Jackson Java-time module.
     */
    @GetMapping("/api/demo/sample-event")
    public EventEnvelope<OrderPlaced> sampleEvent() {
        var payload = new OrderPlaced(
                "order-123",            // orderId (also the partition key)
                "user-42",              // userId
                "req-abc",              // client request idempotency token
                "seat-A1",              // seatId
                "show-9",               // eventScheduleId
                new Money(new BigDecimal("49.99"), "USD"));

        // Build via the registry so eventType + schemaVersion are correct.
        return EventType.ORDER_PLACED.newEnvelope("trace-xyz", payload);
    }
}
