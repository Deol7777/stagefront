package com.ticketing.contracts;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.ticketing.contracts.events.EventType;
import com.ticketing.contracts.events.OrderPlaced;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves an event survives the JSON round-trip it will make on Kafka:
 * object -> JSON bytes (producer) -> object (consumer). If this holds, the
 * envelope + payload shapes are wire-safe.
 */
class EventSerializationTest {

    // Same config a service would use: register JavaTimeModule so Instant
    // serializes as a proper timestamp (not a broken numeric array).
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Test
    void orderPlaced_roundTripsThroughJson() throws Exception {
        // 1. Build the event the way a producer will.
        var payload = new OrderPlaced(
                "order-123", "user-42", "req-abc",
                "seat-A1", "show-9", new Money(new BigDecimal("49.99"), "USD"));
        var envelope = EventType.ORDER_PLACED.newEnvelope("trace-xyz", payload);

        // 2. Serialize to JSON (what gets written to the topic).
        String json = mapper.writeValueAsString(envelope);

        // Sanity: metadata the registry stamped is present.
        assertTrue(json.contains("\"eventType\":\"OrderPlaced\""), json);
        assertTrue(json.contains("\"schemaVersion\":1"), json);

        // 3. Deserialize back into a typed envelope (what a consumer does).
        //    Explicit type (not var): the JavaType overload returns the target
        //    type via inference, so the variable's declared type drives it.
        EventEnvelope<OrderPlaced> back = mapper.readValue(
                json,
                mapper.getTypeFactory().constructParametricType(
                        EventEnvelope.class, OrderPlaced.class));

        // 4. The payload must come back identical (record equality).
        assertEquals(payload, back.payload());
        assertEquals("OrderPlaced", back.eventType());
        assertEquals(1, back.schemaVersion());

        // 5. The contract's routing/idempotency keys survive too.
        assertEquals("order-123", back.partitionKey());
        assertEquals("order-123", back.payload().dedupKey());
    }
}
