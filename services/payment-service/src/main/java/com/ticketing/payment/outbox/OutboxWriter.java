package com.ticketing.payment.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.contracts.events.EventPayload;
import com.ticketing.contracts.events.EventType;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/** Serializes an event and appends it to the outbox (call inside a transaction). */
@Component
public class OutboxWriter {

    private final OutboxRepository outbox;
    private final ObjectMapper mapper;
    private final Tracer tracer;
    private final Propagator propagator;

    public OutboxWriter(OutboxRepository outbox, ObjectMapper mapper,
                        Tracer tracer, Propagator propagator) {
        this.outbox = outbox;
        this.mapper = mapper;
        this.tracer = tracer;
        this.propagator = propagator;
    }

    public void append(EventType type, String aggregateType, String aggregateId,
                       EventPayload payload, String traceId) {
        // Capture the caller's trace context so the relay can publish inside that
        // same trace later. See order-service OutboxWriter/OutboxRelay for why.
        String traceParent = currentTraceParent();
        String effectiveTraceId = (traceId != null) ? traceId : currentTraceId();

        var envelope = type.newEnvelope(effectiveTraceId, payload);
        var row = new OutboxEntity(
                UUID.randomUUID(),
                aggregateType,
                aggregateId,
                type.eventName(),
                type.topic(),
                payload.partitionKey(),
                toJson(envelope),
                type.schemaVersion(),
                Instant.now(),
                traceParent);
        outbox.save(row);
    }

    /** Active trace context as a W3C traceparent string, or null if untraced. */
    private String currentTraceParent() {
        var context = tracer.currentTraceContext().context();
        if (context == null) {
            return null;
        }
        Map<String, String> carrier = new HashMap<>();
        propagator.inject(context, carrier, Map::put);
        return carrier.get("traceparent");
    }

    private String currentTraceId() {
        var context = tracer.currentTraceContext().context();
        return (context != null) ? context.traceId() : null;
    }

    private String toJson(Object envelope) {
        try {
            return mapper.writeValueAsString(envelope);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize event for outbox", e);
        }
    }
}
