package com.ticketing.order.outbox;

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
        // Snapshot the trace context of whoever is calling us — the HTTP request
        // that placed the order, or the Kafka listener handling the previous saga
        // step. It is stored ON THE ROW and restored by the relay at publish time.
        // See OutboxRelay and V3__outbox_trace_parent.sql for why that's needed.
        String traceParent = currentTraceParent();

        // If the caller didn't hand us a trace id, fall back to the real one from
        // the active trace. Makes the envelope's traceId agree with what Jaeger
        // and the logs show, instead of the null it used to carry.
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

    /**
     * Serialize the ACTIVE trace context into a W3C {@code traceparent} string.
     *
     * <p>We ask the {@link Propagator} to format it rather than concatenating the
     * ids by hand: the propagator is the same component that writes this header
     * onto Kafka records, so the format is guaranteed to match what the consumer
     * side will later parse — including the sampled flag, which decides whether
     * downstream spans get recorded at all.
     *
     * @return the traceparent value, or null when no trace is active (a
     *         background job, or a unit test with tracing switched off)
     */
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
