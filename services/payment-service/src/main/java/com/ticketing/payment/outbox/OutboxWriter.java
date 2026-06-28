package com.ticketing.payment.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.contracts.events.EventPayload;
import com.ticketing.contracts.events.EventType;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

/** Serializes an event and appends it to the outbox (call inside a transaction). */
@Component
public class OutboxWriter {

    private final OutboxRepository outbox;
    private final ObjectMapper mapper;

    public OutboxWriter(OutboxRepository outbox, ObjectMapper mapper) {
        this.outbox = outbox;
        this.mapper = mapper;
    }

    public void append(EventType type, String aggregateType, String aggregateId,
                       EventPayload payload, String traceId) {
        var envelope = type.newEnvelope(traceId, payload);
        var row = new OutboxEntity(
                UUID.randomUUID(),
                aggregateType,
                aggregateId,
                type.eventName(),
                type.topic(),
                payload.partitionKey(),
                toJson(envelope),
                type.schemaVersion(),
                Instant.now());
        outbox.save(row);
    }

    private String toJson(Object envelope) {
        try {
            return mapper.writeValueAsString(envelope);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize event for outbox", e);
        }
    }
}
