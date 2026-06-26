package com.ticketing.contracts;

import com.ticketing.contracts.events.EventPayload;

import java.time.Instant;
import java.util.UUID;

/**
 * The common wrapper around every event published to Kafka. The metadata lives
 * here; the event-specific data lives in {@code payload}. This is the actual
 * JSON shape on the wire (see the "Common envelope" table in docs/events.md).
 *
 * <p>Generic over the payload type {@code T} so a consumer that expects, say,
 * {@code OrderPlaced} can deserialize straight into
 * {@code EventEnvelope<OrderPlaced>} and get a typed payload — no casting.
 *
 * @param eventId       unique id for THIS event instance (a redelivered duplicate
 *                      keeps the same id; the dedup key is what consumers compare)
 * @param eventType     e.g. "OrderPlaced" — lets generic tooling route without reflection
 * @param schemaVersion bumped on a breaking shape change so consumers can adapt
 * @param occurredAt    when the event happened (UTC)
 * @param traceId       OpenTelemetry trace id, propagated so one order's hops
 *                      across services stitch into a single distributed trace
 * @param payload       the event-specific body
 */
public record EventEnvelope<T extends EventPayload>(
        UUID eventId,
        String eventType,
        int schemaVersion,
        Instant occurredAt,
        String traceId,
        T payload) {

    /**
     * Factory for producing a fresh event: stamps a random {@code eventId} and
     * the current time so callers only supply what they actually know.
     *
     * @param eventType     the event name (e.g. "OrderPlaced")
     * @param schemaVersion current schema version for that event
     * @param traceId       current trace id to propagate (may be null if no trace)
     * @param payload       the event body
     */
    public static <T extends EventPayload> EventEnvelope<T> of(
            String eventType, int schemaVersion, String traceId, T payload) {
        return new EventEnvelope<>(
                UUID.randomUUID(), eventType, schemaVersion, Instant.now(), traceId, payload);
    }

    /** Convenience: the Kafka partition key for this event (delegates to the payload). */
    public String partitionKey() {
        return payload.partitionKey();
    }
}
