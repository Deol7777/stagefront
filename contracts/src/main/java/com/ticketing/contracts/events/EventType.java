package com.ticketing.contracts.events;

import com.ticketing.contracts.EventEnvelope;
import com.ticketing.contracts.Topics;

/**
 * Single registry tying each event to its canonical name, current schema
 * version, and Kafka topic. One place to look up "what is this event called,
 * what version are we on, where does it go" — instead of scattering literals.
 *
 * <p>Producers use {@link #newEnvelope} so the name + version are always correct
 * and consistent with this table (and with docs/events.md).
 */
public enum EventType {

    ORDER_PLACED("OrderPlaced", 1, Topics.ORDERS_PLACED),
    SEAT_RESERVED("SeatReserved", 1, Topics.INVENTORY_RESERVED),
    SEAT_RESERVATION_FAILED("SeatReservationFailed", 1, Topics.INVENTORY_RESERVATION_FAILED),
    PAYMENT_AUTHORIZED("PaymentAuthorized", 1, Topics.PAYMENTS_AUTHORIZED),
    PAYMENT_DECLINED("PaymentDeclined", 1, Topics.PAYMENTS_DECLINED),
    PAYMENT_REFUNDED("PaymentRefunded", 1, Topics.PAYMENTS_REFUNDED),
    ORDER_CONFIRMED("OrderConfirmed", 1, Topics.ORDERS_CONFIRMED),
    ORDER_CANCELLED("OrderCancelled", 1, Topics.ORDERS_CANCELLED);

    private final String eventName;
    private final int schemaVersion;
    private final String topic;

    EventType(String eventName, int schemaVersion, String topic) {
        this.eventName = eventName;
        this.schemaVersion = schemaVersion;
        this.topic = topic;
    }

    public String eventName()  { return eventName; }
    public int schemaVersion() { return schemaVersion; }
    public String topic()      { return topic; }

    /** DLQ topic for this event's consumers (source topic + ".DLQ"). */
    public String dlqTopic() {
        return Topics.dlq(topic);
    }

    /**
     * Wrap a payload in an envelope stamped with this event's name + current
     * schema version (and a fresh id/timestamp). The one correct way to build
     * an event to publish.
     */
    public <T extends EventPayload> EventEnvelope<T> newEnvelope(String traceId, T payload) {
        return EventEnvelope.of(eventName, schemaVersion, traceId, payload);
    }
}
