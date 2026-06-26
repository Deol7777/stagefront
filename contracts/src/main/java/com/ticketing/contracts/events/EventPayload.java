package com.ticketing.contracts.events;

/**
 * Marker for every event payload, and the place we encode two cross-cutting
 * routing/correctness concerns that EVERY event must answer:
 *
 * <ul>
 *   <li><b>partition key</b> — which Kafka partition the event goes to. We use
 *       {@code orderId} for all events so that every event for one order lands
 *       in the same partition and is therefore processed <i>in order</i>.</li>
 *   <li><b>dedup key</b> — the value an idempotent consumer stores to recognise
 *       a duplicate and no-op. At-least-once delivery means duplicates WILL
 *       arrive; this key is how we make reprocessing safe. It differs per event
 *       (orderId / reservationId / paymentId / refundId — see docs/events.md).</li>
 * </ul>
 *
 * <p>This is a {@code sealed} interface: the set of event types is closed and
 * known here. Adding an event means adding it to {@code permits} — which makes
 * the compiler force every exhaustive {@code switch} over events to handle it.
 * All permitted types live in this same package (required for a sealed type in
 * the unnamed module).
 */
public sealed interface EventPayload
        permits OrderPlaced, SeatReserved, SeatReservationFailed,
                PaymentAuthorized, PaymentDeclined, PaymentRefunded,
                OrderConfirmed, OrderCancelled {

    /** The order this event belongs to. Present on every event. */
    String orderId();

    /** Idempotency key the consumer dedups on (see docs/events.md per event). */
    String dedupKey();

    /** Kafka partition key. orderId everywhere → per-order ordering. */
    default String partitionKey() {
        return orderId();
    }
}
