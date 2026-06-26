package com.ticketing.contracts.events;

import java.time.Instant;

/**
 * Saga step 4 (terminal happy). Produced by order-service once payment is
 * authorized. Consumed by notification-service (notify the buyer) and
 * inventory-service (flip the seat reservation → sold, the final state).
 *
 * <p>Topic: {@code orders.confirmed} · dedup key: {@code orderId} · schemaVersion 1.
 *
 * @param orderId     the confirmed order (partition key)
 * @param userId      buyer to notify
 * @param seatId      seat now sold
 * @param paymentId   the captured payment
 * @param confirmedAt when the order was confirmed
 */
public record OrderConfirmed(
        String orderId,
        String userId,
        String seatId,
        String paymentId,
        Instant confirmedAt) implements EventPayload {

    @Override
    public String dedupKey() {
        return orderId;
    }
}
