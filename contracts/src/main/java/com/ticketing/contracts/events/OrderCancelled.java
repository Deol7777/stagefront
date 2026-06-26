package com.ticketing.contracts.events;

import java.time.Instant;

/**
 * Terminal failure state. Produced by order-service when an order is cancelled
 * (seat reservation failed, payment declined, or post-payment cancellation).
 * Consumed by notification-service (notify the buyer) and inventory-service
 * (release the seat if still held).
 *
 * <p>Topic: {@code orders.cancelled} · dedup key: {@code orderId} · schemaVersion 1.
 *
 * @param orderId     the cancelled order (partition key)
 * @param userId      buyer to notify
 * @param seatId      seat to release (if held)
 * @param reason      free-text/justification for the cancellation
 * @param cancelledAt when it was cancelled
 */
public record OrderCancelled(
        String orderId,
        String userId,
        String seatId,
        String reason,
        Instant cancelledAt) implements EventPayload {

    @Override
    public String dedupKey() {
        return orderId;
    }
}
