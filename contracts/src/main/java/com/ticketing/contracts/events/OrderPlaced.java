package com.ticketing.contracts.events;

import com.ticketing.contracts.Money;

/**
 * Saga step 1. Produced by order-service when a ticket order is accepted;
 * consumed by inventory-service to reserve the seat.
 *
 * <p>Topic: {@code orders.placed} · dedup key: {@code orderId} · schemaVersion 1.
 *
 * @param orderId         the new order's id (also the partition key)
 * @param userId          who is buying
 * @param requestId       the client's idempotency token for the original request
 *                        (lets order-service itself dedup retried POSTs)
 * @param seatId          the specific seat requested
 * @param eventScheduleId which show/date the seat belongs to
 * @param amount          price to be charged
 */
public record OrderPlaced(
        String orderId,
        String userId,
        String requestId,
        String seatId,
        String eventScheduleId,
        Money amount) implements EventPayload {

    /** Inventory dedups on the order id (one reservation per order). */
    @Override
    public String dedupKey() {
        return orderId;
    }
}
