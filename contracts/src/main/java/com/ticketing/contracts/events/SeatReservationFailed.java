package com.ticketing.contracts.events;

/**
 * Failure branch A. Produced by inventory-service when the seat can't be
 * reserved; consumed by order-service to cancel the order. No compensation
 * needed beyond cancelling (nothing was held or charged).
 *
 * <p>Topic: {@code inventory.reservation-failed} · dedup key: {@code orderId} · schemaVersion 1.
 *
 * @param orderId owning order (partition key)
 * @param seatId  the seat that couldn't be reserved
 * @param reason  why it failed
 */
public record SeatReservationFailed(
        String orderId,
        String seatId,
        ReservationFailureReason reason) implements EventPayload {

    @Override
    public String dedupKey() {
        return orderId;
    }
}
