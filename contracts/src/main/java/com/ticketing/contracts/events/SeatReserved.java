package com.ticketing.contracts.events;

import com.ticketing.contracts.Money;

import java.time.Instant;

/**
 * Saga step 2. Produced by inventory-service after the seat is locked+reserved;
 * consumed by payment-service to authorize the charge.
 *
 * <p>Topic: {@code inventory.reserved} · dedup key: {@code reservationId} · schemaVersion 1.
 *
 * @param orderId       owning order (partition key)
 * @param seatId        the reserved seat
 * @param reservationId id of this reservation (payment dedups on this)
 * @param reservedUntil hold expiry — if payment doesn't complete by then the
 *                      reservation can be released (prevents stuck-forever holds)
 * @param amount        amount to authorize
 */
public record SeatReserved(
        String orderId,
        String seatId,
        String reservationId,
        Instant reservedUntil,
        Money amount) implements EventPayload {

    /** Payment dedups on the reservation id. */
    @Override
    public String dedupKey() {
        return reservationId;
    }
}
