package com.ticketing.inventory.cache;

import com.ticketing.inventory.domain.SeatEntity;

/**
 * An immutable, JSON-friendly projection of a seat — the shape we cache in Redis and
 * return from the read API.
 *
 * <p>Why not cache the {@link SeatEntity} directly? It's a JPA entity: mutable, tied to a
 * persistence context, and awkward to serialize. A flat record is a clean cache value —
 * and it deliberately omits the {@code updatedAt} {@link java.time.Instant} so the JSON
 * needs no time module. {@code status} is carried as a plain String for the same reason.
 */
public record SeatView(
        String seatId,
        String eventScheduleId,
        String status,
        String reservedByOrder,
        String reservationId) {

    /** Project a persistent seat into its cacheable view. */
    public static SeatView from(SeatEntity s) {
        return new SeatView(
                s.getSeatId(),
                s.getEventScheduleId(),
                s.getStatus().name(),
                s.getReservedByOrder(),
                s.getReservationId());
    }
}
