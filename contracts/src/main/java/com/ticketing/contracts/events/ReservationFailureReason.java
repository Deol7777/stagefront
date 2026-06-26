package com.ticketing.contracts.events;

/** Why a seat reservation failed. Carried by {@link SeatReservationFailed}. */
public enum ReservationFailureReason {
    /** Another order already holds/owns this seat. */
    ALREADY_RESERVED,
    /** No seats left for the event. */
    SOLD_OUT
}
