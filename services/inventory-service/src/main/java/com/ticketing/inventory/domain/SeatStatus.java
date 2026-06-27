package com.ticketing.inventory.domain;

/** Seat lifecycle. AVAILABLE -> RESERVED -> SOLD, or RESERVED -> AVAILABLE (released). */
public enum SeatStatus {
    AVAILABLE,
    RESERVED,
    SOLD
}
