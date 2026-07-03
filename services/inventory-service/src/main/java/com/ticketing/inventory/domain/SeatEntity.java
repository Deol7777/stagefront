package com.ticketing.inventory.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * A seat in the inventory (maps to the {@code seats} table). The reservation
 * state lives on the seat itself: who holds it and under which reservation id.
 */
@Entity
@Table(name = "seats")
public class SeatEntity {

    @Id
    @Column(name = "seat_id")
    private String seatId;

    @Column(name = "event_schedule_id", nullable = false)
    private String eventScheduleId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private SeatStatus status;

    @Column(name = "reserved_by_order")
    private String reservedByOrder;

    @Column(name = "reservation_id")
    private String reservationId;

    @Column(name = "updated_at")
    private Instant updatedAt;

    protected SeatEntity() {
    }

    public String getSeatId()          { return seatId; }
    public String getEventScheduleId() { return eventScheduleId; }
    public SeatStatus getStatus()      { return status; }
    public String getReservedByOrder() { return reservedByOrder; }
    public String getReservationId()   { return reservationId; }

    /** Reserve this seat for an order. Caller must hold the seat lock. */
    public void reserve(String orderId, String reservationId, Instant when) {
        this.status = SeatStatus.RESERVED;
        this.reservedByOrder = orderId;
        this.reservationId = reservationId;
        this.updatedAt = when;
    }

    /** Compensation: free the seat back to AVAILABLE (order cancelled / payment declined). */
    public void release(Instant when) {
        this.status = SeatStatus.AVAILABLE;
        this.reservedByOrder = null;
        this.reservationId = null;
        this.updatedAt = when;
    }

    /** Finalize: the order confirmed, the seat is now permanently SOLD. */
    public void markSold(Instant when) {
        this.status = SeatStatus.SOLD;
        this.updatedAt = when;
    }
}
