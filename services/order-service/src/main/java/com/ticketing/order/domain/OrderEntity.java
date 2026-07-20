package com.ticketing.order.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * The order aggregate this service owns (maps to the {@code orders} table).
 *
 * <p>JPA needs a no-arg constructor and is happiest with field access, so the
 * fields are private with getters; the public constructor is what application
 * code uses to create a new order.
 */
@Entity
@Table(name = "orders")
public class OrderEntity {

    @Id
    private String id;

    @Column(name = "user_id", nullable = false)
    private String userId;

    @Column(name = "seat_id", nullable = false)
    private String seatId;

    @Column(name = "event_schedule_id", nullable = false)
    private String eventScheduleId;

    @Column(nullable = false)
    private BigDecimal amount;

    @Column(nullable = false)
    private String currency;

    @Enumerated(EnumType.STRING)          // store "PENDING", not an ordinal int
    @Column(nullable = false)
    private OrderStatus status;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    /**
     * Client-supplied idempotency token for the POST that created this order.
     * Unique (see V4 migration) so a retried request cannot create a second
     * order. Nullable: pre-migration rows, and clients that don't send one.
     */
    @Column(name = "request_id")
    private String requestId;

    /** Required by JPA — do not use directly. */
    protected OrderEntity() {
    }

    public OrderEntity(String id, String userId, String seatId, String eventScheduleId,
                       BigDecimal amount, String currency, OrderStatus status, Instant createdAt,
                       String requestId) {
        this.id = id;
        this.userId = userId;
        this.seatId = seatId;
        this.eventScheduleId = eventScheduleId;
        this.amount = amount;
        this.currency = currency;
        this.status = status;
        this.createdAt = createdAt;
        this.requestId = requestId;
    }

    public String getRequestId()        { return requestId; }
    public String getId()               { return id; }
    public String getUserId()           { return userId; }
    public String getSeatId()           { return seatId; }
    public String getEventScheduleId()  { return eventScheduleId; }
    public BigDecimal getAmount()       { return amount; }
    public String getCurrency()         { return currency; }
    public OrderStatus getStatus()      { return status; }
    public Instant getCreatedAt()       { return createdAt; }

    /** Status transitions happen via the saga (later steps). */
    public void setStatus(OrderStatus status) {
        this.status = status;
    }
}
