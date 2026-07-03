package com.ticketing.payment.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;

/** A payment attempt for an order's reservation (maps to {@code payments}). */
@Entity
@Table(name = "payments")
public class PaymentEntity {

    @Id
    @Column(name = "payment_id")
    private String paymentId;

    @Column(name = "order_id", nullable = false)
    private String orderId;

    @Column(name = "reservation_id", nullable = false)
    private String reservationId;

    @Column(nullable = false)
    private BigDecimal amount;

    @Column(nullable = false)
    private String currency;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private PaymentStatus status;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected PaymentEntity() {
    }

    public PaymentEntity(String paymentId, String orderId, String reservationId,
                         BigDecimal amount, String currency, PaymentStatus status, Instant createdAt) {
        this.paymentId = paymentId;
        this.orderId = orderId;
        this.reservationId = reservationId;
        this.amount = amount;
        this.currency = currency;
        this.status = status;
        this.createdAt = createdAt;
    }

    public String getPaymentId()  { return paymentId; }
    public String getOrderId()    { return orderId; }
    public BigDecimal getAmount() { return amount; }
    public String getCurrency()   { return currency; }
    public PaymentStatus getStatus() { return status; }

    /** Compensation: reverse an authorized charge. */
    public void markRefunded() {
        this.status = PaymentStatus.REFUNDED;
    }
}
