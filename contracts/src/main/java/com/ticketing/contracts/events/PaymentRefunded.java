package com.ticketing.contracts.events;

import com.ticketing.contracts.Money;

import java.time.Instant;

/**
 * Compensation event (failure branch C). Produced by payment-service when an
 * already-authorized charge must be reversed because the order is being
 * cancelled after payment. Consumed by order-service and notification-service.
 *
 * <p>Topic: {@code payments.refunded} · dedup key: {@code refundId} · schemaVersion 1.
 *
 * @param orderId    owning order (partition key)
 * @param paymentId  the original payment being reversed
 * @param refundId   id of this refund (dedup key — a refund must not double-fire)
 * @param amount     amount refunded
 * @param refundedAt when the refund was issued
 */
public record PaymentRefunded(
        String orderId,
        String paymentId,
        String refundId,
        Money amount,
        Instant refundedAt) implements EventPayload {

    @Override
    public String dedupKey() {
        return refundId;
    }
}
