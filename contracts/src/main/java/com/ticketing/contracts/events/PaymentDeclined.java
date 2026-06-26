package com.ticketing.contracts.events;

/**
 * Failure branch B. Produced by payment-service when a charge is declined.
 * Consumed by BOTH inventory-service (release the reserved seat — compensation)
 * and order-service (cancel the order). No refund (nothing was captured).
 *
 * <p>Topic: {@code payments.declined} · dedup key: {@code paymentId} · schemaVersion 1.
 *
 * @param orderId   owning order (partition key)
 * @param paymentId the declined payment attempt
 * @param reason    why it was declined
 */
public record PaymentDeclined(
        String orderId,
        String paymentId,
        PaymentDeclineReason reason) implements EventPayload {

    @Override
    public String dedupKey() {
        return paymentId;
    }
}
