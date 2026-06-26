package com.ticketing.contracts.events;

import com.ticketing.contracts.Money;

import java.time.Instant;

/**
 * Saga step 3 (happy). Produced by payment-service on a successful charge;
 * consumed by order-service to confirm the order.
 *
 * <p>Topic: {@code payments.authorized} · dedup key: {@code paymentId} · schemaVersion 1.
 *
 * @param orderId      owning order (partition key)
 * @param paymentId    id of the payment (order-service dedups on this)
 * @param amount       amount authorized
 * @param authorizedAt when authorization succeeded
 */
public record PaymentAuthorized(
        String orderId,
        String paymentId,
        Money amount,
        Instant authorizedAt) implements EventPayload {

    @Override
    public String dedupKey() {
        return paymentId;
    }
}
