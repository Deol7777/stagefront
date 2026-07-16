package com.ticketing.contracts.events;

/** Why a payment was declined. Carried by {@link PaymentDeclined}. */
public enum PaymentDeclineReason {
    INSUFFICIENT_FUNDS,
    CARD_DECLINED,

    /**
     * The payment gateway was unreachable and the circuit breaker gave up on it
     * (breaker OPEN or retries exhausted). This is a graceful decline, not a crash:
     * rather than hang or dead-letter the message, payment-service fast-fails the
     * charge and lets the saga compensate (release seat, cancel order). Added in
     * schemaVersion 1 as a NEW enum value — see docs/events.md for the schema-
     * evolution note (old consumers must tolerate unknown reason values).
     */
    GATEWAY_UNAVAILABLE
}
