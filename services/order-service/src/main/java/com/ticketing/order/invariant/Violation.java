package com.ticketing.order.invariant;

/**
 * One detected inconsistency between services.
 *
 * @param rule     which invariant was broken (stable identifier, safe to alert on)
 * @param orderId  the order the inconsistency is about
 * @param detail   human-readable specifics, e.g. what each service actually said
 */
public record Violation(Rule rule, String orderId, String detail) {

    /**
     * The invariants we check. Each one is a statement that must hold across
     * service boundaries ONCE THE SAGA HAS SETTLED — never while it is in flight.
     */
    public enum Rule {
        /** Order says CONFIRMED, but inventory does not have the seat sold to it. */
        CONFIRMED_ORDER_WITHOUT_SOLD_SEAT,
        /** Order says CONFIRMED, but payment has no AUTHORIZED payment for it. */
        CONFIRMED_ORDER_WITHOUT_AUTHORIZED_PAYMENT,
        /** Order was CANCELLED, but inventory still holds the seat for it (leaked seat). */
        CANCELLED_ORDER_STILL_HOLDING_SEAT,
        /** Order was CANCELLED, money was taken, and it was never refunded. */
        CANCELLED_ORDER_WITH_UNREFUNDED_PAYMENT,
        /** Saga never reached a terminal state — stuck PENDING well past the grace period. */
        STUCK_PENDING_ORDER
    }
}
