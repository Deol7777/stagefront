package com.ticketing.contracts;

/**
 * Kafka topic names — one constant per event topic, matching docs/events.md.
 *
 * <p>Centralised so a producer and its consumers reference the SAME string;
 * a typo'd topic name is a silent bug (auto-create is disabled in the broker,
 * but a mismatched name still just goes nowhere). Keeping them here makes the
 * topic list reviewable in one place.
 */
public final class Topics {

    public static final String ORDERS_PLACED = "orders.placed";
    public static final String INVENTORY_RESERVED = "inventory.reserved";
    public static final String INVENTORY_RESERVATION_FAILED = "inventory.reservation-failed";
    public static final String PAYMENTS_AUTHORIZED = "payments.authorized";
    public static final String PAYMENTS_DECLINED = "payments.declined";
    public static final String PAYMENTS_REFUNDED = "payments.refunded";
    public static final String ORDERS_CONFIRMED = "orders.confirmed";
    public static final String ORDERS_CANCELLED = "orders.cancelled";

    /** Dead-letter topic name for a given source topic (convention: suffix ".DLQ"). */
    public static String dlq(String sourceTopic) {
        return sourceTopic + ".DLQ";
    }

    private Topics() {
        // Constants holder — never instantiated.
    }
}
