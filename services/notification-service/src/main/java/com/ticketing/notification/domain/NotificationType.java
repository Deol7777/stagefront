package com.ticketing.notification.domain;

/** Which terminal saga event produced the notification. */
public enum NotificationType {
    ORDER_CONFIRMED,
    ORDER_CANCELLED,
    PAYMENT_REFUNDED
}
