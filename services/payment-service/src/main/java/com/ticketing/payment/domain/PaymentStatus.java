package com.ticketing.payment.domain;

/** Payment lifecycle. AUTHORIZED or DECLINED now; REFUNDED used by compensation later. */
public enum PaymentStatus {
    AUTHORIZED,
    DECLINED,
    REFUNDED
}
