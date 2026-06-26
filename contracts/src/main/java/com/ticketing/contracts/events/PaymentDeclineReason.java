package com.ticketing.contracts.events;

/** Why a payment was declined. Carried by {@link PaymentDeclined}. */
public enum PaymentDeclineReason {
    INSUFFICIENT_FUNDS,
    CARD_DECLINED
}
