package com.ticketing.order.domain;

/** Lifecycle states of an order. Stored as text in the DB (readable, stable). */
public enum OrderStatus {
    PENDING,    // placed, saga in flight
    CONFIRMED,  // payment authorized, seat sold
    CANCELLED   // reservation failed / payment declined / cancelled
}
