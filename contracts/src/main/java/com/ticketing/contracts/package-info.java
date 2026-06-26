/**
 * Shared Kafka event contracts for the ticketing platform.
 *
 * <p>This package will hold the Java representations of every event in
 * {@code docs/events.md} (OrderPlaced, SeatReserved, PaymentAuthorized, ...),
 * plus the common envelope (eventId, eventType, schemaVersion, occurredAt,
 * traceId). It is shared by all services so producers and consumers agree on
 * the exact wire shape.
 *
 * <p>Rule: this package IS the contract. Change a shape here only alongside a
 * {@code schemaVersion} bump and an update to {@code docs/events.md}.
 *
 * <p>Currently empty — event types are added in the events-implementation step.
 */
package com.ticketing.contracts;
