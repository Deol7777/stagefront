/**
 * Shared Kafka event contracts for the ticketing platform.
 *
 * <p>Holds the shared building blocks: {@link com.ticketing.contracts.EventEnvelope}
 * (the common wire wrapper), {@link com.ticketing.contracts.Money} (value object),
 * and {@link com.ticketing.contracts.Topics} (Kafka topic names). The event
 * payloads themselves live in the {@code events} sub-package, with
 * {@code EventPayload} (sealed) and {@code EventType} (the registry).
 *
 * <p>Rule: this is the contract. Change a shape only alongside a
 * {@code schemaVersion} bump and an update to {@code docs/events.md}.
 */
package com.ticketing.contracts;
