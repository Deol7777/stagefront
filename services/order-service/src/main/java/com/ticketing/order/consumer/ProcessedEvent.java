package com.ticketing.order.consumer;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * Dedup record for events this service consumes. The key is namespaced as
 * {@code "<eventType>#<dedupKey>"} so two different event types that happen to
 * share an id (e.g. OrderConfirmed and OrderCancelled both keyed by orderId)
 * never collide in the dedup store.
 */
@Entity
@Table(name = "processed_events")
public class ProcessedEvent {

    @Id
    @Column(name = "dedup_key")
    private String dedupKey;

    @Column(nullable = false)
    private String consumer;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;

    protected ProcessedEvent() {
    }

    public ProcessedEvent(String dedupKey, String consumer, Instant processedAt) {
        this.dedupKey = dedupKey;
        this.consumer = consumer;
        this.processedAt = processedAt;
    }

    public String getDedupKey() { return dedupKey; }
}
