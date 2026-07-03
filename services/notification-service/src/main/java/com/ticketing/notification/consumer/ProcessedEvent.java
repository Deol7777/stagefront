package com.ticketing.notification.consumer;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * Dedup record. Namespaced key "<eventType>#<dedupKey>". Critical here: without
 * it, a duplicate delivery would send the user a second notification.
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
