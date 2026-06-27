package com.ticketing.inventory.consumer;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * A record that this consumer has already handled a given event (the dedup key).
 * Written in the SAME transaction as the work it represents, so "did the work"
 * and "marked it done" are atomic — a duplicate delivery finds the key and skips.
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
