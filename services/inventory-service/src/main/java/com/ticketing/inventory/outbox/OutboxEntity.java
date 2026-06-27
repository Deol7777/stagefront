package com.ticketing.inventory.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

/**
 * One event in the inventory-service outbox. Same transactional-outbox pattern
 * as order-service: written atomically with the business change, published later
 * by the relay. See docs/notes/04-transactional-outbox.md.
 */
@Entity
@Table(name = "outbox")
public class OutboxEntity {

    @Id
    private UUID id;

    @Column(name = "aggregate_type", nullable = false)
    private String aggregateType;

    @Column(name = "aggregate_id", nullable = false)
    private String aggregateId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(nullable = false)
    private String topic;

    @Column(name = "partition_key", nullable = false)
    private String partitionKey;

    @Column(nullable = false)
    private String payload;

    @Column(name = "schema_version", nullable = false)
    private int schemaVersion;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(nullable = false)
    private boolean published;

    @Column(name = "published_at")
    private Instant publishedAt;

    protected OutboxEntity() {
    }

    public OutboxEntity(UUID id, String aggregateType, String aggregateId, String eventType,
                        String topic, String partitionKey, String payload, int schemaVersion,
                        Instant createdAt) {
        this.id = id;
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.topic = topic;
        this.partitionKey = partitionKey;
        this.payload = payload;
        this.schemaVersion = schemaVersion;
        this.createdAt = createdAt;
        this.published = false;
    }

    public UUID getId()             { return id; }
    public String getTopic()        { return topic; }
    public String getPartitionKey() { return partitionKey; }
    public String getPayload()      { return payload; }
    public String getEventType()    { return eventType; }

    public void markPublished(Instant when) {
        this.published = true;
        this.publishedAt = when;
    }
}
