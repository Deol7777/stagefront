package com.ticketing.notification.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

/**
 * A notification we "sent" to a user. For the demo we persist + log instead of
 * sending a real email/SMS — the point is proving the event-driven delivery, not
 * the channel.
 */
@Entity
@Table(name = "notifications")
public class NotificationEntity {

    @Id
    private UUID id;

    @Column(name = "order_id", nullable = false)
    private String orderId;

    @Column(name = "user_id", nullable = false)
    private String userId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private NotificationType type;

    @Column(nullable = false)
    private String message;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected NotificationEntity() {
    }

    public NotificationEntity(UUID id, String orderId, String userId,
                              NotificationType type, String message, Instant createdAt) {
        this.id = id;
        this.orderId = orderId;
        this.userId = userId;
        this.type = type;
        this.message = message;
        this.createdAt = createdAt;
    }

    public UUID getId()            { return id; }
    public String getOrderId()     { return orderId; }
    public String getUserId()      { return userId; }
    public NotificationType getType() { return type; }
    public String getMessage()     { return message; }
    public Instant getCreatedAt()  { return createdAt; }
}
