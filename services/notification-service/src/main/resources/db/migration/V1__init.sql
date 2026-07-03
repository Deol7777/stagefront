-- notification-service schema, v1.
-- notifications: what we "sent" (persisted instead of a real email/SMS for the demo).
-- processed_events: idempotency/dedup store (no outbox — this service produces nothing).

CREATE TABLE notifications (
    id          UUID         PRIMARY KEY,
    order_id    VARCHAR(64)  NOT NULL,
    user_id     VARCHAR(64)  NOT NULL,
    type        VARCHAR(32)  NOT NULL,   -- ORDER_CONFIRMED | ORDER_CANCELLED | PAYMENT_REFUNDED
    message     TEXT         NOT NULL,
    created_at  TIMESTAMPTZ  NOT NULL
);
CREATE INDEX idx_notifications_order ON notifications (order_id);

CREATE TABLE processed_events (
    dedup_key    VARCHAR(160) PRIMARY KEY,   -- namespaced "<eventType>#<dedupKey>"
    consumer     VARCHAR(64)  NOT NULL,
    processed_at TIMESTAMPTZ  NOT NULL
);
