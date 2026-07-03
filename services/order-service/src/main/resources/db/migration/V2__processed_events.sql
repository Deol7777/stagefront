-- order-service now consumes events too (payment outcomes, reservation failures),
-- so it needs an idempotency/dedup store like the other consumers.
CREATE TABLE processed_events (
    dedup_key    VARCHAR(160) PRIMARY KEY,   -- namespaced: "<eventType>#<dedupKey>"
    consumer     VARCHAR(64)  NOT NULL,
    processed_at TIMESTAMPTZ  NOT NULL
);
