-- payment-service schema, v1.
-- Tables: payments (owned), processed_events (dedup), outbox (same pattern).

CREATE TABLE payments (
    payment_id     VARCHAR(64)  PRIMARY KEY,
    order_id       VARCHAR(64)  NOT NULL,
    reservation_id VARCHAR(64)  NOT NULL,
    amount         NUMERIC(12,2) NOT NULL,
    currency       VARCHAR(3)   NOT NULL,
    status         VARCHAR(16)  NOT NULL,   -- AUTHORIZED | DECLINED | REFUNDED
    created_at     TIMESTAMPTZ  NOT NULL
);

-- Idempotency store (dedup key = reservationId for SeatReserved).
CREATE TABLE processed_events (
    dedup_key    VARCHAR(128) PRIMARY KEY,
    consumer     VARCHAR(64)  NOT NULL,
    processed_at TIMESTAMPTZ  NOT NULL
);

-- Transactional outbox.
CREATE TABLE outbox (
    id             UUID         PRIMARY KEY,
    aggregate_type VARCHAR(64)  NOT NULL,
    aggregate_id   VARCHAR(64)  NOT NULL,
    event_type     VARCHAR(64)  NOT NULL,
    topic          VARCHAR(128) NOT NULL,
    partition_key  VARCHAR(128) NOT NULL,
    payload        TEXT         NOT NULL,
    schema_version INT          NOT NULL,
    created_at     TIMESTAMPTZ  NOT NULL,
    published      BOOLEAN      NOT NULL DEFAULT FALSE,
    published_at   TIMESTAMPTZ
);
CREATE INDEX idx_outbox_unpublished ON outbox (created_at) WHERE published = FALSE;
