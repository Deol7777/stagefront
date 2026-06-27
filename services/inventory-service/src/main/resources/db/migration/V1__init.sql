-- inventory-service schema, v1.
-- Tables: seats (owned inventory), processed_events (idempotency/dedup store),
-- and outbox (same transactional-outbox pattern as order-service).

-- The seat inventory this service owns.
CREATE TABLE seats (
    seat_id           VARCHAR(64)  PRIMARY KEY,
    event_schedule_id VARCHAR(64)  NOT NULL,
    status            VARCHAR(16)  NOT NULL,   -- AVAILABLE | RESERVED | SOLD
    reserved_by_order VARCHAR(64),             -- which order holds it (if any)
    reservation_id    VARCHAR(64),             -- id of the active reservation
    updated_at        TIMESTAMPTZ
);

-- Idempotency store. A consumer records the dedup key of every event it has
-- successfully handled. Seeing a key already here => duplicate => skip. The
-- PRIMARY KEY also makes a concurrent double-process fail on insert (safe).
CREATE TABLE processed_events (
    dedup_key    VARCHAR(128) PRIMARY KEY,
    consumer     VARCHAR(64)  NOT NULL,
    processed_at TIMESTAMPTZ  NOT NULL
);

-- Transactional outbox (identical pattern to order-service).
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

-- Seed a few seats for the demo show so reservations have something to grab.
INSERT INTO seats (seat_id, event_schedule_id, status) VALUES
    ('seat-A1', 'show-9', 'AVAILABLE'),
    ('seat-B2', 'show-9', 'AVAILABLE'),
    ('seat-C3', 'show-9', 'AVAILABLE'),
    ('seat-D4', 'show-9', 'AVAILABLE');
