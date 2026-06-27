-- order-service schema, v1. Flyway applies this once on startup and records it.
-- Two tables: the business table (orders) and the transactional outbox.

-- The order aggregate this service owns.
CREATE TABLE orders (
    id                VARCHAR(64)  PRIMARY KEY,        -- order id (UUID string)
    user_id           VARCHAR(64)  NOT NULL,
    seat_id           VARCHAR(64)  NOT NULL,
    event_schedule_id VARCHAR(64)  NOT NULL,
    amount            NUMERIC(12,2) NOT NULL,          -- exact money (never float)
    currency          VARCHAR(3)   NOT NULL,
    status            VARCHAR(32)  NOT NULL,           -- PENDING | CONFIRMED | CANCELLED
    created_at        TIMESTAMPTZ  NOT NULL
);

-- The transactional outbox. A row is written in the SAME transaction as the
-- business change, so "save order" and "record event" commit atomically. A
-- separate relay then publishes unpublished rows to Kafka. This is what removes
-- the dual-write problem (DB and Kafka can never disagree).
CREATE TABLE outbox (
    id             UUID         PRIMARY KEY,
    aggregate_type VARCHAR(64)  NOT NULL,   -- e.g. "Order" (what the event is about)
    aggregate_id   VARCHAR(64)  NOT NULL,   -- e.g. the orderId
    event_type     VARCHAR(64)  NOT NULL,   -- e.g. "OrderPlaced"
    topic          VARCHAR(128) NOT NULL,   -- Kafka topic to publish to
    partition_key  VARCHAR(128) NOT NULL,   -- Kafka message key (orderId → ordering)
    payload        TEXT         NOT NULL,   -- the full event envelope, as JSON
    schema_version INT          NOT NULL,
    created_at     TIMESTAMPTZ  NOT NULL,
    published      BOOLEAN      NOT NULL DEFAULT FALSE,
    published_at   TIMESTAMPTZ
);

-- Partial index: the relay only ever queries unpublished rows in time order.
-- Indexing just those keeps the poll cheap even as the table grows.
CREATE INDEX idx_outbox_unpublished ON outbox (created_at) WHERE published = FALSE;
