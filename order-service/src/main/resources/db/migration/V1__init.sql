-- ---------------------------------------------------------------------------
-- Order service schema.
--
-- Two tables here exist purely to make an unreliable network survivable:
-- outbox_events (so a published message can never disagree with committed
-- state) and processed_messages (so a redelivered message cannot be applied
-- twice). Everything else is ordinary order storage.
-- ---------------------------------------------------------------------------

CREATE TABLE orders (
    id             BIGSERIAL PRIMARY KEY,
    saga_id        UUID        NOT NULL,
    customer_ref   VARCHAR(64) NOT NULL,
    status         VARCHAR(32) NOT NULL,
    total_cents    BIGINT      NOT NULL,
    failure_reason VARCHAR(512),
    payment_ref    VARCHAR(64),
    reservation_ref VARCHAR(64),
    version        BIGINT      NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ux_orders_saga UNIQUE (saga_id),
    CONSTRAINT ck_orders_status CHECK (status IN (
        'PENDING', 'INVENTORY_RESERVED', 'CONFIRMED', 'COMPENSATING', 'CANCELLED'))
);
CREATE INDEX ix_orders_customer ON orders (customer_ref, created_at DESC);
CREATE INDEX ix_orders_status ON orders (status);

CREATE TABLE order_lines (
    id               BIGSERIAL PRIMARY KEY,
    order_id         BIGINT      NOT NULL REFERENCES orders (id) ON DELETE CASCADE,
    sku              VARCHAR(64) NOT NULL,
    quantity         INT         NOT NULL CHECK (quantity > 0),
    unit_price_cents BIGINT      NOT NULL CHECK (unit_price_cents >= 0)
);
CREATE INDEX ix_order_lines_order ON order_lines (order_id);

-- ---------------------------------------------------------------------------
-- The transactional outbox.
--
-- Solves the dual-write problem: a service cannot atomically commit to Postgres
-- and publish to Kafka. Whichever it does first, a crash in between leaves the
-- two permanently disagreeing -- an order with no message, or a message for an
-- order that was rolled back. Writing the message to this table inside the same
-- transaction as the state change makes the two atomic by construction, and a
-- relay publishes from here afterwards.
-- ---------------------------------------------------------------------------
CREATE TABLE outbox_events (
    id             BIGSERIAL PRIMARY KEY,
    message_id     UUID        NOT NULL,
    saga_id        UUID        NOT NULL,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id   VARCHAR(64) NOT NULL,
    topic          VARCHAR(128) NOT NULL,
    message_key    VARCHAR(128) NOT NULL,
    message_type   VARCHAR(64) NOT NULL,
    payload        TEXT        NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ,
    attempts       INT         NOT NULL DEFAULT 0,
    last_error     TEXT,
    CONSTRAINT ux_outbox_message_id UNIQUE (message_id)
);

-- The relay's only query. Partial, so it stays small no matter how much
-- published history accumulates behind it.
CREATE INDEX ix_outbox_pending ON outbox_events (id) WHERE published_at IS NULL;

-- ---------------------------------------------------------------------------
-- Consumer-side deduplication.
--
-- Kafka is at-least-once and the outbox relay is too: if a publish succeeds but
-- the row is not marked before the process dies, the message goes out again.
-- Recording handled message ids is what turns at-least-once delivery into
-- exactly-once *effect*, which is the only kind of exactly-once that exists.
-- ---------------------------------------------------------------------------
CREATE TABLE processed_messages (
    id           BIGSERIAL    PRIMARY KEY,
    message_id   UUID         NOT NULL,
    consumer     VARCHAR(128) NOT NULL,
    processed_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    -- Keyed by (message, consumer) rather than message alone: one message may
    -- legitimately be handled by several independent consumers, and each needs
    -- its own record of having done so.
    CONSTRAINT ux_processed_message_consumer UNIQUE (message_id, consumer)
);
-- Supports pruning old rows; this table grows forever otherwise.
CREATE INDEX ix_processed_messages_time ON processed_messages (processed_at);

CREATE TABLE shedlock (
    name       VARCHAR(64)  NOT NULL PRIMARY KEY,
    lock_until TIMESTAMPTZ  NOT NULL,
    locked_at  TIMESTAMPTZ  NOT NULL,
    locked_by  VARCHAR(255) NOT NULL
);
