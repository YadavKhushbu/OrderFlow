CREATE TABLE payments (
    id              BIGSERIAL PRIMARY KEY,
    saga_id         UUID        NOT NULL,
    order_id        BIGINT      NOT NULL,
    customer_ref    VARCHAR(64) NOT NULL,
    amount_cents    BIGINT      NOT NULL CHECK (amount_cents >= 0),
    status          VARCHAR(16) NOT NULL,
    transaction_ref VARCHAR(64),
    decline_code    VARCHAR(64),
    -- The provider-facing idempotency key. Unique, because this constraint is
    -- the last thing standing between a redelivered AuthorizePayment command and
    -- a customer being charged twice.
    idempotency_key VARCHAR(128) NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    refunded_at     TIMESTAMPTZ,
    CONSTRAINT ux_payments_idempotency_key UNIQUE (idempotency_key),
    CONSTRAINT ux_payments_saga UNIQUE (saga_id),
    CONSTRAINT ck_payment_status CHECK (status IN ('AUTHORIZED', 'DECLINED', 'REFUNDED'))
);
CREATE INDEX ix_payments_order ON payments (order_id);

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
CREATE INDEX ix_outbox_pending ON outbox_events (id) WHERE published_at IS NULL;

CREATE TABLE processed_messages (
    id           BIGSERIAL    PRIMARY KEY,
    message_id   UUID         NOT NULL,
    consumer     VARCHAR(128) NOT NULL,
    processed_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT ux_processed_message_consumer UNIQUE (message_id, consumer)
);
CREATE INDEX ix_processed_messages_time ON processed_messages (processed_at);

CREATE TABLE shedlock (
    name       VARCHAR(64)  NOT NULL PRIMARY KEY,
    lock_until TIMESTAMPTZ  NOT NULL,
    locked_at  TIMESTAMPTZ  NOT NULL,
    locked_by  VARCHAR(255) NOT NULL
);
