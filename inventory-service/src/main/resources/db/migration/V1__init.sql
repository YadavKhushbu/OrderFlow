CREATE TABLE stock_items (
    sku      VARCHAR(64) PRIMARY KEY,
    on_hand  INT    NOT NULL CHECK (on_hand >= 0),
    reserved INT    NOT NULL DEFAULT 0 CHECK (reserved >= 0),
    version  BIGINT NOT NULL DEFAULT 0,
    -- Reserved stock is a promise against stock that physically exists. Letting
    -- it exceed on_hand would mean the service had promised goods it does not
    -- have, which is the exact failure this service exists to prevent.
    CONSTRAINT ck_reserved_within_on_hand CHECK (reserved <= on_hand)
);

-- What each saga is holding, so a release knows what to give back and a repeat
-- reservation can be recognised instead of double-counted.
CREATE TABLE reservations (
    id           BIGSERIAL PRIMARY KEY,
    saga_id      UUID        NOT NULL,
    order_id     BIGINT      NOT NULL,
    reservation_ref VARCHAR(64) NOT NULL,
    status       VARCHAR(16) NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    released_at  TIMESTAMPTZ,
    -- One live reservation per saga. A redelivered ReserveInventory command
    -- cannot create a second one.
    CONSTRAINT ux_reservations_saga UNIQUE (saga_id),
    CONSTRAINT ck_reservation_status CHECK (status IN ('HELD', 'RELEASED', 'COMMITTED'))
);

CREATE TABLE reservation_lines (
    id             BIGSERIAL PRIMARY KEY,
    reservation_id BIGINT      NOT NULL REFERENCES reservations (id) ON DELETE CASCADE,
    sku            VARCHAR(64) NOT NULL,
    quantity       INT         NOT NULL CHECK (quantity > 0)
);
CREATE INDEX ix_reservation_lines_reservation ON reservation_lines (reservation_id);

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
