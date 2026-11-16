-- Transactional outbox (ADR 0016): one row per committed transfer, written in the transfer's own
-- transaction and moved to Kafka by the relay. The identity is the ledger sequence: it is drawn
-- after the transfer locked or read its accounts, so per account it follows commit order.
create table outbox_events (
    id             bigint generated always as identity primary key,
    event_id       uuid         not null unique,
    aggregate_type varchar(32)  not null,
    aggregate_id   uuid         not null,
    event_type     varchar(64)  not null,
    schema_version int          not null,
    payload        jsonb        not null,
    occurred_at    timestamptz  not null,
    published_at   timestamptz,
    attempts       int          not null default 0,
    last_error     text,
    -- Set when Kafka rejected the event for good (poison message); the relay skips it.
    parked_at      timestamptz
);

-- What the relay scans: rows still waiting, in sequence order.
create index outbox_events_pending_idx on outbox_events (id) where published_at is null and parked_at is null;
-- What the cleanup job scans.
create index outbox_events_published_at_idx on outbox_events (published_at) where published_at is not null;
