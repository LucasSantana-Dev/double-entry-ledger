-- Account statement projection (ADR 0017): a read model fed only by Kafka events. It lives in its
-- own schema and has no foreign keys into the ledger, so it can move to its own database.
create schema statement;

-- One row per applied event. Inserted in the same transaction as the lines: a duplicate key
-- means the event was already applied.
create table statement.processed_events (
    event_id     uuid primary key,
    processed_at timestamptz not null default now()
);

-- One line per account per event: the debit on the source, the credit on the destination.
create table statement.statement_lines (
    id           bigint generated always as identity primary key,
    account_id   uuid        not null,
    transfer_id  uuid        not null,
    event_id     uuid        not null,
    -- Ledger sequence from the event: per account it follows commit order, so the statement is
    -- read in this order, never in arrival order.
    sequence     bigint      not null,
    amount_cents bigint      not null check (amount_cents <> 0),
    occurred_at  timestamptz not null,
    applied_at   timestamptz not null default now(),
    constraint statement_line_once_per_account unique (event_id, account_id)
);

create index statement_lines_account_sequence_idx on statement.statement_lines (account_id, sequence);
create index statement_lines_transfer_id_idx on statement.statement_lines (transfer_id);
