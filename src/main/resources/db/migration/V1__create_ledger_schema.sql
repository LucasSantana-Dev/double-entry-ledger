create table accounts (
    id            uuid primary key,
    kind          varchar(16) not null check (kind in ('CUSTOMER', 'SETTLEMENT')),
    balance_cents bigint      not null default 0,
    created_at    timestamptz not null default now(),
    -- Backstop for the service rule: customers never go negative. The settlement
    -- account mirrors money that came from outside the ledger, so it may.
    constraint customer_balance_non_negative check (kind = 'SETTLEMENT' or balance_cents >= 0)
);

create table transfers (
    id              uuid primary key,
    from_account_id uuid        not null references accounts (id),
    to_account_id   uuid        not null references accounts (id),
    amount_cents    bigint      not null check (amount_cents > 0),
    created_at      timestamptz not null default now(),
    constraint transfer_between_distinct_accounts check (from_account_id <> to_account_id)
);

-- Double entry: every transfer writes one negative (debit) and one positive (credit) entry.
create table entries (
    id           uuid primary key,
    transfer_id  uuid        not null references transfers (id),
    account_id   uuid        not null references accounts (id),
    amount_cents bigint      not null check (amount_cents <> 0),
    created_at   timestamptz not null default now()
);

create index entries_account_id_idx on entries (account_id);
create index entries_transfer_id_idx on entries (transfer_id);

-- A key only exists once its transfer committed: failed requests roll the key back (ADR 0006).
create table idempotency_keys (
    idempotency_key     varchar(255) primary key,
    request_fingerprint varchar(255) not null,
    transfer_id         uuid         not null unique references transfers (id),
    created_at          timestamptz  not null default now()
);

-- Entries are append-only. A correction is a new transfer, never an edit.
create function reject_entry_mutation() returns trigger
    language plpgsql as
$$
begin
    raise exception 'entries are append-only: % rejected', tg_op;
end;
$$;

create trigger entries_append_only
    before update or delete on entries
    for each row execute function reject_entry_mutation();

insert into accounts (id, kind) values ('00000000-0000-0000-0000-000000000001', 'SETTLEMENT');
