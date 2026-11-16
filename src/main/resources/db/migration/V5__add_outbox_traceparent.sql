-- Tracing (ADR 0021): the W3C trace context of the transfer that wrote the row, restored by the
-- relay as the parent of the span that sends it. Null when nothing was being traced, and for rows
-- written before this migration; the relay then starts a new trace.
alter table outbox_events add column traceparent text;

-- What the parked gauge counts (ADR 0022): parked rows are rare and never deleted, so a partial
-- index keeps the count from scanning the published history.
create index outbox_events_parked_idx on outbox_events (id) where parked_at is not null;
