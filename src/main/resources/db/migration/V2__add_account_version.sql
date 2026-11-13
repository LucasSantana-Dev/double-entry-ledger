-- Optimistic concurrency (ADR 0010): every balance change bumps the version, and the optimistic
-- strategy only writes when the version it read is still the current one.
alter table accounts add column version bigint not null default 0;
