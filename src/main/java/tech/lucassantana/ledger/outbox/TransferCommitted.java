package tech.lucassantana.ledger.outbox;

import java.util.UUID;

/** Payload of the {@code TransferCommitted} event (ADR 0018). Amounts in cents (ADR 0004). */
public record TransferCommitted(UUID transferId, UUID fromAccountId, UUID toAccountId, long amountCents) {

    public static final String EVENT_TYPE = "TransferCommitted";
    public static final String AGGREGATE_TYPE = "transfer";
    public static final int SCHEMA_VERSION = 1;
}
