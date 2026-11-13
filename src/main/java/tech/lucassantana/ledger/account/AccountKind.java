package tech.lucassantana.ledger.account;

public enum AccountKind {
    /** Belongs to a customer; its balance can never go negative. */
    CUSTOMER,
    /** Internal mirror of money that entered the ledger from outside (ADR 0008). */
    SETTLEMENT
}
