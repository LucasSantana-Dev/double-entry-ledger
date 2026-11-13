package tech.lucassantana.ledger.transfer;

/** {@code replayed} is true when the key already had a committed transfer and nothing moved. */
public record TransferOutcome(Transfer transfer, boolean replayed) {
}
