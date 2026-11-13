package tech.lucassantana.ledger.transfer;

public class InvalidTransferException extends RuntimeException {

    public InvalidTransferException(String reason) {
        super(reason);
    }
}
