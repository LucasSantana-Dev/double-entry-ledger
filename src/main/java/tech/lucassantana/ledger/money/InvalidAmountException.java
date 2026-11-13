package tech.lucassantana.ledger.money;

public class InvalidAmountException extends RuntimeException {

    public InvalidAmountException(String value) {
        super("amount must match the pattern \\d{1,10}\\.\\d{2}, got: " + value);
    }
}
