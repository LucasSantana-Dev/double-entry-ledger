package tech.lucassantana.ledger.account;

import java.util.UUID;

public class InsufficientFundsException extends RuntimeException {

    public InsufficientFundsException(UUID accountId) {
        super("insufficient funds in account " + accountId);
    }
}
