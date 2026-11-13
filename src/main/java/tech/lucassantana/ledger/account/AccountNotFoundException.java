package tech.lucassantana.ledger.account;

import java.util.UUID;

public class AccountNotFoundException extends RuntimeException {

    public AccountNotFoundException(UUID id) {
        super("account not found: " + id);
    }
}
