package tech.lucassantana.ledger.account;

import java.net.URI;
import java.time.Instant;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import tech.lucassantana.ledger.money.Money;

@RestController
@RequestMapping("/accounts")
class AccountController {

    private final AccountRepository accounts;

    AccountController(AccountRepository accounts) {
        this.accounts = accounts;
    }

    @PostMapping
    ResponseEntity<AccountResponse> open() {
        Account account = accounts.save(Account.openCustomer());
        return ResponseEntity.created(URI.create("/accounts/" + account.getId()))
                .body(AccountResponse.of(account));
    }

    /** The settlement account is internal, so it answers 404 like an unknown id. */
    @GetMapping("/{id}")
    AccountResponse get(@PathVariable UUID id) {
        return accounts.findById(id)
                .filter(Account::isCustomer)
                .map(AccountResponse::of)
                .orElseThrow(() -> new AccountNotFoundException(id));
    }

    record AccountResponse(UUID id, String balance, Instant createdAt) {

        static AccountResponse of(Account account) {
            return new AccountResponse(
                    account.getId(), new Money(account.getBalanceCents()).toString(), account.getCreatedAt());
        }
    }
}
