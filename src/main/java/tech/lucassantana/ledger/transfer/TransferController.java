package tech.lucassantana.ledger.transfer;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import tech.lucassantana.ledger.idempotency.InvalidIdempotencyKeyException;
import tech.lucassantana.ledger.money.Money;

@RestController
class TransferController {

    private static final String IDEMPOTENCY_KEY = "Idempotency-Key";
    /** Same header name Stripe uses to flag a replayed response. */
    private static final String REPLAYED = "Idempotent-Replayed";

    private final TransferService service;

    TransferController(TransferService service) {
        this.service = service;
    }

    @PostMapping("/transfers")
    ResponseEntity<TransferResponse> transfer(
            @RequestHeader(IDEMPOTENCY_KEY) String key, @Valid @RequestBody TransferRequest request) {
        TransferOutcome outcome = service.transfer(
                checked(key), request.fromAccountId(), request.toAccountId(), Money.parse(request.amount()));
        Transfer t = outcome.transfer();
        return created(outcome, new TransferResponse(
                t.getId(), t.getFromAccountId(), t.getToAccountId(), amount(t), t.getCreatedAt()));
    }

    @PostMapping("/deposits")
    ResponseEntity<DepositResponse> deposit(
            @RequestHeader(IDEMPOTENCY_KEY) String key, @Valid @RequestBody DepositRequest request) {
        TransferOutcome outcome = service.deposit(checked(key), request.accountId(), Money.parse(request.amount()));
        Transfer t = outcome.transfer();
        return created(outcome, new DepositResponse(t.getId(), t.getToAccountId(), amount(t), t.getCreatedAt()));
    }

    private static <T> ResponseEntity<T> created(TransferOutcome outcome, T body) {
        ResponseEntity.BodyBuilder response = ResponseEntity.status(HttpStatus.CREATED);
        if (outcome.replayed()) {
            response.header(REPLAYED, "true");
        }
        return response.body(body);
    }

    private static String checked(String key) {
        if (key.isBlank() || key.length() > 255) {
            throw new InvalidIdempotencyKeyException();
        }
        return key;
    }

    private static String amount(Transfer transfer) {
        return new Money(transfer.getAmountCents()).toString();
    }

    record TransferRequest(@NotNull UUID fromAccountId, @NotNull UUID toAccountId, @NotNull String amount) {
    }

    record DepositRequest(@NotNull UUID accountId, @NotNull String amount) {
    }

    record TransferResponse(UUID id, UUID fromAccountId, UUID toAccountId, String amount, Instant createdAt) {
    }

    record DepositResponse(UUID id, UUID accountId, String amount, Instant createdAt) {
    }
}
