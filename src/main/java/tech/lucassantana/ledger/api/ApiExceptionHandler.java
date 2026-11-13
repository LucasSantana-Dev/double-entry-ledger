package tech.lucassantana.ledger.api;

import java.net.URI;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import tech.lucassantana.ledger.account.AccountNotFoundException;
import tech.lucassantana.ledger.account.InsufficientFundsException;
import tech.lucassantana.ledger.idempotency.IdempotencyKeyInProgressException;
import tech.lucassantana.ledger.idempotency.IdempotencyKeyReusedException;
import tech.lucassantana.ledger.idempotency.InvalidIdempotencyKeyException;
import tech.lucassantana.ledger.money.InvalidAmountException;
import tech.lucassantana.ledger.transfer.ConcurrencyConflictException;
import tech.lucassantana.ledger.transfer.InvalidTransferException;

/** Domain failures as RFC 9457 problem details, one {@code type} per failure. */
@RestControllerAdvice
class ApiExceptionHandler {

    @ExceptionHandler
    ProblemDetail invalidAmount(InvalidAmountException ex) {
        return problem(400, "invalid-amount", ex);
    }

    @ExceptionHandler
    ProblemDetail invalidKey(InvalidIdempotencyKeyException ex) {
        return problem(400, "invalid-idempotency-key", ex);
    }

    @ExceptionHandler
    ProblemDetail accountNotFound(AccountNotFoundException ex) {
        return problem(404, "account-not-found", ex);
    }

    /** Draft IETF httpapi-idempotency-key-header-08: retry while the original is still running. */
    @ExceptionHandler
    ProblemDetail keyInProgress(IdempotencyKeyInProgressException ex) {
        return problem(409, "idempotency-key-in-progress", ex);
    }

    /** Same draft: a key reused with a different payload. */
    @ExceptionHandler
    ProblemDetail keyReused(IdempotencyKeyReusedException ex) {
        return problem(422, "idempotency-key-reused", ex);
    }

    @ExceptionHandler
    ProblemDetail insufficientFunds(InsufficientFundsException ex) {
        return problem(422, "insufficient-funds", ex);
    }

    @ExceptionHandler
    ProblemDetail invalidTransfer(InvalidTransferException ex) {
        return problem(422, "invalid-transfer", ex);
    }

    /** The strategy kept aborting on conflict (ADR 0010). Nothing was written; retry with the same key. */
    @ExceptionHandler
    ResponseEntity<ProblemDetail> concurrencyConflict(ConcurrencyConflictException ex) {
        return ResponseEntity.status(503)
                .header(HttpHeaders.RETRY_AFTER, "1")
                .body(problem(503, "concurrency-conflict", ex));
    }

    private static ProblemDetail problem(int status, String type, RuntimeException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatusCode.valueOf(status), ex.getMessage());
        problem.setType(URI.create("/problems/" + type));
        problem.setTitle(type.replace('-', ' '));
        return problem;
    }
}
