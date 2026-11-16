package tech.lucassantana.ledger.reconciliation;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Ledger against statement for every transfer that happened before {@code cutoff} (ADR 0017).
 *
 * @param missingFromStatement transfers in the ledger with no statement line
 * @param malformedInStatement transfers whose statement lines are not exactly two that cancel out
 * @param unknownInStatement transfer ids in the statement that the ledger does not have
 * @param windows hour windows where transfer count or credited sum differ
 * @param accounts accounts whose net movement differs inside a differing window
 */
public record ReconciliationReport(
        Instant cutoff,
        long ledgerTransfers,
        long statementTransfers,
        List<UUID> missingFromStatement,
        List<UUID> malformedInStatement,
        List<UUID> unknownInStatement,
        List<WindowDifference> windows,
        List<AccountDifference> accounts) {

    public record WindowDifference(
            Instant window, long ledgerTransfers, long statementTransfers, long ledgerCents, long statementCents) {
    }

    public record AccountDifference(Instant window, UUID accountId, long ledgerCents, long statementCents) {
    }

    public boolean isClean() {
        return missingFromStatement.isEmpty() && malformedInStatement.isEmpty() && unknownInStatement.isEmpty()
                && windows.isEmpty() && accounts.isEmpty();
    }
}
