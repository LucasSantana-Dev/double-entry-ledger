package tech.lucassantana.ledger.reconciliation;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Ledger against statement for every transfer in {@code [from, cutoff)} (ADR 0017). Transfers
 * whose event is still in the outbox are listed apart and left out of every comparison.
 *
 * @param pendingInOutbox transfers whose event the relay has not published yet; not a difference
 * @param parkedInOutbox transfers whose event Kafka rejected for good (ADR 0016); a difference
 * @param missingFromStatement published transfers with no statement line
 * @param malformedInStatement transfers whose statement lines are not exactly two that cancel out
 * @param unknownInStatement transfer ids in the statement that the ledger does not have
 * @param windows hour windows where transfer count or credited sum differ
 * @param accounts accounts whose net movement differs inside a differing window
 */
public record ReconciliationReport(
        Instant from,
        Instant cutoff,
        long ledgerTransfers,
        long statementTransfers,
        List<UUID> pendingInOutbox,
        List<UUID> parkedInOutbox,
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

    /** No difference. Pending events are on their way, so they do not make a report dirty. */
    public boolean isClean() {
        return parkedInOutbox.isEmpty() && missingFromStatement.isEmpty() && malformedInStatement.isEmpty()
                && unknownInStatement.isEmpty() && windows.isEmpty() && accounts.isEmpty();
    }
}
