package tech.lucassantana.ledger.benchmark;

import java.util.ArrayList;
import java.util.List;

/**
 * One configuration measured by the benchmark (ADR 0012).
 *
 * @param grid           {@code strategy} or {@code threads}, the question this cell answers
 * @param strategy       value of {@code ledger.concurrency.strategy}
 * @param hotShare       share of transfers that touch the hot account (0, 0.3 or 0.9)
 * @param users          concurrent JMeter users, each sending its next request when the last one answered
 * @param virtualThreads value of {@code spring.threads.virtual.enabled}
 */
record Cell(String grid, String strategy, double hotShare, int users, boolean virtualThreads) {

    static final List<String> STRATEGIES = List.of("pessimistic", "optimistic", "serializable", "conditional-update");
    static final List<Double> HOT_SHARES = List.of(0.0, 0.3, 0.9);

    /** 4 strategies x 3 hot shares, 256 users, virtual threads. */
    static List<Cell> strategyGrid() {
        List<Cell> cells = new ArrayList<>();
        for (String strategy : STRATEGIES) {
            for (double hot : HOT_SHARES) {
                cells.add(new Cell("strategy", strategy, hot, 256, true));
            }
        }
        return cells;
    }

    /** Pessimistic x 3 hot shares x 64 and 1,024 users (below and above Tomcat's 200 threads) x thread mode. */
    static List<Cell> threadGrid() {
        List<Cell> cells = new ArrayList<>();
        for (double hot : HOT_SHARES) {
            for (int users : List.of(64, 1024)) {
                for (boolean virtual : List.of(true, false)) {
                    cells.add(new Cell("threads", "pessimistic", hot, users, virtual));
                }
            }
        }
        return cells;
    }

    /** Stable, readable id, also used to filter cells on the command line. */
    String id() {
        return "%s-%s-hot%02d-u%d-%s".formatted(
                grid, strategy, Math.round(hotShare * 100), users, virtualThreads ? "virtual" : "platform");
    }
}
