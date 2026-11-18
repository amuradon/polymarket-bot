package cz.polymarket.bot.engine;

import java.util.Locale;

/**
 * Execution mode of the trading strategy inside the unified Polymarket Bot application.
 */
public enum StrategyMode {

    /** Real-money order execution on Polymarket CLOB (EIP-712 signing + {@code POST /order} + {@code /ws/user}). */
    LIVE,

    /** Simulated paper order execution against live market feeds persisted to {@code trades.jsonl}. */
    PAPER,

    /** Strategy completely stopped (zero strategy threads or strategy stream queues; Data Collector runs standalone). */
    OFF;

    /**
     * Parses a configuration or REST mode string into a {@link StrategyMode}, failing fast on invalid values.
     *
     * @param raw mode string ({@code "LIVE"}, {@code "PAPER"}, or {@code "OFF"}, case-insensitive)
     * @return parsed {@link StrategyMode}
     * @throws IllegalArgumentException if {@code raw} is null, blank, or unrecognized
     */
    public static StrategyMode parse(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException("StrategyMode cannot be null or blank (expected LIVE, PAPER, or OFF)");
        }
        String normalized = raw.trim().toUpperCase(Locale.ROOT);
        return switch (normalized) {
            case "LIVE" -> LIVE;
            case "PAPER" -> PAPER;
            case "OFF", "NONE", "DISABLED" -> OFF;
            default -> throw new IllegalArgumentException(
                    "Invalid StrategyMode '" + raw + "' (expected LIVE, PAPER, or OFF)");
        };
    }
}
