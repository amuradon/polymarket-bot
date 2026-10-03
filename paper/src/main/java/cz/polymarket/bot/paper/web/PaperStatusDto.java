package cz.polymarket.bot.paper.web;

import cz.polymarket.bot.paper.engine.PaperPosition;

/**
 * Status information response for the paper trading engine.
 */
public record PaperStatusDto(
        String strategyName,
        String status,
        long orderLatencyMs,
        double initialCapital,
        double currentBalance,
        int totalTrades,
        PaperPosition activePosition
) {
}
