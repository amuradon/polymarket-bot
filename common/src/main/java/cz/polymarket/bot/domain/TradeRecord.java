package cz.polymarket.bot.domain;

import cz.polymarket.bot.strategy.TradeDirection;

/**
 * Immutable record representing an executed trade.
 * Shared across Backtest, Paper, and Live trading engines to guarantee zero discrepancy.
 */
public record TradeRecord(
        String entryTimeUtc,
        long intervalStartSec,
        TradeDirection side,
        double entryPrice,
        double exitPrice,
        double sizeUsd,
        double shares,
        double feeUsd,
        double netPnl,
        String exitReason,
        double modelProbability,
        double marketPrice,
        double edge,
        boolean isWin,
        double balanceAfterTrade
) {
    public TradeRecord {
        if (entryPrice <= 0 || exitPrice < 0) {
            throw new IllegalArgumentException("Prices cannot be negative or zero for entry: entry=" + entryPrice + ", exit=" + exitPrice);
        }
        if (sizeUsd <= 0 || shares <= 0) {
            throw new IllegalArgumentException("Trade size and shares must be positive: size=" + sizeUsd + ", shares=" + shares);
        }
    }
}
