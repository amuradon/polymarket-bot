package cz.polymarket.bot.paper.engine;

import cz.polymarket.bot.strategy.TradeDirection;

/**
 * Immutable state of an open paper trading position.
 */
public record PaperPosition(
        String clientOrderId,
        TradeDirection side,
        double entryPrice,
        double shares,
        double sizeUsd,
        double entryFee,
        long entryTimestampMs,
        long intervalStartSec,
        String token,
        double modelProbability,
        double marketPrice,
        double edge,
        double maxObservedPrice,
        boolean trailingStopArmed
) {
    public PaperPosition {
        if (clientOrderId == null || clientOrderId.isBlank()) {
            throw new IllegalArgumentException("clientOrderId cannot be null or blank");
        }
        if (side == null || side == TradeDirection.NO_TRADE) {
            throw new IllegalArgumentException("side must be UP or DOWN");
        }
        if (entryPrice <= 0 || shares <= 0 || sizeUsd <= 0) {
            throw new IllegalArgumentException("Price, shares, and size must be positive");
        }
    }

    public PaperPosition withPriceObservation(double currentPrice, boolean armed) {
        double newMax = Math.max(this.maxObservedPrice, currentPrice);
        return new PaperPosition(
                clientOrderId,
                side,
                entryPrice,
                shares,
                sizeUsd,
                entryFee,
                entryTimestampMs,
                intervalStartSec,
                token,
                modelProbability,
                marketPrice,
                edge,
                newMax,
                armed || trailingStopArmed
        );
    }
}
