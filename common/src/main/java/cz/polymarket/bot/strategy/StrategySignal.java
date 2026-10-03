package cz.polymarket.bot.strategy;

/**
 * Immutable trading signal produced by the strategy evaluation engine.
 */
public record StrategySignal(
        TradeDirection direction,
        double modelProbability,
        double marketPrice,
        double edge,
        double suggestedSizeUsd,
        String reason
) {
    public StrategySignal {
        if (direction == null) {
            throw new IllegalArgumentException("direction cannot be null");
        }
    }

    public static StrategySignal noTrade(String reason) {
        return new StrategySignal(TradeDirection.NO_TRADE, 0.50, 0.50, 0.0, 0.0, reason);
    }

    public boolean isTrade() {
        return direction.isTrade();
    }
}
