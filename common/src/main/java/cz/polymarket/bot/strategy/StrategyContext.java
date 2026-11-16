package cz.polymarket.bot.strategy;

/**
 * Execution context provided to a TradingStrategy instance during execution.
 * Provides access to indicators and execution routing.
 */
public interface StrategyContext {

    /**
     * Returns the execution router for order submission and cancellation.
     *
     * @return the execution router
     */
    ExecutionRouter getExecutionRouter();

    /**
     * Returns the current real-time or cached value of the requested indicator.
     *
     * @param type indicator type
     * @return the current indicator value, or Double.NaN if not available
     */
    default double getIndicatorValue(IndicatorType type) {
        return Double.NaN;
    }

    /**
     * Returns all currently computed indicator values.
     *
     * @return map of indicator types to current values
     */
    default java.util.Map<IndicatorType, Double> getIndicators() {
        return java.util.Map.of();
    }

    /**
     * Forces a refresh of the account collateral balance from Polymarket in live trading
     * (no-op in paper trading and backtesting).
     */
    default void refreshAccountBalance() {
    }
}
