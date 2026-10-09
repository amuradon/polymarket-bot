package cz.polymarket.bot.strategy;

import cz.polymarket.bot.cache.HourlyPriceCache;

/**
 * Execution context provided to a TradingStrategy instance during execution.
 * Provides access to price cache and execution routing.
 */
public interface StrategyContext {

    /**
     * Returns the execution router for order submission and cancellation.
     *
     * @return the execution router
     */
    ExecutionRouter getExecutionRouter();

    /**
     * Returns the shared rolling price cache.
     *
     * @return the hourly price cache
     */
    HourlyPriceCache getPriceCache();

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
}
