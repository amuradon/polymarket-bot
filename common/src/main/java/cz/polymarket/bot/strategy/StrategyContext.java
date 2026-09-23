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
}
