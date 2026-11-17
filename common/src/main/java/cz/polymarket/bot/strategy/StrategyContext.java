package cz.polymarket.bot.strategy;

/**
 * Execution context provided to a TradingStrategy instance during execution.
 * Provides access to execution routing and account operations.
 */
public interface StrategyContext {

    /**
     * Returns the execution router for order submission and cancellation.
     *
     * @return the execution router
     */
    ExecutionRouter getExecutionRouter();

    /**
     * Forces a refresh of the account collateral balance from Polymarket in live trading
     * (no-op in paper trading and backtesting).
     */
    default void refreshAccountBalance() {
    }
}
