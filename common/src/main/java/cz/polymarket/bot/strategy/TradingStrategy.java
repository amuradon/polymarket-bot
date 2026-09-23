package cz.polymarket.bot.strategy;

import cz.polymarket.bot.domain.TwapUpdate;

/**
 * Standard contract for trading strategies.
 * Implemented once in common/strategy layer, executing identically across
 * Backtesting, Paper Trading, and Live Trading without modification.
 */
public interface TradingStrategy {

    /**
     * Initializes the strategy with the execution context.
     *
     * @param context the execution context providing market data and order placement capabilities
     */
    void init(StrategyContext context);

    /**
     * Callback triggered when a new aggregate TWAP / reference price update arrives.
     *
     * @param update the latest TWAP update
     */
    void onTwapUpdate(TwapUpdate update);

    /**
     * Callback triggered upon execution feedback (fills, rejections, cancellations).
     *
     * @param report the execution report
     */
    void onExecutionReport(ExecutionReport report);
}
