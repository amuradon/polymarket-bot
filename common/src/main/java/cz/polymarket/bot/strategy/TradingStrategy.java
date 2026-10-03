package cz.polymarket.bot.strategy;

import cz.polymarket.bot.domain.MarketCandle;
import cz.polymarket.bot.domain.OrderBookQuote;
import cz.polymarket.bot.domain.TwapUpdate;

/**
 * Standard contract for trading strategies.
 * Implemented once in common/strategy layer, executing identically across
 * Backtesting, Paper Trading, and Live Trading without modification.
 */
public interface TradingStrategy {

    /**
     * Unique human-readable name of the strategy used for lookup and metrics reporting.
     */
    default String getName() {
        return getClass().getSimpleName();
    }

    /**
     * Resets the internal state of the strategy for a new run or test.
     */
    default void reset() {}

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

    /**
     * Optional callback triggered when a market candle completes.
     *
     * @param candle the completed market candle
     */
    default void onMarketCandleCompleted(MarketCandle candle) {}

    /**
     * Optional callback triggered when an order book quote arrives.
     *
     * @param quote the order book quote
     */
    default void onOrderBookQuote(OrderBookQuote quote) {}

    /**
     * Optional callback triggered when a market candle resolves at settlement.
     *
     * @param actualOutcome the final outcome direction
     */
    default void onCandleResolution(TradeDirection actualOutcome) {}
}

