package cz.polymarket.bot.strategy;

import cz.polymarket.bot.domain.MarketCandle;
import cz.polymarket.bot.domain.OrderBookQuote;
import cz.polymarket.bot.domain.TwapUpdate;

import java.util.Set;

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
     * Declares the set of technical indicators required by this strategy.
     * The engine dynamically computes and updates these indicators on-the-fly and from cache.
     *
     * @return the set of required indicator types
     */
    default Set<IndicatorType> getRequiredIndicators() {
        return Set.of();
    }

    /**
     * Resets the internal state of the strategy for a new run or test.
     */
    default void reset() {}

    /**
     * The primary market timeframe for which this strategy evaluates TWAP candles.
     */
    default cz.polymarket.bot.domain.Timeframe getTimeframe() {
        return cz.polymarket.bot.domain.Timeframe.FIFTEEN_MINUTES;
    }

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

    /**
     * Optional callback triggered when an aggressive spot trade occurs on Binance.
     *
     * @param timestampMs transaction timestamp in milliseconds UTC
     * @param price execution price
     * @param quantity trade volume
     * @param isBuyerMaker true if maker was a buyer (aggressive sell), false if aggressive buy
     */
    default void onBinanceSpotTrade(long timestampMs, double price, double quantity, boolean isBuyerMaker) {}

    /**
     * Optional callback triggered when an aggressive futures trade occurs on Binance.
     *
     * @param timestampMs transaction timestamp in milliseconds UTC
     * @param price execution price
     * @param quantity trade volume
     * @param isBuyerMaker true if maker was a buyer (aggressive sell), false if aggressive buy
     */
    default void onBinanceFuturesTrade(long timestampMs, double price, double quantity, boolean isBuyerMaker) {}

    /**
     * Optional callback triggered when Binance Futures order book depth is updated.
     *
     * @param timestampMs update timestamp in milliseconds UTC
     * @param bestBid best bid price
     * @param bestAsk best ask price
     * @param depthBids total depth of top bid levels
     * @param depthAsks total depth of top ask levels
     * @param obi order book imbalance (bids - asks) / (bids + asks)
     */
    default void onBinanceFuturesOrderBook(long timestampMs, double bestBid, double bestAsk, double depthBids, double depthAsks, double obi) {}

    /**
     * Optional callback triggered when a technical indicator value is updated.
     *
     * @param type indicator type
     * @param value updated value
     * @param timestampMs timestamp in milliseconds UTC
     */
    default void onIndicatorUpdate(IndicatorType type, double value, long timestampMs) {}
}
