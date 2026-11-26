package cz.polymarket.bot.strategy;

import cz.polymarket.bot.domain.OrderBookQuote;

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
     * Declares the duration in milliseconds before candle expiration ({@code tEndMs}) during which
     * this strategy requires Polymarket CLOB order book quotes ({@link #onOrderBookQuote(OrderBookQuote)}).
     * <p>
     * Returns {@link Long#MAX_VALUE} by default so full-candle strategies receive CLOB quotes across
     * the entire market interval. Final-window strategies may return a bounded duration (while still
     * receiving {@code polymarket/price_crypto} and {@code polymarket/price_crypto_twap_60s} from
     * the very start of the market interval) so the backtesting engine can prune early CLOB row groups
     * after seeding from the latest full {@code book} snapshot.
     *
     * @return required CLOB lookback window before {@code tEndMs} in milliseconds
     */
    default long getRequiredClobLookbackMs() {
        return Long.MAX_VALUE;
    }

    /**
     * Resets the internal state of the strategy for a new run or test.
     */
    default void reset() {}

    /**
     * Creates a new isolated instance of this strategy for parallel backtest execution across
     * multiple markets simultaneously.
     * <p>
     * Strategies maintaining mutable per-market state should override this method to return
     * a fresh instance configured with the same immutable parameters and models.
     *
     * @return isolated strategy instance
     */
    default TradingStrategy createInstance() {
        return this;
    }

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
     * Optional callback triggered when a real-time crypto spot/oracle price update arrives
     * from the {@code polymarket/price_crypto} stream.
     *
     * @param timestampMs local EC2 receipt timestamp in milliseconds UTC
     * @param symbol crypto symbol (e.g. {@code "btcusd"})
     * @param source price feed source (e.g. {@code "chainlink"})
     * @param price spot/oracle price in USD
     * @param priceTimestampMs source oracle timestamp in milliseconds UTC
     */
    default void onPolymarketCryptoPrice(
            long timestampMs,
            String symbol,
            String source,
            double price,
            long priceTimestampMs) {}

    /**
     * Optional callback triggered when a real-time 60-second TWAP update arrives
     * from the {@code polymarket/price_crypto_twap_60s} stream.
     *
     * @param timestampMs local EC2 receipt timestamp in milliseconds UTC
     * @param symbol crypto symbol (e.g. {@code "btcusd"})
     * @param source TWAP feed source (e.g. {@code "chainlink"})
     * @param twapPrice 60-second time-weighted average price in USD
     * @param windowSeconds TWAP rolling window length in seconds (typically 60)
     * @param twapTimestampMs source oracle TWAP timestamp in milliseconds UTC
     */
    default void onPolymarketCryptoTwap60s(
            long timestampMs,
            String symbol,
            String source,
            double twapPrice,
            int windowSeconds,
            long twapTimestampMs) {}

    /**
     * Callback triggered upon execution feedback (fills, rejections, cancellations).
     *
     * @param report the execution report
     */
    void onExecutionReport(ExecutionReport report);

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
     * Returns the strategy's latest estimated probability for the {@link TradeDirection#UP} outcome in {@code (0.0, 1.0)},
     * or {@link Double#NaN} when no directional probability estimate is available yet.
     *
     * @return latest {@code UP} model probability, or {@link Double#NaN}
     */
    default double getLastModelProbabilityUp() {
        return Double.NaN;
    }
}
