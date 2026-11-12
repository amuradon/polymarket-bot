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
     * The engine dynamically computes and updates these indicators on-the-fly.
     *
     * @return the set of required indicator types
     */
    default Set<IndicatorType> getRequiredIndicators() {
        return Set.of();
    }

    /**
     * Declares the set of external (Binance, Deribit) market data streams required by this strategy.
     * <p>
     * If a strategy returns an empty set, the backtesting engine skips loading all external
     * Parquet files and skips computing indicators derived from external streams, streaming
     * exclusively Polymarket CLOB, {@code polymarket/price_crypto}, and
     * {@code polymarket/price_crypto_twap_60s} data.
     *
     * @return the set of required external stream types (empty by default)
     */
    default Set<ExternalStreamType> getRequiredExternalStreams() {
        return Set.of();
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
     * Optional callback triggered when a new aggregate TWAP / reference price update arrives.
     *
     * @param update the latest TWAP update
     */
    default void onTwapUpdate(TwapUpdate update) {}

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
     * Optional callback triggered when a Chainlink-emulated USD spot price and 60-second rolling TWAP
     * are calculated for the asset by aggregating Binance Spot ({@code <ASSET>USDT} converted via
     * Kraken {@code USDT/USD} 5s EMA), Coinbase Spot ({@code <ASSET>USD}), and Kraken Spot
     * ({@code <ASSET>USD}) trades.
     * <p>
     * Delivered automatically to all strategies without requiring explicit stream declaration.
     *
     * @param timestampMs local EC2 receipt timestamp in milliseconds UTC
     * @param symbol lowercase USD asset symbol (e.g. {@code "btcusd"})
     * @param spotPrice calculated multi-exchange USD spot price
     * @param twap60sPrice calculated 60-second time-weighted average USD price
     */
    default void onCalculatedSpotTwap(
            long timestampMs,
            String symbol,
            double spotPrice,
            double twap60sPrice) {}

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
     * @param timestampMs event receipt timestamp in milliseconds UTC
     * @param price execution price
     * @param quantity trade volume
     * @param isBuyerMaker true if maker was a buyer (aggressive sell), false if aggressive buy
     */
    default void onBinanceSpotTrade(long timestampMs, double price, double quantity, boolean isBuyerMaker) {}

    /**
     * Optional callback triggered when an aggressive futures trade occurs on Binance.
     *
     * @param timestampMs event receipt timestamp in milliseconds UTC
     * @param price execution price
     * @param quantity trade volume
     * @param isBuyerMaker true if maker was a buyer (aggressive sell), false if aggressive buy
     */
    default void onBinanceFuturesTrade(long timestampMs, double price, double quantity, boolean isBuyerMaker) {}

    /**
     * Optional callback triggered when Binance Futures order book depth is updated.
     *
     * @param timestampMs update receipt timestamp in milliseconds UTC
     * @param bestBid best bid price
     * @param bestAsk best ask price
     * @param depthBids total depth of top bid levels
     * @param depthAsks total depth of top ask levels
     * @param obi order book imbalance (bids - asks) / (bids + asks)
     */
    default void onBinanceFuturesOrderBook(long timestampMs, double bestBid, double bestAsk, double depthBids, double depthAsks, double obi) {}

    /**
     * Optional callback triggered when a liquidation force-order occurs on Binance Futures.
     *
     * @param timestampMs event receipt timestamp in milliseconds UTC
     * @param side liquidation order side ({@code "BUY"} or {@code "SELL"})
     * @param price limit price of the force order
     * @param averagePrice average fill price of the force order
     * @param quantity total liquidated quantity
     */
    default void onBinanceFuturesForceOrder(
            long timestampMs,
            String side,
            double price,
            double averagePrice,
            double quantity) {}

    /**
     * Optional callback triggered when a perpetual futures trade occurs on Deribit.
     *
     * @param timestampMs event receipt timestamp in milliseconds UTC
     * @param instrumentName Deribit instrument name (e.g. {@code "BTC-PERPETUAL"})
     * @param direction trade direction ({@code "buy"} or {@code "sell"})
     * @param price trade execution price
     * @param amount trade amount in USD/contracts
     * @param markPrice current Deribit mark price
     * @param indexPrice current Deribit index price
     */
    default void onDeribitFuturesTrade(
            long timestampMs,
            String instrumentName,
            String direction,
            double price,
            double amount,
            double markPrice,
            double indexPrice) {}

    /**
     * Optional callback triggered when a technical indicator value is updated.
     *
     * @param type indicator type
     * @param value updated value
     * @param timestampMs timestamp in milliseconds UTC
     */
    default void onIndicatorUpdate(IndicatorType type, double value, long timestampMs) {}
}
