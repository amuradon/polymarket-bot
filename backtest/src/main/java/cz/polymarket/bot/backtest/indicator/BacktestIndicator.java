package cz.polymarket.bot.backtest.indicator;

import cz.polymarket.bot.indicators.SpotTwapIndicator;
import cz.polymarket.bot.strategy.TradingStrategy;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * Service Provider Interface (SPI) for indicators participating in backtest Parquet caching
 * and single-pass multi-indicator raw exchange streaming.
 * <p>
 * When one or more registered {@link BacktestIndicator} implementations lack a daily cache file
 * for a given UTC day, {@link IndicatorCacheService} opens the raw exchange Parquet streams once,
 * feeds every delayed trade chronologically into each missing indicator's {@link ComputationSession},
 * and persists each indicator's daily Parquet cache file.
 */
public interface BacktestIndicator {

    /**
     * Returns the canonical indicator name used as the {@code <indicator>} directory name
     * under {@code D:\Crypto\data\Polymarket\cache\<indicator>\<hash>}.
     *
     * @return canonical indicator name
     */
    String getName();

    /**
     * Returns an immutable, deterministically ordered map of the indicator's configuration parameters
     * included in the cache hash and serialized to {@code config.json}.
     *
     * @return configuration parameter map
     */
    Map<String, Object> getConfigurationParameters();

    /**
     * Returns whether the given {@link TradingStrategy} consumes updates produced by this indicator.
     *
     * @param strategy target trading strategy
     * @return {@code true} if the strategy subscribes to this indicator's events
     */
    boolean isConsumedBy(TradingStrategy strategy);

    /**
     * Returns the pre-day spot trade warm-up duration in milliseconds required before {@code 00:00:00 UTC}.
     *
     * @return spot trade warm-up window in milliseconds
     */
    long getSpotWarmupMs();

    /**
     * Returns the pre-day Kraken {@code USDT/USD} peg trade warm-up duration in milliseconds
     * required before {@code 00:00:00 UTC}.
     *
     * @return Kraken {@code USDT/USD} warm-up window in milliseconds
     */
    long getUsdtPegWarmupMs();

    /**
     * Creates a new stateful per-day computation session for single-pass raw exchange trade streaming.
     *
     * @param cryptoSymbol normalized lowercase USD crypto symbol (e.g. {@code "btcusd"})
     * @return stateful {@link ComputationSession}
     */
    ComputationSession createComputationSession(String cryptoSymbol);

    /**
     * Reads a previously cached daily Parquet file from disk into an in-memory {@link DailyIndicatorTrack}.
     *
     * @param connection active DuckDB connection
     * @param parquetFile path to the cached daily Parquet file
     * @param cryptoSymbol normalized lowercase USD crypto symbol (e.g. {@code "btcusd"})
     * @return loaded {@link DailyIndicatorTrack}
     * @throws SQLException if reading the Parquet archive via DuckDB fails
     */
    DailyIndicatorTrack readCachedTrack(Connection connection, Path parquetFile, String cryptoSymbol) throws SQLException;

    /**
     * Stateful single-day indicator computation session receiving delayed raw exchange trades
     * in chronological order during a cache-miss computation pass.
     */
    interface ComputationSession {

        /**
         * Processes a delayed Kraken Spot {@code USDT/USD} trade.
         *
         * @param timestampMs simulated arrival timestamp ({@code server_ts + kraken_delay_ms}) in milliseconds UTC
         * @param usdtUsdPrice Kraken {@code USDT/USD} trade price
         */
        void onUsdtUsdTrade(long timestampMs, double usdtUsdPrice);

        /**
         * Processes a delayed constituent spot exchange trade and optionally records the resulting
         * indicator output if the trade falls within the target UTC day {@code [dayStartMs, dayEndMs]}.
         *
         * @param venue constituent spot exchange ({@code BINANCE}, {@code COINBASE}, {@code KRAKEN})
         * @param timestampMs simulated arrival timestamp ({@code server_ts + venue_delay_ms}) in milliseconds UTC
         * @param rawPrice raw trade price (in USDT for Binance, in USD for Coinbase and Kraken)
         * @param quantity executed trade quantity in base asset units
         * @param isTakerBuy {@code true} when the buyer was the trade aggressor
         * @param recordOutput {@code true} if {@code timestampMs >= dayStartMs} and the output should be cached for the day
         */
        void onSpotVenueTrade(
                SpotTwapIndicator.SpotVenue venue,
                long timestampMs,
                double rawPrice,
                double quantity,
                boolean isTakerBuy,
                boolean recordOutput);

        /**
         * Atomically writes all recorded daily indicator values to {@code targetParquetFile}
         * and returns the in-memory {@link DailyIndicatorTrack} for immediate use by the backtest engine.
         *
         * @param connection active DuckDB connection
         * @param targetParquetFile destination Parquet cache file path
         * @return in-memory {@link DailyIndicatorTrack} containing the day's computed values
         * @throws SQLException if DuckDB table creation, appending, or Parquet export fails
         * @throws IOException if directory creation or atomic file move fails
         */
        DailyIndicatorTrack writeParquetAndBuildTrack(Connection connection, Path targetParquetFile)
                throws SQLException, IOException;
    }
}
