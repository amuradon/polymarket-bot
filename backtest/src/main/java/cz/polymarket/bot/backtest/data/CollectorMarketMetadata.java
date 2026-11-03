package cz.polymarket.bot.backtest.data;

import cz.polymarket.bot.strategy.TradeDirection;

import java.nio.file.Path;

/**
 * Metadata descriptor for a single 15-minute Polymarket binary market archive
 * captured by the data collector ({@code polymarket/<market>-updown-15m/<market>-updown-15m-<tStart>.parquet}).
 *
 * @param tStart candle start timestamp in epoch seconds UTC
 * @param tEnd candle end timestamp in epoch seconds UTC ({@code tStart + 900})
 * @param datetimeUtc ISO-8601 formatted start timestamp in UTC (e.g. {@code "2026-04-11T20:30:00Z"})
 * @param dateStr UTC date string in {@code yyyy-MM-dd} format
 * @param marketParquetFile path to the market Parquet file on disk
 * @param upTokenId Polymarket CLOB token ID corresponding to the UP ("Up" / YES) outcome
 * @param downTokenId Polymarket CLOB token ID corresponding to the DOWN ("Down" / NO) outcome
 * @param twapOpen opening 60-second Chainlink TWAP reference price in USD (or {@link Double#NaN} if determined dynamically)
 * @param twapClose closing 60-second Chainlink TWAP settlement price in USD (or {@link Double#NaN} if determined dynamically)
 * @param actualOutcome resolved binary outcome ({@link TradeDirection#UP} or {@link TradeDirection#DOWN})
 */
public record CollectorMarketMetadata(
        long tStart,
        long tEnd,
        String datetimeUtc,
        String dateStr,
        Path marketParquetFile,
        String upTokenId,
        String downTokenId,
        double twapOpen,
        double twapClose,
        TradeDirection actualOutcome
) {

    /**
     * Creates an initial discovered market descriptor from a filename timestamp without reading
     * any Parquet metadata. Market metadata is populated later via lazy-loading when the market is processed.
     *
     * @param tStart candle start timestamp in epoch seconds UTC
     * @param tEnd candle end timestamp in epoch seconds UTC ({@code tStart + 900})
     * @param datetimeUtc ISO-8601 formatted start timestamp in UTC
     * @param dateStr UTC date string in {@code yyyy-MM-dd} format
     * @param marketParquetFile path to the market Parquet file on disk
     * @return discovered market descriptor with unloaded metadata fields
     */
    public static CollectorMarketMetadata discovered(
            long tStart,
            long tEnd,
            String datetimeUtc,
            String dateStr,
            Path marketParquetFile) {
        return new CollectorMarketMetadata(
                tStart,
                tEnd,
                datetimeUtc,
                dateStr,
                marketParquetFile,
                null,
                null,
                Double.NaN,
                Double.NaN,
                null
        );
    }
}
