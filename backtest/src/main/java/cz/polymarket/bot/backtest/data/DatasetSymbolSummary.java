package cz.polymarket.bot.backtest.data;

import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Summary of an available historical Polymarket collector dataset discovered on disk.
 *
 * @param symbol normalized trading symbol (e.g. {@code "BTCUSDT"})
 * @param baseAsset uppercase base asset code (e.g. {@code "BTC"})
 * @param minDate earliest available UTC date ({@code yyyy-MM-dd})
 * @param maxDate latest available UTC date ({@code yyyy-MM-dd})
 * @param marketCount total number of 15-minute market Parquet files available
 * @param dayCount total number of distinct UTC days covered
 */
@Schema(name = "DatasetSymbolSummary", description = "Discovered historical market dataset summary for a symbol.")
public record DatasetSymbolSummary(
        String symbol,
        String baseAsset,
        String minDate,
        String maxDate,
        int marketCount,
        int dayCount
) {
}
