package cz.polymarket.bot.backtest.engine;

import com.fasterxml.jackson.annotation.JsonInclude;
import cz.polymarket.bot.calculator.PerformanceMetrics;
import cz.polymarket.bot.domain.TradeRecord;

import java.time.Instant;
import java.util.List;

/**
 * Encapsulates the complete backtest simulation outcome,
 * including summary performance metrics and all individual trade records.
 *
 * @param strategyName name of the evaluated trading strategy
 * @param symbol evaluated trading symbol
 * @param executionTimeUtc timestamp when the simulation was executed
 * @param totalMarkets count of evaluated 15-minute intervals
 * @param metrics summary performance metrics (PnL, Sharpe, Brier, Win rate)
 * @param trades complete list of executed trade records
 * @param jsonFilePath absolute path to exported JSON result file on disk
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record BacktestResult(
        String strategyName,
        String symbol,
        Instant executionTimeUtc,
        int totalMarkets,
        PerformanceMetrics metrics,
        List<TradeRecord> trades,
        String jsonFilePath
) {
    /**
     * Creates a copy of this result with an updated exported JSON file path.
     *
     * @param path absolute file path where JSON was saved
     * @return new BacktestResult with updated file path
     */
    public BacktestResult withJsonFilePath(String path) {
        return new BacktestResult(
                strategyName,
                symbol,
                executionTimeUtc,
                totalMarkets,
                metrics,
                trades,
                path
        );
    }
}
