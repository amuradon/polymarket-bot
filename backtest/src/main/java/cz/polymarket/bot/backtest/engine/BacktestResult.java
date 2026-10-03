package cz.polymarket.bot.backtest.engine;

import com.fasterxml.jackson.annotation.JsonInclude;
import cz.polymarket.bot.calculator.PerformanceMetrics;
import cz.polymarket.bot.domain.TradeRecord;

import java.time.Instant;
import java.util.List;

/**
 * Encapsulates the complete backtest simulation outcome,
 * including summary performance metrics and all individual trade records.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record BacktestResult(
        String strategyName,
        String datasetPath,
        Instant executionTimeUtc,
        int totalMarkets,
        PerformanceMetrics metrics,
        List<TradeRecord> trades,
        String jsonFilePath
) {
    public BacktestResult withJsonFilePath(String path) {
        return new BacktestResult(
                strategyName,
                datasetPath,
                executionTimeUtc,
                totalMarkets,
                metrics,
                trades,
                path
        );
    }
}
