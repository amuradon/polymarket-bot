package cz.polymarket.bot.backtest.engine;

import cz.polymarket.bot.backtest.data.BacktestRunConfig;
import cz.polymarket.bot.backtest.data.MarketEquityPoint;
import cz.polymarket.bot.calculator.PerformanceMetrics;
import cz.polymarket.bot.domain.TradeRecord;

import java.time.Instant;
import java.util.List;

/**
 * Encapsulates the complete backtest simulation outcome, including resolved configuration,
 * summary performance metrics, per-market equity progression, and individual trade records.
 *
 * @param runId unique persistent run identifier
 * @param label human-readable label for the run
 * @param strategyName name of the evaluated trading strategy
 * @param symbol evaluated trading symbol
 * @param executionTimeUtc timestamp when the simulation completed
 * @param durationMs simulation wall-clock duration in milliseconds
 * @param totalMarkets count of evaluated 15-minute market intervals
 * @param config resolved configuration snapshot of the run
 * @param metrics summary performance metrics (PnL, Sharpe, Brier, Win rate)
 * @param marketPoints per-market equity progression across all evaluated markets
 * @param trades complete list of executed trade records
 */
public record BacktestResult(
        String runId,
        String label,
        String strategyName,
        String symbol,
        Instant executionTimeUtc,
        long durationMs,
        int totalMarkets,
        BacktestRunConfig config,
        PerformanceMetrics metrics,
        List<MarketEquityPoint> marketPoints,
        List<TradeRecord> trades
) {
}
