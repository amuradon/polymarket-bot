package cz.polymarket.bot.backtest.data;

import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Request payload containing parameters for launching a backtest simulation run.
 *
 * @param strategyName optional name of the registered trading strategy to evaluate (defaults to UtterParameciumStrategy)
 * @param symbol optional cryptocurrency symbol (defaults to BTCUSDT if omitted)
 * @param startDate optional start date filter in ISO format (yyyy-MM-dd, defaults to 2026-10-12)
 * @param endDate optional end date filter in ISO format (yyyy-MM-dd, defaults to current UTC date)
 * @param initialCapital optional initial starting capital in USD (defaults to 10000.0)
 * @param outputDirectory optional directory path where exported JSON reports are saved
 * @param latencyFactor optional multiplier for simulated order execution latency (defaults to 1.0)
 */
@Schema(name = "BacktestRunRequest", description = "Parameters for executing a backtest simulation run")
public record BacktestRunRequest(
        @Schema(description = "Optional trading strategy name to execute (default UtterParameciumStrategy)", examples = "UtterParameciumStrategy")
        String strategyName,

        @Schema(description = "Optional symbol to evaluate (default BTCUSDT)", examples = "BTCUSDT")
        String symbol,

        @Schema(description = "Optional start date filter (yyyy-MM-dd, default 2026-10-12)", examples = "2026-10-12")
        String startDate,

        @Schema(description = "Optional end date filter (yyyy-MM-dd, default current UTC date)", examples = "2026-11-11")
        String endDate,

        @Schema(description = "Optional initial capital in USD", examples = "10000.0")
        Double initialCapital,

        @Schema(description = "Optional directory to store exported JSON results", examples = "D:/Crypto/data/Polymarket/backtesting")
        String outputDirectory,

        @Schema(description = "Optional latency scaling factor (default 1.0)", examples = "1.0")
        Double latencyFactor
) {
    /**
     * Convenience constructor defaulting {@code latencyFactor} to {@code null} (uses engine default 1.0).
     *
     * @param strategyName mandatory name of the registered trading strategy
     * @param symbol optional cryptocurrency symbol
     * @param startDate optional start date filter (yyyy-MM-dd)
     * @param endDate optional end date filter (yyyy-MM-dd)
     * @param initialCapital optional initial capital in USD
     * @param outputDirectory optional output directory path
     */
    public BacktestRunRequest(
            String strategyName,
            String symbol,
            String startDate,
            String endDate,
            Double initialCapital,
            String outputDirectory) {
        this(strategyName, symbol, startDate, endDate, initialCapital, outputDirectory, null);
    }
}
