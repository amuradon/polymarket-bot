package cz.polymarket.bot.backtest.data;

import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Request payload containing parameters for launching a backtest simulation run.
 *
 * @param strategyName mandatory name of the registered trading strategy to evaluate
 * @param symbol optional cryptocurrency symbol (defaults to BTCUSDT if omitted)
 * @param startDate optional start date filter in ISO format (yyyy-MM-dd)
 * @param endDate optional end date filter in ISO format (yyyy-MM-dd)
 * @param initialCapital optional initial starting capital in USD (defaults to 10000.0)
 * @param outputDirectory optional directory path where exported JSON reports are saved
 */
@Schema(name = "BacktestRunRequest", description = "Parameters for executing a backtest simulation run")
public record BacktestRunRequest(
        @Schema(description = "Mandatory trading strategy name to execute", example = "TWAPArbitrageStrategy", required = true)
        String strategyName,

        @Schema(description = "Optional symbol to evaluate (default BTCUSDT)", example = "BTCUSDT")
        String symbol,

        @Schema(description = "Optional start date filter (yyyy-MM-dd)", example = "2026-08-01")
        String startDate,

        @Schema(description = "Optional end date filter (yyyy-MM-dd)", example = "2026-08-05")
        String endDate,

        @Schema(description = "Optional initial capital in USD", example = "10000.0")
        Double initialCapital,

        @Schema(description = "Optional directory to store exported JSON results", example = "D:/Crypto/data/Polymarket/backtesting")
        String outputDirectory
) {}
