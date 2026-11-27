package cz.polymarket.bot.backtest.data;

import org.eclipse.microprofile.openapi.annotations.media.Schema;

import java.util.Map;

/**
 * Request payload containing parameters for launching a backtest simulation run.
 *
 * @param label                 optional human-readable label identifying the run in history and comparison views
 * @param strategyName          optional name of the registered trading strategy to evaluate (defaults to configured strategy)
 * @param symbol                optional cryptocurrency symbol (defaults to BTCUSDT if omitted)
 * @param startDate             optional start date filter in ISO format (yyyy-MM-dd)
 * @param endDate               optional end date filter in ISO format (yyyy-MM-dd)
 * @param fullRange             when true, evaluates the full available dataset date range for the requested symbol
 * @param initialCapital        optional initial starting capital in USD (defaults to configured initial capital)
 * @param orderGatewayDelayMs   optional base Polymarket CLOB {@code POST /order} gateway round-trip delay in ms (default {@code 220})
 * @param binanceDelayMs        optional simulated Binance one-way feed delay in ms
 * @param krakenDelayMs         optional simulated Kraken one-way feed delay in ms
 * @param coinbaseDelayMs       optional simulated Coinbase one-way feed delay in ms
 * @param strategyParams        optional strategy parameter overrides keyed by parameter name
 * @param forceNewRun           when true, creates a new separate run even if an identical configuration already exists
 * @param overwriteExistingDays when true, re-simulates and overwrites UTC days already stored in a matching run
 */
@Schema(name = "BacktestRunRequest", description = "Parameters for executing a backtest simulation run")
public record BacktestRunRequest(
        @Schema(description = "Optional custom label for the backtest run", examples = "OraclePulse v2 tight spread")
        String label,

        @Schema(description = "Optional trading strategy name to execute (default OraclePulseLockStrategy)", examples = "OraclePulseLockStrategy")
        String strategyName,

        @Schema(description = "Optional symbol to evaluate (default BTCUSDT)", examples = "BTCUSDT")
        String symbol,

        @Schema(description = "Optional start date filter (yyyy-MM-dd)", examples = "2026-10-12")
        String startDate,

        @Schema(description = "Optional end date filter (yyyy-MM-dd)", examples = "2026-11-11")
        String endDate,

        @Schema(description = "When true, runs across the entire available dataset date range for the symbol", examples = "false")
        Boolean fullRange,

        @Schema(description = "Optional initial capital in USD", examples = "10000.0")
        Double initialCapital,

        @Schema(description = "Optional Polymarket CLOB order gateway round-trip delay in milliseconds (default 220)", examples = "220")
        Long orderGatewayDelayMs,

        @Schema(description = "Optional Binance one-way feed delay in milliseconds", examples = "100")
        Long binanceDelayMs,

        @Schema(description = "Optional Kraken one-way feed delay in milliseconds", examples = "50")
        Long krakenDelayMs,

        @Schema(description = "Optional Coinbase one-way feed delay in milliseconds", examples = "50")
        Long coinbaseDelayMs,

        @Schema(description = "Optional map of strategy parameter overrides")
        Map<String, Object> strategyParams,

        @Schema(description = "When true, creates a separate new run instead of merging into a matching existing run", examples = "false")
        Boolean forceNewRun,

        @Schema(description = "When true, overwrites UTC days already present in the matching run instead of skipping them", examples = "false")
        Boolean overwriteExistingDays
) {
}
