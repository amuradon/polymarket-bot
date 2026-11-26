package cz.polymarket.bot.web;

import cz.polymarket.bot.calculator.PerformanceMetrics;
import cz.polymarket.bot.engine.Position;
import cz.polymarket.bot.engine.StrategyMode;

import java.util.List;

/**
 * Unified diagnostic status DTO combining Strategy runtime state, Data Collector status,
 * Paper Trading metrics, and 4-core / OS / WebSocket telemetry.
 *
 * @param healthy          overall application health flag
 * @param strategyMode     active {@link StrategyMode} ({@code LIVE}, {@code PAPER}, or {@code OFF})
 * @param strategyName     configured strategy name (e.g. {@code OraclePulseLockStrategy})
 * @param orderType        configured CLOB order time-in-force (e.g. {@code "FAK"})
 * @param orderLatencyMs   configured virtual order latency in milliseconds (used in {@code PAPER} mode)
 * @param activeMarketSlug currently tracked Polymarket market slug
 * @param yesTokenId       Up (YES) conditional token ID for the active market
 * @param noTokenId        Down (NO) conditional token ID for the active market
 * @param twapOpenPrice    captured 60-second Chainlink TWAP target price for the active candle
 * @param currentSpotPrice latest observed spot price in USD
 * @param depth1cUp        current effective 1-cent L2 ask depth for UP token in shares
 * @param depth1cDown      current effective 1-cent L2 ask depth for DOWN token in shares
 * @param initialCapital   initial bankroll in USD (CLOB collateral in {@code LIVE}, virtual capital in {@code PAPER})
 * @param currentBalance   current balance in USD
 * @param activePosition   most recent open position, or {@code null}
 * @param openPositions    list of all open positions in the active candle
 * @param paperMetrics     aggregated paper trading metrics
 * @param collector        Data Collector status snapshot
 * @param telemetry        system, 4-core thread, order execution, and WebSocket stream telemetry snapshot
 */
public record StatusDto(
        boolean healthy,
        StrategyMode strategyMode,
        String strategyName,
        String orderType,
        long orderLatencyMs,
        String activeMarketSlug,
        String yesTokenId,
        String noTokenId,
        double twapOpenPrice,
        double currentSpotPrice,
        double depth1cUp,
        double depth1cDown,
        double initialCapital,
        double currentBalance,
        Position activePosition,
        List<Position> openPositions,
        PerformanceMetrics paperMetrics,
        CollectorStatusDto collector,
        TelemetryDto telemetry
) {
}
