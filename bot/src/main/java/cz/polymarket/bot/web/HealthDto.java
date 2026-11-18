package cz.polymarket.bot.web;

import cz.polymarket.bot.engine.StrategyMode;

import java.util.List;

/**
 * Operational health check DTO evaluated by {@code GET /api/health} and deployment scripts
 * to decide whether a newly deployed release is healthy or must be rolled back to {@code .previous}.
 *
 * @param healthy          {@code true} if both Data Collector and the configured {@link StrategyMode} are healthy
 * @param strategyMode     current {@link StrategyMode} ({@code LIVE}, {@code PAPER}, or {@code OFF})
 * @param collectorHealthy whether Data Collector streams and writers are healthy
 * @param strategyHealthy  whether Strategy Engine streams/workers/queues satisfy the active {@link StrategyMode}
 * @param twapOpenPrice    current 15m market target price in USD (or {@code 0.0} if not yet locked)
 * @param issues           human-readable list of any failing health checks (empty when {@code healthy == true})
 */
public record HealthDto(
        boolean healthy,
        StrategyMode strategyMode,
        boolean collectorHealthy,
        boolean strategyHealthy,
        double twapOpenPrice,
        List<String> issues
) {
}
