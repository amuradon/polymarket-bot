package cz.polymarket.bot.backtest.data;

import java.util.Map;

/**
 * Fully resolved immutable configuration snapshot of an executed backtest run,
 * capturing both engine parameters and effective strategy parameters/overrides.
 *
 * @param strategyName evaluated trading strategy name
 * @param symbol evaluated cryptocurrency symbol
 * @param startDate inclusive UTC start date ({@code yyyy-MM-dd})
 * @param endDate inclusive UTC end date ({@code yyyy-MM-dd})
 * @param initialCapital initial account capital in USD
 * @param latencyFactor simulated order latency scaling factor
 * @param binanceDelayMs simulated Binance one-way feed delay in milliseconds
 * @param krakenDelayMs simulated Kraken one-way feed delay in milliseconds
 * @param coinbaseDelayMs simulated Coinbase one-way feed delay in milliseconds
 * @param strategyParams complete effective strategy parameters map
 * @param strategyOverrides subset of strategy parameters overridden relative to defaults
 */
public record BacktestRunConfig(
        String strategyName,
        String symbol,
        String startDate,
        String endDate,
        double initialCapital,
        double latencyFactor,
        long binanceDelayMs,
        long krakenDelayMs,
        long coinbaseDelayMs,
        Map<String, Object> strategyParams,
        Map<String, Object> strategyOverrides
) {
}
