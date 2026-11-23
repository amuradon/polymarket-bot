package cz.polymarket.bot.backtest.indicator;

import java.util.Map;

/**
 * Immutable metadata descriptor persisted as {@code config.json} inside each indicator cache directory
 * ({@code D:\Crypto\data\Polymarket\cache\<indicator>\<hash>\config.json}).
 *
 * @param indicator canonical indicator name (e.g. {@code "SpotTwapIndicator"})
 * @param hash deterministic hexadecimal hash derived from {@code indicator}, {@code exchangeDelaysMs}, and {@code parameters}
 * @param exchangeDelaysMs lexicographically sorted map of simulated one-way exchange stream delays in milliseconds
 * @param parameters lexicographically sorted map of indicator algorithm parameters
 */
public record IndicatorCacheConfigDescriptor(
        String indicator,
        String hash,
        Map<String, Long> exchangeDelaysMs,
        Map<String, Object> parameters
) {}
