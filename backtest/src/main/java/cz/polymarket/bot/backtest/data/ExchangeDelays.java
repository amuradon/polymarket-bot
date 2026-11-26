package cz.polymarket.bot.backtest.data;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Immutable simulated one-way exchange WebSocket feed delays in milliseconds
 * used for indicator cache isolation and per-run engine configuration.
 *
 * @param binanceDelayMs simulated one-way feed delay for Binance in milliseconds
 * @param krakenDelayMs simulated one-way feed delay for Kraken in milliseconds
 * @param coinbaseDelayMs simulated one-way feed delay for Coinbase in milliseconds
 */
public record ExchangeDelays(
        long binanceDelayMs,
        long krakenDelayMs,
        long coinbaseDelayMs
) {
    /**
     * Validates that all exchange feed delays are non-negative.
     */
    public ExchangeDelays {
        if (binanceDelayMs < 0L) {
            throw new IllegalArgumentException("binanceDelayMs must be non-negative");
        }
        if (krakenDelayMs < 0L) {
            throw new IllegalArgumentException("krakenDelayMs must be non-negative");
        }
        if (coinbaseDelayMs < 0L) {
            throw new IllegalArgumentException("coinbaseDelayMs must be non-negative");
        }
    }

    /**
     * Returns an unmodifiable map sorted deterministically by canonical delay keys
     * for indicator cache hashing and config metadata persistence.
     *
     * @return sorted map of delay key names to millisecond values
     */
    public Map<String, Long> toSortedMap() {
        Map<String, Long> map = new LinkedHashMap<>(3);
        map.put("binanceDelayMs", binanceDelayMs);
        map.put("coinbaseDelayMs", coinbaseDelayMs);
        map.put("krakenDelayMs", krakenDelayMs);
        return Collections.unmodifiableMap(map);
    }
}
