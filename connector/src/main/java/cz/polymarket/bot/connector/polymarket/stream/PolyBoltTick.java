package cz.polymarket.bot.connector.polymarket.stream;

/**
 * Normalized tick parsed from the Polymarket PolyBolt v2 WebSocket stream
 * ({@code price.crypto} or {@code price.crypto.twap}).
 *
 * @param timestampMs       effective timestamp in epoch milliseconds (oracle timestamp for snapshot items, local receipt timestamp for live ticks)
 * @param oracleTimestampMs oracle Unix timestamp in epoch milliseconds
 * @param symbol            normalized PolyBolt symbol (e.g. {@code "btcusd"})
 * @param source            oracle source identifier (e.g. {@code "chainlink"})
 * @param price             oracle price in USD
 * @param isTwap            {@code true} if parsed from {@code price.crypto.twap}, {@code false} if from {@code price.crypto}
 * @param windowSeconds     TWAP window in seconds (e.g. {@code 60} for {@code price.crypto.twap}, {@code 0} for spot)
 */
public record PolyBoltTick(
        long timestampMs,
        long oracleTimestampMs,
        String symbol,
        String source,
        double price,
        boolean isTwap,
        int windowSeconds
) {
}
