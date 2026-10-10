package cz.polymarket.bot.connector.polymarket;

import java.util.List;

/**
 * Minimal immutable metadata record representing a Polymarket market discovered via the Gamma API.
 * Only extracts the fields strictly necessary for stream subscription and lifecycle management,
 * preserving the entire unmodified Gamma JSON payload in {@code rawJson}.
 *
 * @param slug         market URL slug (e.g. btc-updown-15m-1789041600)
 * @param startSec     market interval opening timestamp in epoch seconds
 * @param endSec       market interval closing/resolution timestamp in epoch seconds
 * @param clobTokenIds CLOB asset token IDs to subscribe on the market WebSocket stream
 * @param closed       whether the market is marked closed/resolved on Gamma API
 * @param rawJson      unmodified JSON object string returned by the Gamma API
 */
public record PolymarketMarketInfo(
        String slug,
        long startSec,
        long endSec,
        List<String> clobTokenIds,
        boolean closed,
        String rawJson
) {

    /**
     * Extracts the lowercase base asset prefix used in Polymarket market slugs (e.g. "BTCUSDT" -> "btc").
     *
     * @param symbol trading pair or asset symbol (e.g. "BTCUSDT", "BTC", "ETHUSDT")
     * @return lowercase base asset code (e.g. "btc")
     */
    public static String normalizeAssetSlugPrefix(String symbol) {
        if (symbol == null || symbol.isBlank()) {
            return "btc";
        }
        String clean = symbol.trim().toLowerCase()
                .replace("/usd", "")
                .replace("-usd", "")
                .replace("_usd", "");
        if (clean.endsWith("usdt") && clean.length() > 4) {
            clean = clean.substring(0, clean.length() - 4);
        } else if (clean.endsWith("usdc") && clean.length() > 4) {
            clean = clean.substring(0, clean.length() - 4);
        } else if (clean.endsWith("usd") && clean.length() > 3) {
            clean = clean.substring(0, clean.length() - 3);
        }
        return clean.isEmpty() ? "btc" : clean;
    }

    /**
     * Converts a trading symbol into the Polymarket PolyBolt symbol format (lowercase without slashes,
     * ending in {@code usd}, e.g. "BTCUSDT" -> "btcusd").
     *
     * @param symbol trading pair or asset symbol
     * @return lowercase PolyBolt symbol string (e.g. "btcusd")
     */
    public static String normalizePolyBoltSymbol(String symbol) {
        return normalizeAssetSlugPrefix(symbol) + "usd";
    }

    /**
     * Parses a timeframe code (such as "5m", "15m", "1h") into its duration in seconds.
     *
     * @param timeframe timeframe code string
     * @return duration in seconds
     */
    public static long parseTimeframeSeconds(String timeframe) {
        if (timeframe == null || timeframe.isBlank()) {
            return 900L;
        }
        String clean = timeframe.trim().toLowerCase();
        if (clean.endsWith("m")) {
            long minutes = Long.parseLong(clean.substring(0, clean.length() - 1));
            if (minutes <= 0) {
                throw new IllegalArgumentException("Timeframe minutes must be positive: " + timeframe);
            }
            return minutes * 60L;
        }
        if (clean.endsWith("h")) {
            long hours = Long.parseLong(clean.substring(0, clean.length() - 1));
            if (hours <= 0) {
                throw new IllegalArgumentException("Timeframe hours must be positive: " + timeframe);
            }
            return hours * 3600L;
        }
        if (clean.endsWith("s")) {
            long seconds = Long.parseLong(clean.substring(0, clean.length() - 1));
            if (seconds <= 0) {
                throw new IllegalArgumentException("Timeframe seconds must be positive: " + timeframe);
            }
            return seconds;
        }
        throw new IllegalArgumentException("Unsupported timeframe format: " + timeframe);
    }

    /**
     * Aligns an arbitrary epoch second timestamp down to the start of its timeframe cycle.
     *
     * @param epochSecond     timestamp in epoch seconds
     * @param durationSeconds timeframe duration in seconds (e.g. 900 for 15m)
     * @return aligned cycle start timestamp in epoch seconds
     */
    public static long alignCycleStart(long epochSecond, long durationSeconds) {
        if (durationSeconds <= 0) {
            throw new IllegalArgumentException("Duration seconds must be positive");
        }
        return (epochSecond / durationSeconds) * durationSeconds;
    }

    /**
     * Builds the deterministic Polymarket Gamma market slug for a given asset, timeframe, and cycle start.
     *
     * @param symbol    asset symbol (e.g. "BTCUSDT" or "BTC")
     * @param timeframe timeframe code (e.g. "15m")
     * @param startSec  aligned cycle start in epoch seconds
     * @return market slug (e.g. "btc-updown-15m-1789041600")
     */
    public static String buildMarketSlug(String symbol, String timeframe, long startSec) {
        String asset = normalizeAssetSlugPrefix(symbol);
        String tf = (timeframe == null || timeframe.isBlank()) ? "15m" : timeframe.trim().toLowerCase();
        return asset + "-updown-" + tf + "-" + startSec;
    }
}
