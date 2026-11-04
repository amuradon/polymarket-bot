package cz.polymarket.bot.calculator;

/**
 * Zero-allocation calculator computing a synthetic USD spot price and continuous 60-second
 * Time-Weighted Average Price (TWAP) for a crypto asset from:
 * <ol>
 *   <li>Binance Spot {@code <ASSET>USDT} trades, and</li>
 *   <li>Kraken Spot {@code USDT/USD} trades smoothed via a 5-second time-weighted EMA.</li>
 * </ol>
 * <p>
 * Maintainable per worker/stream instance without heap allocations on the hot path.
 */
public final class CalculatedSpotTwapEngine {

    /** Time constant (5,000 ms) for the Kraken {@code USDT/USD} exponential moving average. */
    public static final double USDT_EMA_TAU_MS = 5_000.0;

    /** Rolling window length (60,000 ms) for the calculated USD 60-second TWAP. */
    public static final long TWAP_WINDOW_MS = 60_000L;

    /**
     * Power-of-two circular buffer capacity (65,536 entries).
     * Since trades arriving within the same millisecond are coalesced in place, at most
     * 60,001 distinct millisecond timestamps can exist inside a 60,000 ms window.
     */
    private static final int CAPACITY = 65_536;
    private static final int MASK = CAPACITY - 1;

    private final long[] timestampsMs = new long[CAPACITY];
    private final double[] pricesUsd = new double[CAPACITY];
    private int head = 0;
    private int len = 0;
    private double windowAreaUsdMs = 0.0;

    private double usdtUsdEma = 1.0;
    private long lastUsdtUsdTimestampMs = -1L;
    private boolean hasUsdtUsdSample = false;

    private double lastSpotPriceUsd = Double.NaN;
    private double lastTwap60sPriceUsd = Double.NaN;
    private long lastAssetTimestampMs = -1L;

    /**
     * Resets all internal EMA and 60s TWAP state.
     */
    public void reset() {
        head = 0;
        len = 0;
        windowAreaUsdMs = 0.0;
        usdtUsdEma = 1.0;
        lastUsdtUsdTimestampMs = -1L;
        hasUsdtUsdSample = false;
        lastSpotPriceUsd = Double.NaN;
        lastTwap60sPriceUsd = Double.NaN;
        lastAssetTimestampMs = -1L;
    }

    /**
     * Updates the 5-second time-weighted EMA of the {@code USDT/USD} peg from a Kraken spot trade.
     *
     * @param timestampMs receipt timestamp in milliseconds UTC
     * @param usdtUsdPrice Kraken {@code USDT/USD} trade price
     * @return updated 5-second {@code USDT/USD} EMA
     */
    public double onUsdtUsdTrade(long timestampMs, double usdtUsdPrice) {
        if (Double.isNaN(usdtUsdPrice) || usdtUsdPrice <= 0.0 || timestampMs < 0L) {
            return usdtUsdEma;
        }
        if (!hasUsdtUsdSample) {
            usdtUsdEma = usdtUsdPrice;
            lastUsdtUsdTimestampMs = timestampMs;
            hasUsdtUsdSample = true;
            return usdtUsdEma;
        }
        long dtMs = Math.max(0L, timestampMs - lastUsdtUsdTimestampMs);
        if (dtMs > 0L) {
            double alpha = dtMs >= 50_000L ? 1.0 : (1.0 - Math.exp(-dtMs / USDT_EMA_TAU_MS));
            usdtUsdEma += alpha * (usdtUsdPrice - usdtUsdEma);
            lastUsdtUsdTimestampMs = timestampMs;
        } else {
            // Same-millisecond trade: apply minimal tick smoothing equivalent to 1ms step
            double alpha = 1.0 - Math.exp(-1.0 / USDT_EMA_TAU_MS);
            usdtUsdEma += alpha * (usdtUsdPrice - usdtUsdEma);
        }
        return usdtUsdEma;
    }

    /**
     * Processes a Binance Spot {@code <ASSET>USDT} trade, converts the price to USD using the
     * current 5-second {@code USDT/USD} EMA, and updates the continuous 60-second USD TWAP.
     *
     * @param timestampMs receipt timestamp in milliseconds UTC
     * @param assetUsdtPrice Binance Spot {@code <ASSET>USDT} trade price
     * @return calculated 60-second TWAP in USD
     */
    public double onAssetUsdtTrade(long timestampMs, double assetUsdtPrice) {
        if (Double.isNaN(assetUsdtPrice) || assetUsdtPrice <= 0.0 || timestampMs < 0L) {
            return lastTwap60sPriceUsd;
        }

        double spotUsd = assetUsdtPrice * usdtUsdEma;
        this.lastSpotPriceUsd = spotUsd;

        if (len == 0) {
            timestampsMs[0] = timestampMs;
            pricesUsd[0] = spotUsd;
            head = 0;
            len = 1;
            windowAreaUsdMs = 0.0;
            lastAssetTimestampMs = timestampMs;
            lastTwap60sPriceUsd = spotUsd;
            return spotUsd;
        }

        long effectiveTsMs = Math.max(timestampMs, lastAssetTimestampMs);
        int lastIdx = (head + len - 1) & MASK;
        long prevTsMs = timestampsMs[lastIdx];
        double prevPriceUsd = pricesUsd[lastIdx];

        if (effectiveTsMs == prevTsMs) {
            pricesUsd[lastIdx] = spotUsd;
        } else {
            windowAreaUsdMs += prevPriceUsd * (effectiveTsMs - prevTsMs);
            if (len == CAPACITY) {
                int secondIdx = (head + 1) & MASK;
                windowAreaUsdMs -= pricesUsd[head] * (timestampsMs[secondIdx] - timestampsMs[head]);
                head = secondIdx;
                len--;
            }
            int tailIdx = (head + len) & MASK;
            timestampsMs[tailIdx] = effectiveTsMs;
            pricesUsd[tailIdx] = spotUsd;
            len++;
        }
        lastAssetTimestampMs = effectiveTsMs;

        long windowStartMs = effectiveTsMs - TWAP_WINDOW_MS;
        while (len >= 2) {
            int nextIdx = (head + 1) & MASK;
            if (timestampsMs[nextIdx] <= windowStartMs) {
                windowAreaUsdMs -= pricesUsd[head] * (timestampsMs[nextIdx] - timestampsMs[head]);
                head = nextIdx;
                len--;
            } else {
                break;
            }
        }

        if (windowAreaUsdMs < 0.0) {
            windowAreaUsdMs = 0.0;
        }

        long oldestTsMs = timestampsMs[head];
        double activeArea = windowAreaUsdMs;
        long activeStartMs = oldestTsMs;
        if (oldestTsMs < windowStartMs && len >= 2) {
            activeArea -= pricesUsd[head] * (windowStartMs - oldestTsMs);
            activeStartMs = windowStartMs;
        }

        long durationMs = effectiveTsMs - activeStartMs;
        if (durationMs > 0L && activeArea > 0.0) {
            lastTwap60sPriceUsd = activeArea / durationMs;
        } else {
            lastTwap60sPriceUsd = spotUsd;
        }
        return lastTwap60sPriceUsd;
    }

    /**
     * Returns the current 5-second time-weighted EMA of the {@code USDT/USD} peg.
     */
    public double getUsdtUsdEma() {
        return usdtUsdEma;
    }

    /**
     * Returns the most recently calculated USD spot price ({@code assetUsdtPrice * usdtUsdEma}).
     */
    public double getLastSpotPriceUsd() {
        return lastSpotPriceUsd;
    }

    /**
     * Returns the most recently calculated 60-second time-weighted average USD price.
     */
    public double getLastTwap60sPriceUsd() {
        return lastTwap60sPriceUsd;
    }
}
