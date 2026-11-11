package cz.polymarket.bot.calculator;

/**
 * Zero-allocation multi-exchange spot & 60-second Time-Weighted Average Price (TWAP) calculator
 * emulating Chainlink Data Streams ({@code price.crypto} and {@code price.crypto.twap}).
 * <p>
 * Aggregates real-time and historical spot trades across three constituent venues:
 * <ol>
 *   <li><b>Binance Spot ({@code <ASSET>USDT})</b> converted to USD via Kraken Spot {@code USDT/USD}
 *       5-second time-weighted EMA (baseline liquidity weight {@code 0.45}),</li>
 *   <li><b>Coinbase Spot ({@code <ASSET>USD})</b> direct USD trades (baseline liquidity weight {@code 0.35}), and</li>
 *   <li><b>Kraken Spot ({@code <ASSET>USD})</b> direct USD trades (baseline liquidity weight {@code 0.20}).</li>
 * </ol>
 * <p>
 * Maintainable per worker/stream instance without heap allocations on the hot path.
 */
public final class CalculatedSpotTwapEngine {

    /**
     * Constituent spot exchanges contributing to the Chainlink-emulated USD spot price.
     */
    public enum SpotVenue {
        /** Binance Spot {@code <ASSET>USDT} converted via Kraken {@code USDT/USD} 5s EMA. */
        BINANCE(0, 0.45),
        /** Coinbase Spot {@code <ASSET>USD}. */
        COINBASE(1, 0.35),
        /** Kraken Spot {@code <ASSET>USD}. */
        KRAKEN(2, 0.20);

        private final int index;
        private final double baseWeight;

        SpotVenue(int index, double baseWeight) {
            this.index = index;
            this.baseWeight = baseWeight;
        }

        /**
         * Returns the dense array index ({@code 0..2}) for this venue.
         */
        public int index() {
            return index;
        }

        /**
         * Returns the baseline structural liquidity weight for this venue.
         */
        public double baseWeight() {
            return baseWeight;
        }
    }

    /** Time constant (5,000 ms) for the Kraken {@code USDT/USD} exponential moving average. */
    public static final double USDT_EMA_TAU_MS = 5_000.0;

    /** Short-horizon decay time constant (250 ms) for per-venue micro-VWAP smoothing. */
    public static final double MICRO_VWAP_TAU_MS = 250.0;

    /** Decay time constant (10,000 ms) for per-venue rolling volume tracking. */
    public static final double ROLLING_VOL_TAU_MS = 10_000.0;

    /** Decay time constant (5,000 ms) for cross-venue quote staleness weighting. */
    public static final double STALENESS_TAU_MS = 5_000.0;

    /** Maximum age (300,000 ms = 5 minutes) before a silent venue is excluded while fresher venues exist. */
    public static final long MAX_VENUE_AGE_MS = 300_000L;

    /** Minimum staleness weight floor for active venues. */
    private static final double MIN_STALENESS_FACTOR = 1e-4;

    /** Share of structural baseline weight in the venue liquidity blend (70%). */
    public static final double BASE_WEIGHT_SHARE = 0.70;

    /** Share of 10-second rolling volume share in the venue liquidity blend (30%). */
    public static final double VOLUME_WEIGHT_SHARE = 0.30;

    /** Relative deviation band (10 bps = 0.10%) around the 3-venue median before Cauchy outlier down-weighting applies. */
    public static final double OUTLIER_TOLERANCE_RATIO = 0.0010;

    /** Rolling window length (60,000 ms) for the calculated USD 60-second TWAP. */
    public static final long TWAP_WINDOW_MS = 60_000L;

    private static final int VENUE_COUNT = 3;
    private static final double[] BASE_WEIGHTS = {
            SpotVenue.BINANCE.baseWeight(),
            SpotVenue.COINBASE.baseWeight(),
            SpotVenue.KRAKEN.baseWeight()
    };

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

    private final double[] venuePriceUsd = { Double.NaN, Double.NaN, Double.NaN };
    private final double[] venueMicroVol = new double[VENUE_COUNT];
    private final double[] venueRollingVol = new double[VENUE_COUNT];
    private final long[] venueLastTsMs = { -1L, -1L, -1L };
    private final double[] scratchDecayedVol = new double[VENUE_COUNT];
    private final boolean[] scratchActive = new boolean[VENUE_COUNT];

    private double usdtUsdEma = 1.0;
    private long lastUsdtUsdTimestampMs = -1L;
    private boolean hasUsdtUsdSample = false;

    private double lastSpotPriceUsd = Double.NaN;
    private double lastTwap60sPriceUsd = Double.NaN;
    private long lastAssetTimestampMs = -1L;

    /**
     * Resets all internal venue, EMA, and 60s TWAP state.
     */
    public void reset() {
        head = 0;
        len = 0;
        windowAreaUsdMs = 0.0;
        for (int i = 0; i < VENUE_COUNT; i++) {
            venuePriceUsd[i] = Double.NaN;
            venueMicroVol[i] = 0.0;
            venueRollingVol[i] = 0.0;
            venueLastTsMs[i] = -1L;
            scratchDecayedVol[i] = 0.0;
            scratchActive[i] = false;
        }
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
            double alpha = 1.0 - Math.exp(-1.0 / USDT_EMA_TAU_MS);
            usdtUsdEma += alpha * (usdtUsdPrice - usdtUsdEma);
        }
        return usdtUsdEma;
    }

    /**
     * Processes a Binance Spot {@code <ASSET>USDT} trade with unit default quantity ({@code 1.0}).
     *
     * @param timestampMs receipt timestamp in milliseconds UTC
     * @param assetUsdtPrice Binance Spot {@code <ASSET>USDT} trade price
     * @return calculated 60-second TWAP in USD
     */
    public double onBinanceUsdtTrade(long timestampMs, double assetUsdtPrice) {
        return onVenueTrade(SpotVenue.BINANCE, timestampMs, assetUsdtPrice, 1.0);
    }

    /**
     * Processes a Binance Spot {@code <ASSET>USDT} trade, converts the price to USD using the
     * current 5-second {@code USDT/USD} EMA, updates multi-exchange spot aggregation, and
     * updates the continuous 60-second USD TWAP.
     *
     * @param timestampMs receipt timestamp in milliseconds UTC
     * @param assetUsdtPrice Binance Spot {@code <ASSET>USDT} trade price
     * @param quantity executed trade quantity in base asset units
     * @return calculated 60-second TWAP in USD
     */
    public double onBinanceUsdtTrade(long timestampMs, double assetUsdtPrice, double quantity) {
        return onVenueTrade(SpotVenue.BINANCE, timestampMs, assetUsdtPrice, quantity);
    }

    /**
     * Processes a Coinbase Spot {@code <ASSET>USD} trade, updates multi-exchange spot aggregation,
     * and updates the continuous 60-second USD TWAP.
     *
     * @param timestampMs receipt timestamp in milliseconds UTC
     * @param assetUsdPrice Coinbase Spot {@code <ASSET>USD} trade price
     * @param quantity executed trade quantity in base asset units
     * @return calculated 60-second TWAP in USD
     */
    public double onCoinbaseUsdTrade(long timestampMs, double assetUsdPrice, double quantity) {
        return onVenueTrade(SpotVenue.COINBASE, timestampMs, assetUsdPrice, quantity);
    }

    /**
     * Processes a Kraken Spot {@code <ASSET>USD} trade, updates multi-exchange spot aggregation,
     * and updates the continuous 60-second USD TWAP.
     *
     * @param timestampMs receipt timestamp in milliseconds UTC
     * @param assetUsdPrice Kraken Spot {@code <ASSET>USD} trade price
     * @param quantity executed trade quantity in base asset units
     * @return calculated 60-second TWAP in USD
     */
    public double onKrakenUsdTrade(long timestampMs, double assetUsdPrice, double quantity) {
        return onVenueTrade(SpotVenue.KRAKEN, timestampMs, assetUsdPrice, quantity);
    }

    /**
     * Processes a spot trade from the specified constituent {@link SpotVenue}, updates the
     * Chainlink-emulated multi-exchange USD spot price, and integrates the continuous 60-second TWAP.
     *
     * @param venue constituent spot exchange
     * @param timestampMs receipt timestamp in milliseconds UTC
     * @param rawPrice raw trade price (in USDT for {@link SpotVenue#BINANCE}, in USD for {@link SpotVenue#COINBASE} and {@link SpotVenue#KRAKEN})
     * @param quantity executed trade quantity in base asset units
     * @return calculated 60-second TWAP in USD
     */
    public double onVenueTrade(SpotVenue venue, long timestampMs, double rawPrice, double quantity) {
        if (venue == null || Double.isNaN(rawPrice) || rawPrice <= 0.0 || timestampMs < 0L) {
            return lastTwap60sPriceUsd;
        }

        double qty = (Double.isNaN(quantity) || quantity <= 0.0) ? 1.0 : quantity;
        double tradePriceUsd = (venue == SpotVenue.BINANCE) ? (rawPrice * usdtUsdEma) : rawPrice;
        int vIdx = venue.index();

        long prevVenueTs = venueLastTsMs[vIdx];
        if (prevVenueTs < 0L) {
            venuePriceUsd[vIdx] = tradePriceUsd;
            venueMicroVol[vIdx] = qty;
            venueRollingVol[vIdx] = qty;
            venueLastTsMs[vIdx] = timestampMs;
        } else {
            long dtMs = Math.max(0L, timestampMs - prevVenueTs);
            double microDecay = dtMs >= 5_000L ? 0.0 : Math.exp(-dtMs / MICRO_VWAP_TAU_MS);
            double newMicroVol = venueMicroVol[vIdx] * microDecay + qty;
            venuePriceUsd[vIdx] += (qty / newMicroVol) * (tradePriceUsd - venuePriceUsd[vIdx]);
            venueMicroVol[vIdx] = newMicroVol;

            double volDecay = dtMs >= 100_000L ? 0.0 : Math.exp(-dtMs / ROLLING_VOL_TAU_MS);
            venueRollingVol[vIdx] = venueRollingVol[vIdx] * volDecay + qty;
            if (timestampMs > prevVenueTs) {
                venueLastTsMs[vIdx] = timestampMs;
            }
        }

        long effectiveTsMs = Math.max(timestampMs, lastAssetTimestampMs);
        double spotUsd = computeAggregatedSpotPriceUsd(effectiveTsMs, vIdx);
        this.lastSpotPriceUsd = spotUsd;

        return updateTwapRingBuffer(effectiveTsMs, spotUsd);
    }

    private double computeAggregatedSpotPriceUsd(long evalTsMs, int latestVenueIdx) {
        int activeCount = 0;
        int singleActiveIdx = latestVenueIdx;
        double activeBaseWeightSum = 0.0;
        double totalDecayedVol = 0.0;

        for (int i = 0; i < VENUE_COUNT; i++) {
            long vTs = venueLastTsMs[i];
            if (vTs >= 0L && (evalTsMs - vTs) <= MAX_VENUE_AGE_MS) {
                scratchActive[i] = true;
                activeCount++;
                singleActiveIdx = i;
                activeBaseWeightSum += BASE_WEIGHTS[i];
                long ageMs = Math.max(0L, evalTsMs - vTs);
                double decVol = venueRollingVol[i] * (ageMs >= 100_000L ? 0.0 : Math.exp(-ageMs / ROLLING_VOL_TAU_MS));
                scratchDecayedVol[i] = decVol;
                totalDecayedVol += decVol;
            } else {
                scratchActive[i] = false;
                scratchDecayedVol[i] = 0.0;
            }
        }

        if (activeCount <= 1) {
            return venuePriceUsd[singleActiveIdx];
        }

        double refMedian = activeCount == 3
                ? median3(venuePriceUsd[0], venuePriceUsd[1], venuePriceUsd[2])
                : 0.0;
        double toleranceUsd = refMedian * OUTLIER_TOLERANCE_RATIO;

        double weightedPriceSum = 0.0;
        double weightSum = 0.0;

        for (int i = 0; i < VENUE_COUNT; i++) {
            if (!scratchActive[i]) {
                continue;
            }
            long ageMs = Math.max(0L, evalTsMs - venueLastTsMs[i]);
            double staleFactor = ageMs >= 50_000L
                    ? MIN_STALENESS_FACTOR
                    : Math.max(MIN_STALENESS_FACTOR, Math.exp(-ageMs / STALENESS_TAU_MS));

            double normBaseW = BASE_WEIGHTS[i] / activeBaseWeightSum;
            double volShare = totalDecayedVol > 0.0 ? (scratchDecayedVol[i] / totalDecayedVol) : normBaseW;
            double liqWeight = BASE_WEIGHT_SHARE * normBaseW + VOLUME_WEIGHT_SHARE * volShare;

            double outlierFactor = 1.0;
            if (activeCount == 3 && toleranceUsd > 0.0) {
                double dev = Math.abs(venuePriceUsd[i] - refMedian);
                if (dev > toleranceUsd) {
                    double ratio = (dev - toleranceUsd) / toleranceUsd;
                    outlierFactor = 1.0 / (1.0 + ratio * ratio);
                }
            }

            double w = staleFactor * liqWeight * outlierFactor;
            weightedPriceSum += w * venuePriceUsd[i];
            weightSum += w;
        }

        return weightSum > 0.0 ? (weightedPriceSum / weightSum) : venuePriceUsd[latestVenueIdx];
    }

    private static double median3(double a, double b, double c) {
        if (a > b) {
            if (b > c) {
                return b;
            }
            return Math.min(a, c);
        } else {
            if (a > c) {
                return a;
            }
            return Math.min(b, c);
        }
    }

    private double updateTwapRingBuffer(long effectiveTsMs, double spotUsd) {
        if (len == 0) {
            timestampsMs[0] = effectiveTsMs;
            pricesUsd[0] = spotUsd;
            head = 0;
            len = 1;
            windowAreaUsdMs = 0.0;
            lastAssetTimestampMs = effectiveTsMs;
            lastTwap60sPriceUsd = spotUsd;
            return spotUsd;
        }

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
     * Returns the most recently calculated Chainlink-emulated USD spot price.
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
