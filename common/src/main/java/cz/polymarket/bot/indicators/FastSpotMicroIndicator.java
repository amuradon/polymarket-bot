package cz.polymarket.bot.indicators;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Zero-allocation real-time spot microstructure indicator extracting low-latency direct-USD price
 * discovery and cross-exchange aggressor flow ahead of Chainlink {@code price.crypto}.
 * <p>
 * Computes three orthogonal primitives in {@code O(1)} time per trade without heap allocations:
 * <ol>
 *   <li><b>Fast Direct-USD Micro-VWAP ({@code 40 ms})</b>: Blends {@code 50 ms}-latency Coinbase ({@code 0.70})
 *       and Kraken ({@code 0.30}) USD trades without contamination from {@code 100 ms}-delayed Binance USDT
 *       (falling back to Kraken-peg-adjusted Binance USD only if both direct-USD venues are silent
 *       beyond {@code directMaxSilenceMs}).</li>
 *   <li><b>Continuous-Time 3-Venue Signed OFI ({@code 1000 ms})</b>: Exponentially weighted net taker-buy
 *       minus taker-sell volume normalized into {@code [-1.0, +1.0]} across Coinbase, Kraken, and Binance Spot.</li>
 *   <li><b>3-Venue Synchronized Impulse ({@code 500 ms})</b>: Weighted USD deviation above/below each venue's
 *       {@code 500 ms} EMA emitted strictly when all three exchanges agree in sign.</li>
 * </ol>
 */
@ApplicationScoped
public class FastSpotMicroIndicator implements Indicator<FastSpotMicroListener> {

    /** Canonical indicator name used for cache directories and metadata descriptors. */
    public static final String INDICATOR_NAME = "FastSpotMicroIndicator";

    private static final int IDX_CB = 0;
    private static final int IDX_KR = 1;
    private static final int IDX_BN = 2;
    private static final int VENUE_COUNT = 3;

    private final FastSpotMicroIndicatorConfig config;
    private final double cbDirectWeight;
    private final double krDirectWeight;
    private final double[] ofiWeights;
    private final double[] syncWeights;
    private final double invUsdtEmaTauMs;
    private final double invMicroVwapTauMs;
    private final double invOfiTauMs;
    private final double ofiRegularizationUsd;
    private final double invSyncEmaTauMs;
    private final double invDirectStalenessTauMs;
    private final long directMaxSilenceMs;

    private final double[] fastPx = new double[VENUE_COUNT];
    private final double[] fastVol = new double[VENUE_COUNT];
    private final double[] ema500Px = new double[VENUE_COUNT];
    private final long[] venueLastTsMs = new long[VENUE_COUNT];

    private final List<FastSpotMicroListener> listeners = new ArrayList<>(4);

    private double usdtUsdEma = 1.0;
    private long lastUsdtTsMs = -1L;
    private double ofiSignedEma = 0.0;
    private double ofiTotalEma = 0.0;
    private long lastOfiTsMs = -1L;

    private long lastAssetTimestampMs = -1L;
    private double lastFastDirectUsd = Double.NaN;
    private double lastSignedOfi1s = 0.0;
    private double lastSyncMove500Usd = 0.0;

    /**
     * Constructs a {@link FastSpotMicroIndicator} with the injected configuration bean,
     * unwrapping any CDI proxy into primitive hot-path constants.
     *
     * @param config indicator configuration
     */
    @Inject
    public FastSpotMicroIndicator(FastSpotMicroIndicatorConfig config) {
        FastSpotMicroIndicatorConfig unwrapped = FastSpotMicroIndicatorConfig.unwrap(config);
        this.config = unwrapped;
        this.cbDirectWeight = unwrapped.coinbaseDirectWeight();
        this.krDirectWeight = unwrapped.krakenDirectWeight();
        this.ofiWeights = new double[]{
                unwrapped.coinbaseOfiWeight(),
                unwrapped.krakenOfiWeight(),
                unwrapped.binanceOfiWeight()
        };
        this.syncWeights = new double[]{
                unwrapped.coinbaseSyncWeight(),
                unwrapped.krakenSyncWeight(),
                unwrapped.binanceSyncWeight()
        };
        this.invUsdtEmaTauMs = 1.0 / unwrapped.usdtEmaTauMs();
        this.invMicroVwapTauMs = 1.0 / unwrapped.microVwapTauMs();
        this.invOfiTauMs = 1.0 / unwrapped.ofiTauMs();
        this.ofiRegularizationUsd = unwrapped.ofiRegularizationUsd();
        this.invSyncEmaTauMs = 1.0 / unwrapped.syncEmaTauMs();
        this.invDirectStalenessTauMs = 1.0 / unwrapped.directStalenessTauMs();
        this.directMaxSilenceMs = unwrapped.directMaxSilenceMs();
        reset();
    }

    @Override
    public String getName() {
        return INDICATOR_NAME;
    }

    @Override
    public Map<String, Object> getConfigurationParameters() {
        return config.toParameterMap();
    }

    /**
     * Returns the immutable configuration governing this indicator.
     *
     * @return {@link FastSpotMicroIndicatorConfig}
     */
    public FastSpotMicroIndicatorConfig getConfig() {
        return config;
    }

    /**
     * Registers a {@link FastSpotMicroListener} to receive microstructure updates.
     * Duplicate registrations are ignored.
     *
     * @param listener listener to register
     */
    @Override
    public void addListener(FastSpotMicroListener listener) {
        if (listener == null) {
            throw new IllegalArgumentException("listener cannot be null");
        }
        if (!listeners.contains(listener)) {
            listeners.add(listener);
        }
    }

    @Override
    public void removeListener(FastSpotMicroListener listener) {
        if (listener != null) {
            listeners.remove(listener);
        }
    }

    @Override
    public void clearListeners() {
        listeners.clear();
    }

    @Override
    public boolean hasListeners() {
        return !listeners.isEmpty();
    }

    @Override
    public void notifyListeners(long timestampMs, String symbol) {
        if (listeners.isEmpty() || Double.isNaN(lastFastDirectUsd) || lastFastDirectUsd <= 0.0) {
            return;
        }
        for (int i = 0, n = listeners.size(); i < n; i++) {
            listeners.get(i).onFastSpotMicro(
                    timestampMs, symbol, lastFastDirectUsd, lastSignedOfi1s, lastSyncMove500Usd);
        }
    }

    @Override
    public void reset() {
        Arrays.fill(fastPx, Double.NaN);
        Arrays.fill(fastVol, 0.0);
        Arrays.fill(ema500Px, Double.NaN);
        Arrays.fill(venueLastTsMs, -1L);
        usdtUsdEma = 1.0;
        lastUsdtTsMs = -1L;
        ofiSignedEma = 0.0;
        ofiTotalEma = 0.0;
        lastOfiTsMs = -1L;
        lastAssetTimestampMs = -1L;
        lastFastDirectUsd = Double.NaN;
        lastSignedOfi1s = 0.0;
        lastSyncMove500Usd = 0.0;
    }

    /**
     * Updates the continuous-time {@code USDT/USD} exponential moving average from a Kraken Spot trade.
     *
     * @param timestampMs  effective arrival timestamp in milliseconds UTC
     * @param usdtUsdPrice Kraken {@code USDT/USD} trade price
     */
    public void onUsdtUsdTrade(long timestampMs, double usdtUsdPrice) {
        if (!Double.isFinite(usdtUsdPrice) || usdtUsdPrice <= 0.0 || timestampMs <= 0L) {
            return;
        }
        long effTs = lastUsdtTsMs > 0L ? Math.max(timestampMs, lastUsdtTsMs) : timestampMs;
        if (lastUsdtTsMs < 0L) {
            usdtUsdEma = usdtUsdPrice;
        } else {
            long dt = effTs - lastUsdtTsMs;
            double alpha = dt >= 50_000L ? 1.0 : (1.0 - Math.exp(-dt * invUsdtEmaTauMs));
            usdtUsdEma += alpha * (usdtUsdPrice - usdtUsdEma);
        }
        lastUsdtTsMs = effTs;
    }

    /**
     * Processes a Coinbase Spot direct-USD trade.
     *
     * @param timestampMs effective arrival timestamp in milliseconds UTC
     * @param usdPrice    trade price in USD
     * @param quantity    executed base-asset quantity
     * @param isTakerBuy  {@code true} when buyer is aggressor (Coinbase {@code match} maker {@code side == "sell"})
     * @return updated {@code fastDirectUsd} price, or {@link Double#NaN} if invalid
     */
    public double onCoinbaseUsdTrade(long timestampMs, double usdPrice, double quantity, boolean isTakerBuy) {
        return updateVenue(IDX_CB, timestampMs, usdPrice, quantity, isTakerBuy);
    }

    /**
     * Processes a Kraken Spot direct-USD trade.
     *
     * @param timestampMs effective arrival timestamp in milliseconds UTC
     * @param usdPrice    trade price in USD
     * @param quantity    executed base-asset quantity
     * @param isTakerBuy  {@code true} when buyer is aggressor (Kraken {@code side == "buy"})
     * @return updated {@code fastDirectUsd} price, or {@link Double#NaN} if invalid
     */
    public double onKrakenUsdTrade(long timestampMs, double usdPrice, double quantity, boolean isTakerBuy) {
        return updateVenue(IDX_KR, timestampMs, usdPrice, quantity, isTakerBuy);
    }

    /**
     * Processes a Binance Spot {@code <ASSET>USDT} trade converted to USD via the Kraken {@code USDT/USD} EMA.
     *
     * @param timestampMs effective arrival timestamp in milliseconds UTC
     * @param usdtPrice   raw Binance trade price in USDT
     * @param quantity    executed base-asset quantity
     * @param isTakerBuy  {@code true} when buyer is aggressor (Binance {@code !isBuyerMaker})
     * @return updated {@code fastDirectUsd} price, or {@link Double#NaN} if invalid
     */
    public double onBinanceUsdtTrade(long timestampMs, double usdtPrice, double quantity, boolean isTakerBuy) {
        if (!Double.isFinite(usdtPrice) || usdtPrice <= 0.0) {
            return Double.NaN;
        }
        return updateVenue(IDX_BN, timestampMs, usdtPrice * usdtUsdEma, quantity, isTakerBuy);
    }

    /**
     * Generic entry point dispatching a constituent {@link SpotTwapIndicator.SpotVenue} trade.
     *
     * @param venue       constituent spot venue
     * @param timestampMs effective arrival timestamp in milliseconds UTC
     * @param rawPrice    raw trade price (USDT for Binance, USD for Coinbase and Kraken)
     * @param quantity    executed base-asset quantity
     * @param isTakerBuy  {@code true} when buyer is the aggressor
     * @return updated {@code fastDirectUsd} price, or {@link Double#NaN} if invalid
     */
    public double onVenueTrade(
            SpotTwapIndicator.SpotVenue venue,
            long timestampMs,
            double rawPrice,
            double quantity,
            boolean isTakerBuy) {
        if (venue == null) {
            return Double.NaN;
        }
        return switch (venue) {
            case COINBASE -> onCoinbaseUsdTrade(timestampMs, rawPrice, quantity, isTakerBuy);
            case KRAKEN -> onKrakenUsdTrade(timestampMs, rawPrice, quantity, isTakerBuy);
            case BINANCE -> onBinanceUsdtTrade(timestampMs, rawPrice, quantity, isTakerBuy);
        };
    }

    private double updateVenue(int vIdx, long timestampMs, double priceUsd, double quantity, boolean isTakerBuy) {
        if (!Double.isFinite(priceUsd) || priceUsd <= 0.0
                || !Double.isFinite(quantity) || quantity <= 0.0
                || timestampMs <= 0L) {
            return Double.NaN;
        }

        long effTs = lastAssetTimestampMs > 0L ? Math.max(timestampMs, lastAssetTimestampMs) : timestampMs;
        lastAssetTimestampMs = effTs;

        long prevVenueTs = venueLastTsMs[vIdx];
        if (prevVenueTs < 0L) {
            fastPx[vIdx] = priceUsd;
            fastVol[vIdx] = quantity;
            ema500Px[vIdx] = priceUsd;
        } else {
            long dtVenue = effTs - prevVenueTs;
            double dFast = dtVenue >= 2_000L ? 0.0 : Math.exp(-dtVenue * invMicroVwapTauMs);
            double prevWeightedVol = fastVol[vIdx] * dFast;
            double newVol = prevWeightedVol + quantity;
            fastVol[vIdx] = newVol;
            fastPx[vIdx] = newVol > 1e-12
                    ? (fastPx[vIdx] * prevWeightedVol + priceUsd * quantity) / newVol
                    : priceUsd;

            double alpha500 = dtVenue >= 10_000L ? 1.0 : (1.0 - Math.exp(-dtVenue * invSyncEmaTauMs));
            ema500Px[vIdx] += alpha500 * (priceUsd - ema500Px[vIdx]);
        }
        venueLastTsMs[vIdx] = effTs;

        // Update 3-venue continuous-time Signed OFI in USD notional with dust-trade regularization
        double weightedNotionalUsd = ofiWeights[vIdx] * quantity * priceUsd;
        double signedNotionalUsd = isTakerBuy ? weightedNotionalUsd : -weightedNotionalUsd;
        if (lastOfiTsMs < 0L) {
            ofiSignedEma = signedNotionalUsd;
            ofiTotalEma = weightedNotionalUsd;
        } else {
            long dtOfi = effTs - lastOfiTsMs;
            double dOfi = dtOfi >= 10_000L ? 0.0 : Math.exp(-dtOfi * invOfiTauMs);
            ofiSignedEma = ofiSignedEma * dOfi + signedNotionalUsd;
            ofiTotalEma = ofiTotalEma * dOfi + weightedNotionalUsd;
        }
        lastOfiTsMs = effTs;
        lastSignedOfi1s = Math.clamp(
                ofiSignedEma / Math.max(1e-6, ofiTotalEma + ofiRegularizationUsd),
                -1.0,
                1.0
        );

        // Compute fast Direct-USD price (Coinbase + Kraken; Binance fallback only if both direct venues silent)
        double directWSum = 0.0;
        double directPxSum = 0.0;
        if (venueLastTsMs[IDX_CB] >= 0L && (effTs - venueLastTsMs[IDX_CB]) <= directMaxSilenceMs) {
            double wCb = cbDirectWeight * Math.exp(-(effTs - venueLastTsMs[IDX_CB]) * invDirectStalenessTauMs);
            directWSum += wCb;
            directPxSum += wCb * fastPx[IDX_CB];
        }
        if (venueLastTsMs[IDX_KR] >= 0L && (effTs - venueLastTsMs[IDX_KR]) <= directMaxSilenceMs) {
            double wKr = krDirectWeight * Math.exp(-(effTs - venueLastTsMs[IDX_KR]) * invDirectStalenessTauMs);
            directWSum += wKr;
            directPxSum += wKr * fastPx[IDX_KR];
        }
        if (directWSum > 1e-6) {
            lastFastDirectUsd = directPxSum / directWSum;
        } else if (venueLastTsMs[IDX_BN] >= 0L) {
            lastFastDirectUsd = fastPx[IDX_BN];
        } else if (!Double.isNaN(fastPx[IDX_CB])) {
            lastFastDirectUsd = fastPx[IDX_CB];
        } else if (!Double.isNaN(fastPx[IDX_KR])) {
            lastFastDirectUsd = fastPx[IDX_KR];
        }

        // Compute 500ms 3-venue synchronized directional impulse
        double syncMove = 0.0;
        if (venueLastTsMs[IDX_CB] >= 0L && venueLastTsMs[IDX_KR] >= 0L && venueLastTsMs[IDX_BN] >= 0L
                && (effTs - venueLastTsMs[IDX_CB]) <= directMaxSilenceMs
                && (effTs - venueLastTsMs[IDX_KR]) <= directMaxSilenceMs
                && (effTs - venueLastTsMs[IDX_BN]) <= directMaxSilenceMs) {
            double d0 = fastPx[IDX_CB] - ema500Px[IDX_CB];
            double d1 = fastPx[IDX_KR] - ema500Px[IDX_KR];
            double d2 = fastPx[IDX_BN] - ema500Px[IDX_BN];
            if ((d0 > 0.0 && d1 > 0.0 && d2 > 0.0) || (d0 < 0.0 && d1 < 0.0 && d2 < 0.0)) {
                syncMove = syncWeights[IDX_CB] * d0 + syncWeights[IDX_KR] * d1 + syncWeights[IDX_BN] * d2;
            }
        }
        lastSyncMove500Usd = syncMove;

        return lastFastDirectUsd;
    }

    /** Returns the current Kraken {@code USDT/USD} EMA. */
    public double getUsdtUsdEma() {
        return usdtUsdEma;
    }

    /** Returns the most recently computed 40ms Direct-USD spot price. */
    public double getLastFastDirectUsd() {
        return lastFastDirectUsd;
    }

    /** Returns the most recently computed 1s 3-venue signed OFI in {@code [-1.0, +1.0]}. */
    public double getLastSignedOfi1s() {
        return lastSignedOfi1s;
    }

    /** Returns the most recently computed 500ms 3-venue synchronized impulse in USD. */
    public double getLastSyncMove500Usd() {
        return lastSyncMove500Usd;
    }

    /** Returns the effective timestamp of the last processed asset trade in ms UTC, or {@code -1}. */
    public long getLastAssetTimestampMs() {
        return lastAssetTimestampMs;
    }
}
