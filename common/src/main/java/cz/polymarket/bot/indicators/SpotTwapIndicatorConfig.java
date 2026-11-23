package cz.polymarket.bot.indicators;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Immutable configuration bean for {@link SpotTwapIndicator}.
 * <p>
 * Encapsulates all structural venue liquidity weights, exponential decay time constants,
 * staleness bounds, outlier tolerance parameters, rolling TWAP window length, and backtest
 * warm-up durations so indicator settings can be deterministically hashed and serialized.
 */
@ApplicationScoped
public class SpotTwapIndicatorConfig {

    private final double binanceBaseWeight;
    private final double coinbaseBaseWeight;
    private final double krakenBaseWeight;
    private final double usdtEmaTauMs;
    private final double microVwapTauMs;
    private final double rollingVolTauMs;
    private final double stalenessTauMs;
    private final long maxVenueAgeMs;
    private final double minStalenessFactor;
    private final double baseWeightShare;
    private final double volumeWeightShare;
    private final double outlierToleranceRatio;
    private final long twapWindowMs;
    private final long spotWarmupMs;
    private final long krakenUsdtWarmupMs;
    private final Map<String, Object> parameterMap;

    /**
     * Constructs the {@link SpotTwapIndicator} configuration with MicroProfile properties.
     *
     * @param binanceBaseWeight baseline structural liquidity weight for Binance Spot (default {@code 0.45})
     * @param coinbaseBaseWeight baseline structural liquidity weight for Coinbase Spot (default {@code 0.35})
     * @param krakenBaseWeight baseline structural liquidity weight for Kraken Spot (default {@code 0.20})
     * @param usdtEmaTauMs time constant in milliseconds for Kraken {@code USDT/USD} EMA (default {@code 5000.0})
     * @param microVwapTauMs decay time constant in milliseconds for per-venue micro-VWAP smoothing (default {@code 250.0})
     * @param rollingVolTauMs decay time constant in milliseconds for per-venue rolling volume tracking (default {@code 10000.0})
     * @param stalenessTauMs decay time constant in milliseconds for cross-venue quote staleness weighting (default {@code 5000.0})
     * @param maxVenueAgeMs maximum age in milliseconds before a silent venue is excluded (default {@code 300000})
     * @param minStalenessFactor minimum staleness weight floor for active venues (default {@code 1e-4})
     * @param baseWeightShare share of structural baseline weight in venue liquidity blend (default {@code 0.70})
     * @param volumeWeightShare share of 10-second rolling volume share in venue liquidity blend (default {@code 0.30})
     * @param outlierToleranceRatio relative deviation band around 3-venue median before Cauchy down-weighting (default {@code 0.0010})
     * @param twapWindowMs rolling window length in milliseconds for calculated USD TWAP (default {@code 60000})
     * @param spotWarmupMs pre-day / pre-session spot trade warm-up window in milliseconds (default {@code 65000})
     * @param krakenUsdtWarmupMs pre-day / pre-session Kraken {@code USDT/USD} peg warm-up window in milliseconds (default {@code 1800000})
     */
    @Inject
    public SpotTwapIndicatorConfig(
            @ConfigProperty(name = "polymarket.indicator.spot-twap.binance-base-weight", defaultValue = "0.45")
            double binanceBaseWeight,
            @ConfigProperty(name = "polymarket.indicator.spot-twap.coinbase-base-weight", defaultValue = "0.35")
            double coinbaseBaseWeight,
            @ConfigProperty(name = "polymarket.indicator.spot-twap.kraken-base-weight", defaultValue = "0.20")
            double krakenBaseWeight,
            @ConfigProperty(name = "polymarket.indicator.spot-twap.usdt-ema-tau-ms", defaultValue = "5000.0")
            double usdtEmaTauMs,
            @ConfigProperty(name = "polymarket.indicator.spot-twap.micro-vwap-tau-ms", defaultValue = "250.0")
            double microVwapTauMs,
            @ConfigProperty(name = "polymarket.indicator.spot-twap.rolling-vol-tau-ms", defaultValue = "10000.0")
            double rollingVolTauMs,
            @ConfigProperty(name = "polymarket.indicator.spot-twap.staleness-tau-ms", defaultValue = "5000.0")
            double stalenessTauMs,
            @ConfigProperty(name = "polymarket.indicator.spot-twap.max-venue-age-ms", defaultValue = "300000")
            long maxVenueAgeMs,
            @ConfigProperty(name = "polymarket.indicator.spot-twap.min-staleness-factor", defaultValue = "0.0001")
            double minStalenessFactor,
            @ConfigProperty(name = "polymarket.indicator.spot-twap.base-weight-share", defaultValue = "0.70")
            double baseWeightShare,
            @ConfigProperty(name = "polymarket.indicator.spot-twap.volume-weight-share", defaultValue = "0.30")
            double volumeWeightShare,
            @ConfigProperty(name = "polymarket.indicator.spot-twap.outlier-tolerance-ratio", defaultValue = "0.0010")
            double outlierToleranceRatio,
            @ConfigProperty(name = "polymarket.indicator.spot-twap.twap-window-ms", defaultValue = "60000")
            long twapWindowMs,
            @ConfigProperty(name = "polymarket.indicator.spot-twap.spot-warmup-ms", defaultValue = "65000")
            long spotWarmupMs,
            @ConfigProperty(name = "polymarket.indicator.spot-twap.kraken-usdt-warmup-ms", defaultValue = "1800000")
            long krakenUsdtWarmupMs) {

        if (Double.isNaN(binanceBaseWeight) || binanceBaseWeight <= 0.0
                || Double.isNaN(coinbaseBaseWeight) || coinbaseBaseWeight <= 0.0
                || Double.isNaN(krakenBaseWeight) || krakenBaseWeight <= 0.0) {
            throw new IllegalArgumentException("Venue base weights must be positive");
        }
        if (Double.isNaN(usdtEmaTauMs) || usdtEmaTauMs <= 0.0
                || Double.isNaN(microVwapTauMs) || microVwapTauMs <= 0.0
                || Double.isNaN(rollingVolTauMs) || rollingVolTauMs <= 0.0
                || Double.isNaN(stalenessTauMs) || stalenessTauMs <= 0.0) {
            throw new IllegalArgumentException("Decay time constants (tauMs) must be positive");
        }
        if (maxVenueAgeMs <= 0L) {
            throw new IllegalArgumentException("maxVenueAgeMs must be positive");
        }
        if (Double.isNaN(minStalenessFactor) || minStalenessFactor <= 0.0 || minStalenessFactor > 1.0) {
            throw new IllegalArgumentException("minStalenessFactor must be in (0.0, 1.0]");
        }
        if (Double.isNaN(baseWeightShare) || baseWeightShare < 0.0
                || Double.isNaN(volumeWeightShare) || volumeWeightShare < 0.0
                || (baseWeightShare + volumeWeightShare) <= 0.0) {
            throw new IllegalArgumentException("baseWeightShare and volumeWeightShare must be non-negative with positive sum");
        }
        if (Double.isNaN(outlierToleranceRatio) || outlierToleranceRatio <= 0.0) {
            throw new IllegalArgumentException("outlierToleranceRatio must be positive");
        }
        if (twapWindowMs <= 0L) {
            throw new IllegalArgumentException("twapWindowMs must be positive");
        }
        if (spotWarmupMs < 0L || krakenUsdtWarmupMs < 0L) {
            throw new IllegalArgumentException("Warm-up durations must be non-negative");
        }

        this.binanceBaseWeight = binanceBaseWeight;
        this.coinbaseBaseWeight = coinbaseBaseWeight;
        this.krakenBaseWeight = krakenBaseWeight;
        this.usdtEmaTauMs = usdtEmaTauMs;
        this.microVwapTauMs = microVwapTauMs;
        this.rollingVolTauMs = rollingVolTauMs;
        this.stalenessTauMs = stalenessTauMs;
        this.maxVenueAgeMs = maxVenueAgeMs;
        this.minStalenessFactor = minStalenessFactor;
        this.baseWeightShare = baseWeightShare;
        this.volumeWeightShare = volumeWeightShare;
        this.outlierToleranceRatio = outlierToleranceRatio;
        this.twapWindowMs = twapWindowMs;
        this.spotWarmupMs = spotWarmupMs;
        this.krakenUsdtWarmupMs = krakenUsdtWarmupMs;

        Map<String, Object> params = new LinkedHashMap<>();
        params.put("baseWeightShare", baseWeightShare);
        params.put("binanceBaseWeight", binanceBaseWeight);
        params.put("coinbaseBaseWeight", coinbaseBaseWeight);
        params.put("krakenBaseWeight", krakenBaseWeight);
        params.put("krakenUsdtWarmupMs", krakenUsdtWarmupMs);
        params.put("maxVenueAgeMs", maxVenueAgeMs);
        params.put("microVwapTauMs", microVwapTauMs);
        params.put("minStalenessFactor", minStalenessFactor);
        params.put("outlierToleranceRatio", outlierToleranceRatio);
        params.put("rollingVolTauMs", rollingVolTauMs);
        params.put("spotWarmupMs", spotWarmupMs);
        params.put("stalenessTauMs", stalenessTauMs);
        params.put("twapWindowMs", twapWindowMs);
        params.put("usdtEmaTauMs", usdtEmaTauMs);
        params.put("volumeWeightShare", volumeWeightShare);
        this.parameterMap = Collections.unmodifiableMap(params);
    }

    /**
     * Returns a default configuration instance initialized with the standard calibrated constants.
     *
     * @return default {@link SpotTwapIndicatorConfig}
     */
    public static SpotTwapIndicatorConfig defaults() {
        return new SpotTwapIndicatorConfig(
                SpotTwapIndicator.SpotVenue.BINANCE.baseWeight(),
                SpotTwapIndicator.SpotVenue.COINBASE.baseWeight(),
                SpotTwapIndicator.SpotVenue.KRAKEN.baseWeight(),
                SpotTwapIndicator.USDT_EMA_TAU_MS,
                SpotTwapIndicator.MICRO_VWAP_TAU_MS,
                SpotTwapIndicator.ROLLING_VOL_TAU_MS,
                SpotTwapIndicator.STALENESS_TAU_MS,
                SpotTwapIndicator.MAX_VENUE_AGE_MS,
                SpotTwapIndicator.MIN_STALENESS_FACTOR,
                SpotTwapIndicator.BASE_WEIGHT_SHARE,
                SpotTwapIndicator.VOLUME_WEIGHT_SHARE,
                SpotTwapIndicator.OUTLIER_TOLERANCE_RATIO,
                SpotTwapIndicator.TWAP_WINDOW_MS,
                SpotTwapIndicator.DEFAULT_SPOT_WARMUP_MS,
                SpotTwapIndicator.DEFAULT_KRAKEN_USDT_WARMUP_MS
        );
    }

    /**
     * Unwraps any CDI client proxy into a direct value instance for zero-overhead field access on the hot path.
     *
     * @param config injected configuration instance (possibly a CDI proxy)
     * @return direct {@link SpotTwapIndicatorConfig} instance
     */
    public static SpotTwapIndicatorConfig unwrap(SpotTwapIndicatorConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("config cannot be null");
        }
        return config.getClass() == SpotTwapIndicatorConfig.class
                ? config
                : new SpotTwapIndicatorConfig(
                        config.binanceBaseWeight(),
                        config.coinbaseBaseWeight(),
                        config.krakenBaseWeight(),
                        config.usdtEmaTauMs(),
                        config.microVwapTauMs(),
                        config.rollingVolTauMs(),
                        config.stalenessTauMs(),
                        config.maxVenueAgeMs(),
                        config.minStalenessFactor(),
                        config.baseWeightShare(),
                        config.volumeWeightShare(),
                        config.outlierToleranceRatio(),
                        config.twapWindowMs(),
                        config.spotWarmupMs(),
                        config.krakenUsdtWarmupMs());
    }

    /** Baseline structural liquidity weight for Binance Spot. */
    public double binanceBaseWeight() {
        return binanceBaseWeight;
    }

    /** Baseline structural liquidity weight for Coinbase Spot. */
    public double coinbaseBaseWeight() {
        return coinbaseBaseWeight;
    }

    /** Baseline structural liquidity weight for Kraken Spot. */
    public double krakenBaseWeight() {
        return krakenBaseWeight;
    }

    /** Time constant in milliseconds for Kraken {@code USDT/USD} EMA. */
    public double usdtEmaTauMs() {
        return usdtEmaTauMs;
    }

    /** Short-horizon decay time constant in milliseconds for per-venue micro-VWAP smoothing. */
    public double microVwapTauMs() {
        return microVwapTauMs;
    }

    /** Decay time constant in milliseconds for per-venue rolling volume tracking. */
    public double rollingVolTauMs() {
        return rollingVolTauMs;
    }

    /** Decay time constant in milliseconds for cross-venue quote staleness weighting. */
    public double stalenessTauMs() {
        return stalenessTauMs;
    }

    /** Maximum age in milliseconds before a silent venue is excluded while fresher venues exist. */
    public long maxVenueAgeMs() {
        return maxVenueAgeMs;
    }

    /** Minimum staleness weight floor for active venues. */
    public double minStalenessFactor() {
        return minStalenessFactor;
    }

    /** Share of structural baseline weight in the venue liquidity blend. */
    public double baseWeightShare() {
        return baseWeightShare;
    }

    /** Share of 10-second rolling volume share in the venue liquidity blend. */
    public double volumeWeightShare() {
        return volumeWeightShare;
    }

    /** Relative deviation band around the 3-venue median before Cauchy outlier down-weighting applies. */
    public double outlierToleranceRatio() {
        return outlierToleranceRatio;
    }

    /** Rolling window length in milliseconds for the calculated USD TWAP. */
    public long twapWindowMs() {
        return twapWindowMs;
    }

    /** Pre-day spot trade warm-up window in milliseconds. */
    public long spotWarmupMs() {
        return spotWarmupMs;
    }

    /** Pre-day Kraken {@code USDT/USD} peg warm-up window in milliseconds. */
    public long krakenUsdtWarmupMs() {
        return krakenUsdtWarmupMs;
    }

    /**
     * Returns an immutable, lexicographically ordered map of all indicator configuration parameters.
     *
     * @return parameter map
     */
    public Map<String, Object> toParameterMap() {
        return parameterMap;
    }
}
