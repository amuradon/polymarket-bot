package cz.polymarket.bot.indicators;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Immutable configuration bean for {@link FastSpotMicroIndicator}.
 * <p>
 * Encapsulates direct-USD venue weights, micro-VWAP decay constants, 3-venue signed order flow
 * imbalance (OFI) parameters, 3-venue synchronized impulse time constants, staleness fallbacks,
 * and backtest warm-up durations for deterministic SHA-256 cache hashing.
 */
@ApplicationScoped
public class FastSpotMicroIndicatorConfig {

    private final double coinbaseDirectWeight;
    private final double krakenDirectWeight;
    private final double coinbaseOfiWeight;
    private final double krakenOfiWeight;
    private final double binanceOfiWeight;
    private final double coinbaseSyncWeight;
    private final double krakenSyncWeight;
    private final double binanceSyncWeight;
    private final double usdtEmaTauMs;
    private final double microVwapTauMs;
    private final double ofiTauMs;
    private final double ofiRegularizationUsd;
    private final double syncEmaTauMs;
    private final double directStalenessTauMs;
    private final long directMaxSilenceMs;
    private final long coalesceWindowMs;
    private final long spotWarmupMs;
    private final long krakenUsdtWarmupMs;
    private final Map<String, Object> parameterMap;

    /**
     * Constructs the {@link FastSpotMicroIndicator} configuration with MicroProfile properties.
     *
     * @param coinbaseDirectWeight direct-USD price weight for Coinbase Spot (default {@code 0.70})
     * @param krakenDirectWeight   direct-USD price weight for Kraken Spot (default {@code 0.30})
     * @param coinbaseOfiWeight    signed OFI weight for Coinbase Spot (default {@code 1.00})
     * @param krakenOfiWeight      signed OFI weight for Kraken Spot (default {@code 1.00})
     * @param binanceOfiWeight     signed OFI weight for Binance Spot (default {@code 1.00})
     * @param coinbaseSyncWeight   3-venue sync impulse weight for Coinbase Spot (default {@code 0.50})
     * @param krakenSyncWeight     3-venue sync impulse weight for Kraken Spot (default {@code 0.20})
     * @param binanceSyncWeight    3-venue sync impulse weight for Binance Spot (default {@code 0.30})
     * @param usdtEmaTauMs         Kraken {@code USDT/USD} EMA time constant in ms (default {@code 5000.0})
     * @param microVwapTauMs       fast per-venue micro-VWAP time constant in ms (default {@code 40.0})
     * @param ofiTauMs             signed 3-venue OFI decay time constant in ms (default {@code 1000.0})
     * @param ofiRegularizationUsd USD notional denominator prior damping dust-trade OFI noise (default {@code 6400.0})
     * @param syncEmaTauMs         per-venue baseline EMA time constant for 3-venue sync in ms (default {@code 500.0})
     * @param directStalenessTauMs staleness decay time constant for direct-USD venues in ms (default {@code 1500.0})
     * @param directMaxSilenceMs   max silence of both direct-USD venues before Binance USD fallback in ms (default {@code 30000})
     * @param coalesceWindowMs     backtest cache coalescing bucket width in ms (default {@code 25})
     * @param spotWarmupMs         pre-day spot trade warm-up duration in ms (default {@code 65000})
     * @param krakenUsdtWarmupMs   pre-day Kraken {@code USDT/USD} warm-up duration in ms (default {@code 1800000})
     */
    @Inject
    public FastSpotMicroIndicatorConfig(
            @ConfigProperty(name = "polymarket.indicator.fast-spot-micro.coinbase-direct-weight", defaultValue = "0.70")
            double coinbaseDirectWeight,
            @ConfigProperty(name = "polymarket.indicator.fast-spot-micro.kraken-direct-weight", defaultValue = "0.30")
            double krakenDirectWeight,
            @ConfigProperty(name = "polymarket.indicator.fast-spot-micro.coinbase-ofi-weight", defaultValue = "1.00")
            double coinbaseOfiWeight,
            @ConfigProperty(name = "polymarket.indicator.fast-spot-micro.kraken-ofi-weight", defaultValue = "1.00")
            double krakenOfiWeight,
            @ConfigProperty(name = "polymarket.indicator.fast-spot-micro.binance-ofi-weight", defaultValue = "1.00")
            double binanceOfiWeight,
            @ConfigProperty(name = "polymarket.indicator.fast-spot-micro.coinbase-sync-weight", defaultValue = "0.50")
            double coinbaseSyncWeight,
            @ConfigProperty(name = "polymarket.indicator.fast-spot-micro.kraken-sync-weight", defaultValue = "0.20")
            double krakenSyncWeight,
            @ConfigProperty(name = "polymarket.indicator.fast-spot-micro.binance-sync-weight", defaultValue = "0.30")
            double binanceSyncWeight,
            @ConfigProperty(name = "polymarket.indicator.fast-spot-micro.usdt-ema-tau-ms", defaultValue = "5000.0")
            double usdtEmaTauMs,
            @ConfigProperty(name = "polymarket.indicator.fast-spot-micro.micro-vwap-tau-ms", defaultValue = "40.0")
            double microVwapTauMs,
            @ConfigProperty(name = "polymarket.indicator.fast-spot-micro.ofi-tau-ms", defaultValue = "1000.0")
            double ofiTauMs,
            @ConfigProperty(name = "polymarket.indicator.fast-spot-micro.ofi-regularization-usd", defaultValue = "6400.0")
            double ofiRegularizationUsd,
            @ConfigProperty(name = "polymarket.indicator.fast-spot-micro.sync-ema-tau-ms", defaultValue = "500.0")
            double syncEmaTauMs,
            @ConfigProperty(name = "polymarket.indicator.fast-spot-micro.direct-staleness-tau-ms", defaultValue = "1500.0")
            double directStalenessTauMs,
            @ConfigProperty(name = "polymarket.indicator.fast-spot-micro.direct-max-silence-ms", defaultValue = "30000")
            long directMaxSilenceMs,
            @ConfigProperty(name = "polymarket.indicator.fast-spot-micro.coalesce-window-ms", defaultValue = "25")
            long coalesceWindowMs,
            @ConfigProperty(name = "polymarket.indicator.fast-spot-micro.spot-warmup-ms", defaultValue = "65000")
            long spotWarmupMs,
            @ConfigProperty(name = "polymarket.indicator.fast-spot-micro.kraken-usdt-warmup-ms", defaultValue = "1800000")
            long krakenUsdtWarmupMs) {

        if (!Double.isFinite(coinbaseDirectWeight) || coinbaseDirectWeight <= 0.0
                || !Double.isFinite(krakenDirectWeight) || krakenDirectWeight <= 0.0) {
            throw new IllegalArgumentException("Direct-USD weights must be finite and positive");
        }
        if (!Double.isFinite(coinbaseOfiWeight) || coinbaseOfiWeight <= 0.0
                || !Double.isFinite(krakenOfiWeight) || krakenOfiWeight <= 0.0
                || !Double.isFinite(binanceOfiWeight) || binanceOfiWeight <= 0.0) {
            throw new IllegalArgumentException("OFI venue weights must be finite and positive");
        }
        if (!Double.isFinite(coinbaseSyncWeight) || coinbaseSyncWeight <= 0.0
                || !Double.isFinite(krakenSyncWeight) || krakenSyncWeight <= 0.0
                || !Double.isFinite(binanceSyncWeight) || binanceSyncWeight <= 0.0) {
            throw new IllegalArgumentException("Sync venue weights must be finite and positive");
        }
        if (!Double.isFinite(usdtEmaTauMs) || usdtEmaTauMs <= 0.0
                || !Double.isFinite(microVwapTauMs) || microVwapTauMs <= 0.0
                || !Double.isFinite(ofiTauMs) || ofiTauMs <= 0.0
                || !Double.isFinite(syncEmaTauMs) || syncEmaTauMs <= 0.0
                || !Double.isFinite(directStalenessTauMs) || directStalenessTauMs <= 0.0) {
            throw new IllegalArgumentException("Decay time constants must be finite and positive");
        }
        if (!Double.isFinite(ofiRegularizationUsd) || ofiRegularizationUsd < 0.0) {
            throw new IllegalArgumentException("ofiRegularizationUsd must be finite and non-negative");
        }
        if (directMaxSilenceMs <= 0L || coalesceWindowMs <= 0L) {
            throw new IllegalArgumentException("directMaxSilenceMs and coalesceWindowMs must be positive");
        }
        if (spotWarmupMs < 0L || krakenUsdtWarmupMs < 0L) {
            throw new IllegalArgumentException("Warm-up durations must be non-negative");
        }

        this.coinbaseDirectWeight = coinbaseDirectWeight;
        this.krakenDirectWeight = krakenDirectWeight;
        this.coinbaseOfiWeight = coinbaseOfiWeight;
        this.krakenOfiWeight = krakenOfiWeight;
        this.binanceOfiWeight = binanceOfiWeight;
        this.coinbaseSyncWeight = coinbaseSyncWeight;
        this.krakenSyncWeight = krakenSyncWeight;
        this.binanceSyncWeight = binanceSyncWeight;
        this.usdtEmaTauMs = usdtEmaTauMs;
        this.microVwapTauMs = microVwapTauMs;
        this.ofiTauMs = ofiTauMs;
        this.ofiRegularizationUsd = ofiRegularizationUsd;
        this.syncEmaTauMs = syncEmaTauMs;
        this.directStalenessTauMs = directStalenessTauMs;
        this.directMaxSilenceMs = directMaxSilenceMs;
        this.coalesceWindowMs = coalesceWindowMs;
        this.spotWarmupMs = spotWarmupMs;
        this.krakenUsdtWarmupMs = krakenUsdtWarmupMs;

        Map<String, Object> params = new LinkedHashMap<>();
        params.put("binanceOfiWeight", binanceOfiWeight);
        params.put("binanceSyncWeight", binanceSyncWeight);
        params.put("coalesceWindowMs", coalesceWindowMs);
        params.put("coinbaseDirectWeight", coinbaseDirectWeight);
        params.put("coinbaseOfiWeight", coinbaseOfiWeight);
        params.put("coinbaseSyncWeight", coinbaseSyncWeight);
        params.put("directMaxSilenceMs", directMaxSilenceMs);
        params.put("directStalenessTauMs", directStalenessTauMs);
        params.put("krakenDirectWeight", krakenDirectWeight);
        params.put("krakenOfiWeight", krakenOfiWeight);
        params.put("krakenSyncWeight", krakenSyncWeight);
        params.put("krakenUsdtWarmupMs", krakenUsdtWarmupMs);
        params.put("microVwapTauMs", microVwapTauMs);
        params.put("ofiRegularizationUsd", ofiRegularizationUsd);
        params.put("ofiTauMs", ofiTauMs);
        params.put("spotWarmupMs", spotWarmupMs);
        params.put("syncEmaTauMs", syncEmaTauMs);
        params.put("usdtEmaTauMs", usdtEmaTauMs);
        this.parameterMap = Collections.unmodifiableMap(params);
    }

    /**
     * Returns the default calibrated configuration for {@link FastSpotMicroIndicator}.
     *
     * @return default {@link FastSpotMicroIndicatorConfig}
     */
    public static FastSpotMicroIndicatorConfig defaults() {
        return new FastSpotMicroIndicatorConfig(
                0.70, 0.30,
                1.00, 1.00, 1.00,
                0.50, 0.20, 0.30,
                5_000.0, 40.0, 1_000.0, 6_400.0, 500.0, 1_500.0,
                30_000L, 25L, 65_000L, 1_800_000L
        );
    }

    /**
     * Unwraps any CDI client proxy into a direct immutable value instance for zero-overhead hot-path access.
     *
     * @param config injected configuration instance
     * @return direct {@link FastSpotMicroIndicatorConfig} instance
     */
    public static FastSpotMicroIndicatorConfig unwrap(FastSpotMicroIndicatorConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("config cannot be null");
        }
        return config.getClass() == FastSpotMicroIndicatorConfig.class
                ? config
                : new FastSpotMicroIndicatorConfig(
                        config.coinbaseDirectWeight(),
                        config.krakenDirectWeight(),
                        config.coinbaseOfiWeight(),
                        config.krakenOfiWeight(),
                        config.binanceOfiWeight(),
                        config.coinbaseSyncWeight(),
                        config.krakenSyncWeight(),
                        config.binanceSyncWeight(),
                        config.usdtEmaTauMs(),
                        config.microVwapTauMs(),
                        config.ofiTauMs(),
                        config.ofiRegularizationUsd(),
                        config.syncEmaTauMs(),
                        config.directStalenessTauMs(),
                        config.directMaxSilenceMs(),
                        config.coalesceWindowMs(),
                        config.spotWarmupMs(),
                        config.krakenUsdtWarmupMs());
    }

    /** Direct-USD price weight for Coinbase Spot. */
    public double coinbaseDirectWeight() { return coinbaseDirectWeight; }

    /** Direct-USD price weight for Kraken Spot. */
    public double krakenDirectWeight() { return krakenDirectWeight; }

    /** Signed OFI weight for Coinbase Spot. */
    public double coinbaseOfiWeight() { return coinbaseOfiWeight; }

    /** Signed OFI weight for Kraken Spot. */
    public double krakenOfiWeight() { return krakenOfiWeight; }

    /** Signed OFI weight for Binance Spot. */
    public double binanceOfiWeight() { return binanceOfiWeight; }

    /** 3-venue sync impulse weight for Coinbase Spot. */
    public double coinbaseSyncWeight() { return coinbaseSyncWeight; }

    /** 3-venue sync impulse weight for Kraken Spot. */
    public double krakenSyncWeight() { return krakenSyncWeight; }

    /** 3-venue sync impulse weight for Binance Spot. */
    public double binanceSyncWeight() { return binanceSyncWeight; }

    /** Time constant in ms for Kraken {@code USDT/USD} EMA. */
    public double usdtEmaTauMs() { return usdtEmaTauMs; }

    /** Decay time constant in ms for fast per-venue micro-VWAP. */
    public double microVwapTauMs() { return microVwapTauMs; }

    /** Decay time constant in ms for 3-venue signed OFI. */
    public double ofiTauMs() { return ofiTauMs; }

    /** USD notional denominator prior damping single-lot dust trades in signed OFI. */
    public double ofiRegularizationUsd() { return ofiRegularizationUsd; }

    /** Decay time constant in ms for per-venue 500ms sync baseline EMA. */
    public double syncEmaTauMs() { return syncEmaTauMs; }

    /** Staleness decay time constant in ms for direct-USD venues. */
    public double directStalenessTauMs() { return directStalenessTauMs; }

    /** Maximum silence of both direct-USD venues in ms before falling back to Binance USD. */
    public long directMaxSilenceMs() { return directMaxSilenceMs; }

    /** Coalescing bucket width in ms for backtest Parquet caching. */
    public long coalesceWindowMs() { return coalesceWindowMs; }

    /** Pre-day spot trade warm-up duration in ms. */
    public long spotWarmupMs() { return spotWarmupMs; }

    /** Pre-day Kraken {@code USDT/USD} peg warm-up duration in ms. */
    public long krakenUsdtWarmupMs() { return krakenUsdtWarmupMs; }

    /**
     * Returns an immutable, lexicographically ordered map of all indicator parameters.
     *
     * @return parameter map
     */
    public Map<String, Object> toParameterMap() {
        return parameterMap;
    }
}
