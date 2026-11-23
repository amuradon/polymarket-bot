package cz.polymarket.bot.strategy;

import cz.polymarket.bot.domain.Timeframe;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Locale;

/**
 * Immutable configuration bean for {@link PricingModelStrategy}.
 * <p>
 * Governs the basis-anchored multi-exchange spot nowcast, continuous 60-second settlement TWAP
 * integrator, two-scale EWMA realized volatility estimator, heavy-tailed {@code Student-t (df=4)}
 * fair-value model, active oracle lead-gap filters, and liquidity-aware position sizing.
 * <p>
 * All underlying price dispersion, bridge-floor, and lead-gap parameters are expressed in
 * asset-agnostic basis points ({@code bps}) or centi-basis points ({@code 0.01 bps}) so the
 * strategy operates identically across {@code BTC}, {@code ETH}, and {@code SOL}.
 */
@ApplicationScoped
public class PricingModelStrategyConfig {

    private final Timeframe timeframe;
    private final long minTauMs;
    private final long maxTauMs;
    private final long cooldownMs;
    private final long maxQuoteAgeMs;
    private final long spotDelayOffsetMs;
    private final double basisEmaAlpha;
    private final double volFastTauSec;
    private final double volSlowTauSec;
    private final double volFastWeight;
    private final double minSigma1sBps;
    private final double maxSingleReturnBps;
    private final double bridgeFloorBps;
    private final int minWarmupSamples;
    private final int minOpenPriceMc;
    private final int maxOpenPriceMc;
    private final int maxSpreadMc;
    private final int minDepthShares;
    private final int minNetEdgeMc;
    private final int minFillNetEdgeMc;
    private final int minLeadGapCentiBps;
    private final long modelJumpWindowMs;
    private final int minModelJumpMc;
    private final int maxSlippageMc;
    private final int takerFeeRateBps;
    private final int minOrderShares;
    private final int maxOrderShares;
    private final int maxInventoryShares;
    private final String defaultMarketId;
    private final String upToken;
    private final String downToken;

    /**
     * Constructs the {@link PricingModelStrategy} configuration with injected MicroProfile properties.
     *
     * @param timeframeLabel market candle timeframe name or code (default {@code "15m"})
     * @param minTauMs minimum remaining time to candle expiry in milliseconds (default {@code 8000})
     * @param maxTauMs maximum remaining time to candle expiry in milliseconds (default {@code 480000})
     * @param cooldownMs minimum interval between same-side scaling orders in milliseconds (default {@code 3000})
     * @param maxQuoteAgeMs maximum allowed age of resting CLOB quote for spot-triggered evaluation in ms (default {@code 2500})
     * @param spotDelayOffsetMs effective exchange delay offset in ms aligning {@code SpotTwapIndicator} to oracle origin time (default {@code 75})
     * @param basisEmaAlpha EMA smoothing factor in {@code (0, 1]} for Chainlink-vs-SpotTwap relative basis (default {@code 0.25})
     * @param volFastTauSec time constant in seconds for fast 1s return variance EWMA (default {@code 30.0})
     * @param volSlowTauSec time constant in seconds for slow 1s return variance EWMA (default {@code 180.0})
     * @param volFastWeight blend weight in {@code [0, 1]} assigned to fast variance EWMA (default {@code 0.55})
     * @param minSigma1sBps floor on 1-second underlying volatility in basis points (default {@code 0.45})
     * @param maxSingleReturnBps outlier cap on single 1-second return in basis points (default {@code 30.0})
     * @param bridgeFloorBps final-minute uncertainty floor in basis points preventing stationary knife-edge overconfidence (default {@code 0.70})
     * @param minWarmupSamples minimum 1-second volatility samples required before trading (default {@code 15})
     * @param minOpenPriceMc minimum best-ask entry price in millicents (default {@code 250})
     * @param maxOpenPriceMc maximum best-ask entry price in millicents (default {@code 920})
     * @param maxSpreadMc maximum allowed BBO spread on traded token in millicents (default {@code 25})
     * @param minDepthShares minimum 1-cent ask depth in shares required for entry (default {@code 25})
     * @param minNetEdgeMc minimum post-fee model edge over best ask in millicents (default {@code 40})
     * @param minFillNetEdgeMc minimum post-fee model edge over 100-share estimated fill price in millicents (default {@code 25})
     * @param minLeadGapCentiBps minimum favorable basis-anchored spot lead over last PolyBolt price in centi-bps (default {@code 220})
     * @param modelJumpWindowMs lookback window in ms for measuring fresh fair-probability jump (default {@code 1250})
     * @param minModelJumpMc minimum fresh fair-probability increase over {@code modelJumpWindowMs} in millicents (default {@code 25})
     * @param maxSlippageMc maximum limit price buffer above best ask in millicents (default {@code 10})
     * @param takerFeeRateBps Polymarket quadratic taker fee rate in basis points (default {@code 700})
     * @param minOrderShares minimum single-order size in shares (default {@code 25})
     * @param maxOrderShares maximum single-order size in shares (default {@code 150})
     * @param maxInventoryShares maximum cumulative shares per market (default {@code 250})
     * @param defaultMarketId default market identifier for order commands
     * @param upToken token identifier for UP contract
     * @param downToken token identifier for DOWN contract
     */
    @Inject
    public PricingModelStrategyConfig(
            @ConfigProperty(name = "polymarket.strategy.pricing-model.timeframe", defaultValue = "15m") String timeframeLabel,
            @ConfigProperty(name = "polymarket.strategy.pricing-model.min-tau-ms", defaultValue = "8000") long minTauMs,
            @ConfigProperty(name = "polymarket.strategy.pricing-model.max-tau-ms", defaultValue = "480000") long maxTauMs,
            @ConfigProperty(name = "polymarket.strategy.pricing-model.cooldown-ms", defaultValue = "3000") long cooldownMs,
            @ConfigProperty(name = "polymarket.strategy.pricing-model.max-quote-age-ms", defaultValue = "2500") long maxQuoteAgeMs,
            @ConfigProperty(name = "polymarket.strategy.pricing-model.spot-delay-offset-ms", defaultValue = "75") long spotDelayOffsetMs,
            @ConfigProperty(name = "polymarket.strategy.pricing-model.basis-ema-alpha", defaultValue = "0.25") double basisEmaAlpha,
            @ConfigProperty(name = "polymarket.strategy.pricing-model.vol-fast-tau-sec", defaultValue = "30.0") double volFastTauSec,
            @ConfigProperty(name = "polymarket.strategy.pricing-model.vol-slow-tau-sec", defaultValue = "180.0") double volSlowTauSec,
            @ConfigProperty(name = "polymarket.strategy.pricing-model.vol-fast-weight", defaultValue = "0.55") double volFastWeight,
            @ConfigProperty(name = "polymarket.strategy.pricing-model.min-sigma-1s-bps", defaultValue = "0.45") double minSigma1sBps,
            @ConfigProperty(name = "polymarket.strategy.pricing-model.max-single-return-bps", defaultValue = "30.0") double maxSingleReturnBps,
            @ConfigProperty(name = "polymarket.strategy.pricing-model.bridge-floor-bps", defaultValue = "0.70") double bridgeFloorBps,
            @ConfigProperty(name = "polymarket.strategy.pricing-model.min-warmup-samples", defaultValue = "15") int minWarmupSamples,
            @ConfigProperty(name = "polymarket.strategy.pricing-model.min-open-price-mc", defaultValue = "250") int minOpenPriceMc,
            @ConfigProperty(name = "polymarket.strategy.pricing-model.max-open-price-mc", defaultValue = "920") int maxOpenPriceMc,
            @ConfigProperty(name = "polymarket.strategy.pricing-model.max-spread-mc", defaultValue = "25") int maxSpreadMc,
            @ConfigProperty(name = "polymarket.strategy.pricing-model.min-depth-shares", defaultValue = "25") int minDepthShares,
            @ConfigProperty(name = "polymarket.strategy.pricing-model.min-net-edge-mc", defaultValue = "40") int minNetEdgeMc,
            @ConfigProperty(name = "polymarket.strategy.pricing-model.min-fill-net-edge-mc", defaultValue = "25") int minFillNetEdgeMc,
            @ConfigProperty(name = "polymarket.strategy.pricing-model.min-lead-gap-centi-bps", defaultValue = "220") int minLeadGapCentiBps,
            @ConfigProperty(name = "polymarket.strategy.pricing-model.model-jump-window-ms", defaultValue = "1250") long modelJumpWindowMs,
            @ConfigProperty(name = "polymarket.strategy.pricing-model.min-model-jump-mc", defaultValue = "25") int minModelJumpMc,
            @ConfigProperty(name = "polymarket.strategy.pricing-model.max-slippage-mc", defaultValue = "10") int maxSlippageMc,
            @ConfigProperty(name = "polymarket.strategy.pricing-model.taker-fee-rate-bps", defaultValue = "700") int takerFeeRateBps,
            @ConfigProperty(name = "polymarket.strategy.pricing-model.min-order-shares", defaultValue = "25") int minOrderShares,
            @ConfigProperty(name = "polymarket.strategy.pricing-model.max-order-shares", defaultValue = "150") int maxOrderShares,
            @ConfigProperty(name = "polymarket.strategy.pricing-model.max-inventory-shares", defaultValue = "250") int maxInventoryShares,
            @ConfigProperty(name = "polymarket.strategy.default-market-id", defaultValue = "pm-btc-15m") String defaultMarketId,
            @ConfigProperty(name = "polymarket.strategy.up-token", defaultValue = "BTC-UP") String upToken,
            @ConfigProperty(name = "polymarket.strategy.down-token", defaultValue = "BTC-DOWN") String downToken) {

        Timeframe parsedTimeframe = parseTimeframe(timeframeLabel);
        long candleDurationMs = parsedTimeframe.getDurationSeconds() * 1_000L;

        if (minTauMs < 0L || maxTauMs <= minTauMs || maxTauMs >= candleDurationMs) {
            throw new IllegalArgumentException("Invalid tau window: 0 <= minTauMs < maxTauMs < candleDurationMs");
        }
        if (cooldownMs < 0L || maxQuoteAgeMs <= 0L || spotDelayOffsetMs < 0L) {
            throw new IllegalArgumentException("Timing parameters (cooldownMs, maxQuoteAgeMs, spotDelayOffsetMs) must be valid");
        }
        if (Double.isNaN(basisEmaAlpha) || basisEmaAlpha <= 0.0 || basisEmaAlpha > 1.0) {
            throw new IllegalArgumentException("basisEmaAlpha must be in (0.0, 1.0]");
        }
        if (Double.isNaN(volFastTauSec) || volFastTauSec <= 0.0
                || Double.isNaN(volSlowTauSec) || volSlowTauSec < volFastTauSec
                || Double.isNaN(volFastWeight) || volFastWeight < 0.0 || volFastWeight > 1.0) {
            throw new IllegalArgumentException("Volatility EWMA parameters must satisfy 0 < volFastTauSec <= volSlowTauSec and weight in [0, 1]");
        }
        if (Double.isNaN(minSigma1sBps) || minSigma1sBps <= 0.0
                || Double.isNaN(maxSingleReturnBps) || maxSingleReturnBps <= minSigma1sBps
                || Double.isNaN(bridgeFloorBps) || bridgeFloorBps < 0.0) {
            throw new IllegalArgumentException("Volatility floor, return cap, and bridge floor bounds must be valid");
        }
        if (minWarmupSamples < 1) {
            throw new IllegalArgumentException("minWarmupSamples must be >= 1");
        }
        if (minOpenPriceMc <= 0 || maxOpenPriceMc >= 1_000 || minOpenPriceMc > maxOpenPriceMc) {
            throw new IllegalArgumentException("Open price bounds must satisfy 0 < minOpenPriceMc <= maxOpenPriceMc < 1000");
        }
        if (maxSpreadMc <= 0 || maxSpreadMc >= 500 || minDepthShares < 1) {
            throw new IllegalArgumentException("Spread and depth bounds must be valid");
        }
        if (minNetEdgeMc < 0 || minFillNetEdgeMc < 0 || minLeadGapCentiBps < 0
                || modelJumpWindowMs <= 0L || minModelJumpMc < 0) {
            throw new IllegalArgumentException("Edge, lead-gap, and model-jump thresholds must be non-negative");
        }
        if (maxSlippageMc < 0 || maxSlippageMc >= 200 || takerFeeRateBps < 0 || takerFeeRateBps > 5_000) {
            throw new IllegalArgumentException("Slippage and taker fee rate out of valid range");
        }
        if (minOrderShares < 5 || maxOrderShares < minOrderShares || maxInventoryShares < minOrderShares) {
            throw new IllegalArgumentException("Order sizing must satisfy 5 <= minOrderShares <= maxOrderShares and <= maxInventoryShares");
        }

        this.timeframe = parsedTimeframe;
        this.minTauMs = minTauMs;
        this.maxTauMs = maxTauMs;
        this.cooldownMs = cooldownMs;
        this.maxQuoteAgeMs = maxQuoteAgeMs;
        this.spotDelayOffsetMs = spotDelayOffsetMs;
        this.basisEmaAlpha = basisEmaAlpha;
        this.volFastTauSec = volFastTauSec;
        this.volSlowTauSec = volSlowTauSec;
        this.volFastWeight = volFastWeight;
        this.minSigma1sBps = minSigma1sBps;
        this.maxSingleReturnBps = maxSingleReturnBps;
        this.bridgeFloorBps = bridgeFloorBps;
        this.minWarmupSamples = minWarmupSamples;
        this.minOpenPriceMc = minOpenPriceMc;
        this.maxOpenPriceMc = maxOpenPriceMc;
        this.maxSpreadMc = maxSpreadMc;
        this.minDepthShares = minDepthShares;
        this.minNetEdgeMc = minNetEdgeMc;
        this.minFillNetEdgeMc = minFillNetEdgeMc;
        this.minLeadGapCentiBps = minLeadGapCentiBps;
        this.modelJumpWindowMs = modelJumpWindowMs;
        this.minModelJumpMc = minModelJumpMc;
        this.maxSlippageMc = maxSlippageMc;
        this.takerFeeRateBps = takerFeeRateBps;
        this.minOrderShares = minOrderShares;
        this.maxOrderShares = maxOrderShares;
        this.maxInventoryShares = maxInventoryShares;
        this.defaultMarketId = (defaultMarketId != null && !defaultMarketId.isBlank()) ? defaultMarketId.trim() : "pm-btc-15m";
        this.upToken = (upToken != null && !upToken.isBlank()) ? upToken.trim() : "BTC-UP";
        this.downToken = (downToken != null && !downToken.isBlank()) ? downToken.trim() : "BTC-DOWN";
    }

    /**
     * Returns calibrated Iteration 1 baseline configuration constants.
     *
     * @return default {@link PricingModelStrategyConfig}
     */
    public static PricingModelStrategyConfig defaults() {
        return new PricingModelStrategyConfig(
                "15m",
                8_000L,
                480_000L,
                3_000L,
                2_500L,
                75L,
                0.25,
                30.0,
                180.0,
                0.55,
                0.45,
                30.0,
                0.70,
                15,
                250,
                920,
                25,
                25,
                40,
                25,
                220,
                1_250L,
                25,
                10,
                700,
                25,
                150,
                250,
                "pm-btc-15m",
                "BTC-UP",
                "BTC-DOWN"
        );
    }

    /**
     * Unwraps any CDI client proxy into a plain immutable {@link PricingModelStrategyConfig} instance
     * for zero-overhead field access on the real-time hot path.
     *
     * @param config injected configuration bean
     * @return unwrapped plain configuration instance
     */
    public static PricingModelStrategyConfig unwrap(PricingModelStrategyConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("config cannot be null");
        }
        if (config.getClass() == PricingModelStrategyConfig.class) {
            return config;
        }
        return new PricingModelStrategyConfig(
                config.timeframe().getCode(),
                config.minTauMs(),
                config.maxTauMs(),
                config.cooldownMs(),
                config.maxQuoteAgeMs(),
                config.spotDelayOffsetMs(),
                config.basisEmaAlpha(),
                config.volFastTauSec(),
                config.volSlowTauSec(),
                config.volFastWeight(),
                config.minSigma1sBps(),
                config.maxSingleReturnBps(),
                config.bridgeFloorBps(),
                config.minWarmupSamples(),
                config.minOpenPriceMc(),
                config.maxOpenPriceMc(),
                config.maxSpreadMc(),
                config.minDepthShares(),
                config.minNetEdgeMc(),
                config.minFillNetEdgeMc(),
                config.minLeadGapCentiBps(),
                config.modelJumpWindowMs(),
                config.minModelJumpMc(),
                config.maxSlippageMc(),
                config.takerFeeRateBps(),
                config.minOrderShares(),
                config.maxOrderShares(),
                config.maxInventoryShares(),
                config.defaultMarketId(),
                config.upToken(),
                config.downToken()
        );
    }

    private static Timeframe parseTimeframe(String raw) {
        if (raw == null || raw.isBlank()) {
            return Timeframe.FIFTEEN_MINUTES;
        }
        String norm = raw.trim().toUpperCase(Locale.ROOT);
        for (Timeframe tf : Timeframe.values()) {
            if (tf.name().equals(norm) || tf.getCode().equalsIgnoreCase(norm)) {
                return tf;
            }
        }
        throw new IllegalArgumentException("Unsupported timeframe: " + raw);
    }

    /** Configured market candle timeframe. */
    public Timeframe timeframe() { return timeframe; }

    /** Minimum remaining time to expiry in milliseconds. */
    public long minTauMs() { return minTauMs; }

    /** Maximum remaining time to expiry in milliseconds. */
    public long maxTauMs() { return maxTauMs; }

    /** Cooldown between consecutive scaling orders in milliseconds. */
    public long cooldownMs() { return cooldownMs; }

    /** Maximum allowed age of resting CLOB quote in milliseconds. */
    public long maxQuoteAgeMs() { return maxQuoteAgeMs; }

    /** Effective exchange delay offset in milliseconds for basis alignment. */
    public long spotDelayOffsetMs() { return spotDelayOffsetMs; }

    /** EMA smoothing factor for relative Chainlink-vs-SpotTwap basis. */
    public double basisEmaAlpha() { return basisEmaAlpha; }

    /** Fast 1s return variance EWMA time constant in seconds. */
    public double volFastTauSec() { return volFastTauSec; }

    /** Slow 1s return variance EWMA time constant in seconds. */
    public double volSlowTauSec() { return volSlowTauSec; }

    /** Weight assigned to fast variance EWMA in the blended volatility estimate. */
    public double volFastWeight() { return volFastWeight; }

    /** Minimum 1-second underlying volatility floor in basis points. */
    public double minSigma1sBps() { return minSigma1sBps; }

    /** Outlier cap on a single 1-second underlying return in basis points. */
    public double maxSingleReturnBps() { return maxSingleReturnBps; }

    /** Final-minute uncertainty floor in basis points. */
    public double bridgeFloorBps() { return bridgeFloorBps; }

    /** Minimum 1-second volatility samples required before trading. */
    public int minWarmupSamples() { return minWarmupSamples; }

    /** Minimum best-ask price for directional entry in millicents. */
    public int minOpenPriceMc() { return minOpenPriceMc; }

    /** Maximum best-ask price for directional entry in millicents. */
    public int maxOpenPriceMc() { return maxOpenPriceMc; }

    /** Maximum allowed BBO spread on traded token in millicents. */
    public int maxSpreadMc() { return maxSpreadMc; }

    /** Minimum 1-cent ask depth in shares required for entry. */
    public int minDepthShares() { return minDepthShares; }

    /** Minimum post-fee model edge over best ask in millicents. */
    public int minNetEdgeMc() { return minNetEdgeMc; }

    /** Minimum post-fee model edge over estimated 100-share fill price in millicents. */
    public int minFillNetEdgeMc() { return minFillNetEdgeMc; }

    /** Minimum favorable basis-anchored spot lead over last PolyBolt price in centi-bps. */
    public int minLeadGapCentiBps() { return minLeadGapCentiBps; }

    /** Lookback window in milliseconds for fresh model probability jump. */
    public long modelJumpWindowMs() { return modelJumpWindowMs; }

    /** Minimum fresh model probability increase over {@code modelJumpWindowMs} in millicents. */
    public int minModelJumpMc() { return minModelJumpMc; }

    /** Maximum limit price buffer above best ask in millicents. */
    public int maxSlippageMc() { return maxSlippageMc; }

    /** Polymarket quadratic taker fee rate in basis points. */
    public int takerFeeRateBps() { return takerFeeRateBps; }

    /** Minimum single-order size in shares. */
    public int minOrderShares() { return minOrderShares; }

    /** Maximum single-order size in shares. */
    public int maxOrderShares() { return maxOrderShares; }

    /** Maximum cumulative inventory in shares per market. */
    public int maxInventoryShares() { return maxInventoryShares; }

    /** Default market identifier for order commands. */
    public String defaultMarketId() { return defaultMarketId; }

    /** Token identifier for UP outcome contract. */
    public String upToken() { return upToken; }

    /** Token identifier for DOWN outcome contract. */
    public String downToken() { return downToken; }
}
