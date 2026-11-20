package cz.polymarket.bot.strategy;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Immutable configuration bean for {@link RoundMainstreamStrategy}.
 * <p>
 * Parameters replicate the empirical low-volatility compression selection, CLOB whale-sweep and
 * binary mirror bid-wall triggers, convex clip sizing, and synthetic opposite-token {@code BUY}
 * offsets ({@code OFFSET_TP}, {@code OFFSET_SL}) of Polymarket trader
 * {@code 0xeda9247a2b3c99a9e0bf46cdac6e1974365cf589} ({@code Round-Mainstream}), executing orders
 * immediately upon signal confirmation without artificial blockchain observation delay.
 * <p>
 * All underlying asset volatility, range, and distance thresholds are expressed in basis points
 * ({@code bps}) or centi-basis points ({@code 0.01 bps}) so the strategy operates identically across
 * {@code BTC}, {@code ETH}, and {@code SOL} price scales.
 */
@ApplicationScoped
public class RoundMainstreamStrategyConfig {

    private final long minTauMs;
    private final long maxTauMs;
    private final long sameSideCooldownMs;
    private final long oppositeSideCooldownMs;
    private final int maxRange5mBps;
    private final int maxCandleRangeBps;
    private final int maxSigma1sCentiBps;
    private final int maxTargetDistanceBps;
    private final int maxAdverseSpotMoveCentiBps;
    private final int minCompressionEdgeMc;
    private final int minWarmupSamples;
    private final long sweepWindowMs;
    private final int minEntryAskAdvanceMc;
    private final int minEntryBidAdvanceMc;
    private final int maxSpreadMc;
    private final int minEffectiveSweepShares;
    private final int minBidWallShares;
    private final int minWallDepletionShares;
    private final int minOpenPriceMc;
    private final int maxOpenPriceMc;
    private final int minOffsetTpPriceMc;
    private final int maxOffsetTpPriceMc;
    private final int maxOffsetTpPairCostMc;
    private final int minOffsetSlPairCostMc;
    private final int maxSlippageMc;
    private final int takerFeeRateBps;
    private final int minOrderShares;
    private final int maxOrderShares;
    private final int sweepShareScaleBps;
    private final int convexWhaleSweepThresholdShares;
    private final int maxInventorySharesPerSide;
    private final String defaultMarketId;
    private final String upToken;
    private final String downToken;

    /**
     * Constructs the {@link RoundMainstreamStrategy} configuration with injected MicroProfile properties.
     *
     * @param minTauMs minimum remaining time to candle expiry in milliseconds (default {@code 15000})
     * @param maxTauMs maximum remaining time to candle expiry in milliseconds (default {@code 875000})
     * @param sameSideCooldownMs minimum interval between same-direction scaling orders in milliseconds (default {@code 2400})
     * @param oppositeSideCooldownMs minimum interval before synthetic opposite-side offset orders in milliseconds (default {@code 2500})
     * @param maxRange5mBps maximum rolling 5-minute underlying price range in basis points (default {@code 7})
     * @param maxCandleRangeBps maximum intra-candle underlying high-low range in basis points (default {@code 14})
     * @param maxSigma1sCentiBps maximum rolling 1-second underlying MAD volatility in centi-bps ({@code 0.01 bps}, default {@code 21})
     * @param maxTargetDistanceBps maximum underlying distance from opening 60s TWAP target in basis points for fresh opens (default {@code 10})
     * @param maxAdverseSpotMoveCentiBps maximum tolerated 4-second adverse underlying move in centi-bps (default {@code 75})
     * @param minCompressionEdgeMc minimum net compression fair-value edge after taker fee in millicents (default {@code 42})
     * @param minWarmupSamples minimum 1-second underlying samples before enabling standard entries (default {@code 105})
     * @param sweepWindowMs rolling CLOB whale-sweep detection window in milliseconds (default {@code 4500})
     * @param minEntryAskAdvanceMc minimum best-ask upward advance in millicents for price-advancing sweeps (default {@code 30})
     * @param minEntryBidAdvanceMc minimum best-bid upward advance in millicents confirming non-vacuum sweep (default {@code 20})
     * @param maxSpreadMc maximum allowed BBO spread on the traded outcome in millicents (default {@code 20})
     * @param minEffectiveSweepShares minimum effective swept shares required to trigger directional entry (default {@code 2000})
     * @param minBidWallShares minimum mirrored 1-cent bid-wall shares ({@code depth1cOpp}) confirming whale buy support (default {@code 1800})
     * @param minWallDepletionShares minimum net non-refilled 1c ask-wall depletion shares at constant tight BBO (default {@code 4200})
     * @param minOpenPriceMc minimum acceptable best-ask price for directional entries in millicents (default {@code 220})
     * @param maxOpenPriceMc maximum acceptable best-ask price for directional entries in millicents (default {@code 880})
     * @param minOffsetTpPriceMc minimum opposite-token ask price for synthetic take-profit in millicents (default {@code 60})
     * @param maxOffsetTpPriceMc maximum opposite-token ask price for synthetic take-profit in millicents (default {@code 320})
     * @param maxOffsetTpPairCostMc maximum combined {@code avgMainPriceMc + askOppMc} for synthetic take-profit in millicents (default {@code 840})
     * @param minOffsetSlPairCostMc minimum combined {@code avgMainPriceMc + askOppMc} for synthetic stop-loss hedge in millicents (default {@code 1080})
     * @param maxSlippageMc maximum limit-order price buffer above best ask in millicents (default {@code 20})
     * @param takerFeeRateBps Polymarket parabolic taker fee rate in basis points (default {@code 700})
     * @param minOrderShares minimum single-order size in integer shares (default {@code 5})
     * @param maxOrderShares maximum single-order size in integer shares (default {@code 650})
     * @param sweepShareScaleBps marginal copy-sizing slope above {@code minEffectiveSweepShares} in basis points (default {@code 180})
     * @param convexWhaleSweepThresholdShares swept-share threshold activating convex multi-whale sizing boost (default {@code 4200})
     * @param maxInventorySharesPerSide maximum cumulative shares per outcome in a single 15m market (default {@code 1800})
     * @param defaultMarketId default market identifier for order commands
     * @param upToken token identifier for the UP contract
     * @param downToken token identifier for the DOWN contract
     */
    @Inject
    public RoundMainstreamStrategyConfig(
            @ConfigProperty(name = "polymarket.strategy.round-mainstream.min-tau-ms", defaultValue = "15000") long minTauMs,
            @ConfigProperty(name = "polymarket.strategy.round-mainstream.max-tau-ms", defaultValue = "875000") long maxTauMs,
            @ConfigProperty(name = "polymarket.strategy.round-mainstream.same-side-cooldown-ms", defaultValue = "2400") long sameSideCooldownMs,
            @ConfigProperty(name = "polymarket.strategy.round-mainstream.opposite-side-cooldown-ms", defaultValue = "2500") long oppositeSideCooldownMs,
            @ConfigProperty(name = "polymarket.strategy.round-mainstream.max-range-5m-bps", defaultValue = "7") int maxRange5mBps,
            @ConfigProperty(name = "polymarket.strategy.round-mainstream.max-candle-range-bps", defaultValue = "14") int maxCandleRangeBps,
            @ConfigProperty(name = "polymarket.strategy.round-mainstream.max-sigma-1s-centi-bps", defaultValue = "21") int maxSigma1sCentiBps,
            @ConfigProperty(name = "polymarket.strategy.round-mainstream.max-target-distance-bps", defaultValue = "10") int maxTargetDistanceBps,
            @ConfigProperty(name = "polymarket.strategy.round-mainstream.max-adverse-spot-move-centi-bps", defaultValue = "75") int maxAdverseSpotMoveCentiBps,
            @ConfigProperty(name = "polymarket.strategy.round-mainstream.min-compression-edge-mc", defaultValue = "42") int minCompressionEdgeMc,
            @ConfigProperty(name = "polymarket.strategy.round-mainstream.min-warmup-samples", defaultValue = "105") int minWarmupSamples,
            @ConfigProperty(name = "polymarket.strategy.round-mainstream.sweep-window-ms", defaultValue = "4500") long sweepWindowMs,
            @ConfigProperty(name = "polymarket.strategy.round-mainstream.min-entry-ask-advance-mc", defaultValue = "30") int minEntryAskAdvanceMc,
            @ConfigProperty(name = "polymarket.strategy.round-mainstream.min-entry-bid-advance-mc", defaultValue = "20") int minEntryBidAdvanceMc,
            @ConfigProperty(name = "polymarket.strategy.round-mainstream.max-spread-mc", defaultValue = "20") int maxSpreadMc,
            @ConfigProperty(name = "polymarket.strategy.round-mainstream.min-effective-sweep-shares", defaultValue = "2000") int minEffectiveSweepShares,
            @ConfigProperty(name = "polymarket.strategy.round-mainstream.min-bid-wall-shares", defaultValue = "1800") int minBidWallShares,
            @ConfigProperty(name = "polymarket.strategy.round-mainstream.min-wall-depletion-shares", defaultValue = "4200") int minWallDepletionShares,
            @ConfigProperty(name = "polymarket.strategy.round-mainstream.min-open-price-mc", defaultValue = "220") int minOpenPriceMc,
            @ConfigProperty(name = "polymarket.strategy.round-mainstream.max-open-price-mc", defaultValue = "880") int maxOpenPriceMc,
            @ConfigProperty(name = "polymarket.strategy.round-mainstream.min-offset-tp-price-mc", defaultValue = "60") int minOffsetTpPriceMc,
            @ConfigProperty(name = "polymarket.strategy.round-mainstream.max-offset-tp-price-mc", defaultValue = "320") int maxOffsetTpPriceMc,
            @ConfigProperty(name = "polymarket.strategy.round-mainstream.max-offset-tp-pair-cost-mc", defaultValue = "840") int maxOffsetTpPairCostMc,
            @ConfigProperty(name = "polymarket.strategy.round-mainstream.min-offset-sl-pair-cost-mc", defaultValue = "1080") int minOffsetSlPairCostMc,
            @ConfigProperty(name = "polymarket.strategy.round-mainstream.max-slippage-mc", defaultValue = "20") int maxSlippageMc,
            @ConfigProperty(name = "polymarket.strategy.round-mainstream.taker-fee-rate-bps", defaultValue = "700") int takerFeeRateBps,
            @ConfigProperty(name = "polymarket.strategy.round-mainstream.min-order-shares", defaultValue = "5") int minOrderShares,
            @ConfigProperty(name = "polymarket.strategy.round-mainstream.max-order-shares", defaultValue = "650") int maxOrderShares,
            @ConfigProperty(name = "polymarket.strategy.round-mainstream.sweep-share-scale-bps", defaultValue = "220") int sweepShareScaleBps,
            @ConfigProperty(name = "polymarket.strategy.round-mainstream.convex-whale-sweep-threshold-shares", defaultValue = "4200") int convexWhaleSweepThresholdShares,
            @ConfigProperty(name = "polymarket.strategy.round-mainstream.max-inventory-shares-per-side", defaultValue = "1800") int maxInventorySharesPerSide,
            @ConfigProperty(name = "polymarket.strategy.default-market-id", defaultValue = "pm-btc-15m") String defaultMarketId,
            @ConfigProperty(name = "polymarket.strategy.up-token", defaultValue = "BTC-UP") String upToken,
            @ConfigProperty(name = "polymarket.strategy.down-token", defaultValue = "BTC-DOWN") String downToken) {

        if (minTauMs < 0L || maxTauMs <= minTauMs || maxTauMs >= 900_000L) {
            throw new IllegalArgumentException("Invalid tau window: 0 <= minTauMs < maxTauMs < 900000");
        }
        if (sameSideCooldownMs < 0L || oppositeSideCooldownMs < 0L) {
            throw new IllegalArgumentException("Cooldown durations must be non-negative");
        }
        if (maxRange5mBps <= 0 || maxCandleRangeBps < maxRange5mBps || maxSigma1sCentiBps <= 0 || maxTargetDistanceBps <= 0) {
            throw new IllegalArgumentException("Volatility and target-distance bounds in bps must be positive");
        }
        if (maxAdverseSpotMoveCentiBps < 0 || minCompressionEdgeMc < 0 || minWarmupSamples < 1) {
            throw new IllegalArgumentException("Spot move, compression edge, and minWarmupSamples bounds must be valid");
        }
        if (sweepWindowMs < 500L || sweepWindowMs > 30_000L) {
            throw new IllegalArgumentException("sweepWindowMs must be in [500, 30000]");
        }
        if (minEntryAskAdvanceMc < 10 || minEntryBidAdvanceMc < 0 || maxSpreadMc <= 0 || maxSpreadMc >= 500) {
            throw new IllegalArgumentException("Sweep price advance and spread bounds must be valid");
        }
        if (minEffectiveSweepShares <= 0 || minBidWallShares <= 0 || minWallDepletionShares <= 0) {
            throw new IllegalArgumentException("Sweep and bid-wall share thresholds must be positive");
        }
        if (minOpenPriceMc <= 0 || maxOpenPriceMc >= 1_000 || minOpenPriceMc > maxOpenPriceMc) {
            throw new IllegalArgumentException("Open price bounds must satisfy 0 < minOpenPriceMc <= maxOpenPriceMc < 1000");
        }
        if (minOffsetTpPriceMc <= 0 || maxOffsetTpPriceMc >= 1_000 || minOffsetTpPriceMc > maxOffsetTpPriceMc) {
            throw new IllegalArgumentException("Offset TP price bounds must satisfy 0 < min <= max < 1000");
        }
        if (maxOffsetTpPairCostMc <= 0 || maxOffsetTpPairCostMc >= 1_000 || minOffsetSlPairCostMc <= maxOffsetTpPairCostMc) {
            throw new IllegalArgumentException("Pair cost bounds must satisfy 0 < maxOffsetTpPairCostMc < 1000 and < minOffsetSlPairCostMc");
        }
        if (maxSlippageMc < 0 || maxSlippageMc >= 200 || takerFeeRateBps < 0 || takerFeeRateBps > 5_000) {
            throw new IllegalArgumentException("Slippage and taker fee bounds out of range");
        }
        if (minOrderShares < 5 || maxOrderShares < minOrderShares || maxInventorySharesPerSide < minOrderShares) {
            throw new IllegalArgumentException("Order shares must satisfy 5 <= minOrderShares <= maxOrderShares and <= maxInventorySharesPerSide");
        }
        if (sweepShareScaleBps <= 0 || sweepShareScaleBps > 10_000 || convexWhaleSweepThresholdShares < minEffectiveSweepShares) {
            throw new IllegalArgumentException("Sweep sizing scale and convex threshold out of range");
        }

        this.minTauMs = minTauMs;
        this.maxTauMs = maxTauMs;
        this.sameSideCooldownMs = sameSideCooldownMs;
        this.oppositeSideCooldownMs = oppositeSideCooldownMs;
        this.maxRange5mBps = maxRange5mBps;
        this.maxCandleRangeBps = maxCandleRangeBps;
        this.maxSigma1sCentiBps = maxSigma1sCentiBps;
        this.maxTargetDistanceBps = maxTargetDistanceBps;
        this.maxAdverseSpotMoveCentiBps = maxAdverseSpotMoveCentiBps;
        this.minCompressionEdgeMc = minCompressionEdgeMc;
        this.minWarmupSamples = minWarmupSamples;
        this.sweepWindowMs = sweepWindowMs;
        this.minEntryAskAdvanceMc = minEntryAskAdvanceMc;
        this.minEntryBidAdvanceMc = minEntryBidAdvanceMc;
        this.maxSpreadMc = maxSpreadMc;
        this.minEffectiveSweepShares = minEffectiveSweepShares;
        this.minBidWallShares = minBidWallShares;
        this.minWallDepletionShares = minWallDepletionShares;
        this.minOpenPriceMc = minOpenPriceMc;
        this.maxOpenPriceMc = maxOpenPriceMc;
        this.minOffsetTpPriceMc = minOffsetTpPriceMc;
        this.maxOffsetTpPriceMc = maxOffsetTpPriceMc;
        this.maxOffsetTpPairCostMc = maxOffsetTpPairCostMc;
        this.minOffsetSlPairCostMc = minOffsetSlPairCostMc;
        this.maxSlippageMc = maxSlippageMc;
        this.takerFeeRateBps = takerFeeRateBps;
        this.minOrderShares = minOrderShares;
        this.maxOrderShares = maxOrderShares;
        this.sweepShareScaleBps = sweepShareScaleBps;
        this.convexWhaleSweepThresholdShares = convexWhaleSweepThresholdShares;
        this.maxInventorySharesPerSide = maxInventorySharesPerSide;
        this.defaultMarketId = (defaultMarketId != null && !defaultMarketId.isBlank()) ? defaultMarketId.trim() : "pm-btc-15m";
        this.upToken = (upToken != null && !upToken.isBlank()) ? upToken.trim() : "BTC-UP";
        this.downToken = (downToken != null && !downToken.isBlank()) ? downToken.trim() : "BTC-DOWN";
    }

    /**
     * Creates a default configuration instance calibrated to empirical {@code Round-Mainstream} parameters.
     *
     * @return calibrated default configuration
     */
    public static RoundMainstreamStrategyConfig defaultModelV1() {
        return new RoundMainstreamStrategyConfig(
                15_000L,
                875_000L,
                2_400L,
                2_500L,
                7,
                14,
                21,
                10,
                75,
                42,
                105,
                4_500L,
                30,
                20,
                20,
                2_000,
                1_800,
                4_200,
                220,
                880,
                60,
                320,
                840,
                1_080,
                20,
                700,
                5,
                650,
                220,
                4_200,
                1_800,
                "pm-btc-15m",
                "BTC-UP",
                "BTC-DOWN"
        );
    }

    /** Minimum remaining time to candle expiry in milliseconds. */
    public long minTauMs() {
        return minTauMs;
    }

    /** Maximum remaining time to candle expiry in milliseconds. */
    public long maxTauMs() {
        return maxTauMs;
    }

    /** Minimum interval between same-side scaling orders in milliseconds. */
    public long sameSideCooldownMs() {
        return sameSideCooldownMs;
    }

    /** Minimum interval before synthetic opposite-side offset orders in milliseconds. */
    public long oppositeSideCooldownMs() {
        return oppositeSideCooldownMs;
    }

    /** Maximum rolling 5-minute underlying range in basis points. */
    public int maxRange5mBps() {
        return maxRange5mBps;
    }

    /** Maximum intra-candle underlying range in basis points. */
    public int maxCandleRangeBps() {
        return maxCandleRangeBps;
    }

    /** Maximum 1-second underlying MAD volatility in centi-bps (0.01 bps). */
    public int maxSigma1sCentiBps() {
        return maxSigma1sCentiBps;
    }

    /** Maximum underlying distance from opening 60s TWAP target in basis points for fresh opens. */
    public int maxTargetDistanceBps() {
        return maxTargetDistanceBps;
    }

    /** Maximum tolerated 4-second adverse underlying move in centi-bps. */
    public int maxAdverseSpotMoveCentiBps() {
        return maxAdverseSpotMoveCentiBps;
    }

    /** Minimum net compression fair-value edge after taker fee in millicents. */
    public int minCompressionEdgeMc() {
        return minCompressionEdgeMc;
    }

    /** Minimum 1-second underlying samples required before activating standard entries. */
    public int minWarmupSamples() {
        return minWarmupSamples;
    }

    /** Rolling CLOB whale-sweep lookback window in milliseconds. */
    public long sweepWindowMs() {
        return sweepWindowMs;
    }

    /** Minimum best-ask upward advance in millicents for price-advancing sweeps. */
    public int minEntryAskAdvanceMc() {
        return minEntryAskAdvanceMc;
    }

    /** Minimum best-bid upward advance in millicents confirming non-vacuum sweep. */
    public int minEntryBidAdvanceMc() {
        return minEntryBidAdvanceMc;
    }

    /** Maximum allowed BBO spread on the traded outcome in millicents. */
    public int maxSpreadMc() {
        return maxSpreadMc;
    }

    /** Minimum effective swept shares required to trigger directional entry. */
    public int minEffectiveSweepShares() {
        return minEffectiveSweepShares;
    }

    /** Minimum mirrored 1-cent bid-wall shares ({@code depth1cOpp}) confirming whale buy support. */
    public int minBidWallShares() {
        return minBidWallShares;
    }

    /** Minimum net non-refilled 1c ask-wall depletion shares at constant tight BBO. */
    public int minWallDepletionShares() {
        return minWallDepletionShares;
    }

    /** Minimum acceptable best-ask price for directional entries in millicents. */
    public int minOpenPriceMc() {
        return minOpenPriceMc;
    }

    /** Maximum acceptable best-ask price for directional entries in millicents. */
    public int maxOpenPriceMc() {
        return maxOpenPriceMc;
    }

    /** Minimum opposite-token ask price for synthetic take-profit in millicents. */
    public int minOffsetTpPriceMc() {
        return minOffsetTpPriceMc;
    }

    /** Maximum opposite-token ask price for synthetic take-profit in millicents. */
    public int maxOffsetTpPriceMc() {
        return maxOffsetTpPriceMc;
    }

    /** Maximum combined pair cost {@code avgMainPriceMc + askOppMc} for synthetic take-profit in millicents. */
    public int maxOffsetTpPairCostMc() {
        return maxOffsetTpPairCostMc;
    }

    /** Minimum combined pair cost {@code avgMainPriceMc + askOppMc} for synthetic stop-loss hedge in millicents. */
    public int minOffsetSlPairCostMc() {
        return minOffsetSlPairCostMc;
    }

    /** Maximum limit-order price buffer above best ask in millicents. */
    public int maxSlippageMc() {
        return maxSlippageMc;
    }

    /** Polymarket parabolic taker fee rate in basis points. */
    public int takerFeeRateBps() {
        return takerFeeRateBps;
    }

    /** Minimum single-order size in integer shares. */
    public int minOrderShares() {
        return minOrderShares;
    }

    /** Maximum single-order size in integer shares. */
    public int maxOrderShares() {
        return maxOrderShares;
    }

    /** Marginal copy-sizing slope above {@code minEffectiveSweepShares} in basis points. */
    public int sweepShareScaleBps() {
        return sweepShareScaleBps;
    }

    /** Swept-share threshold activating convex multi-whale sizing boost. */
    public int convexWhaleSweepThresholdShares() {
        return convexWhaleSweepThresholdShares;
    }

    /** Maximum cumulative shares per outcome in a single 15m market. */
    public int maxInventorySharesPerSide() {
        return maxInventorySharesPerSide;
    }

    /** Default market identifier for order commands. */
    public String defaultMarketId() {
        return defaultMarketId;
    }

    /** Token identifier for UP outcome contract. */
    public String upToken() {
        return upToken;
    }

    /** Token identifier for DOWN outcome contract. */
    public String downToken() {
        return downToken;
    }
}
