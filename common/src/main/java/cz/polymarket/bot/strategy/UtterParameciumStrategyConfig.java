package cz.polymarket.bot.strategy;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Configuration bean for {@link UtterParameciumStrategy} (reconstructed Model V1).
 * <p>
 * All parameters are represented in deterministic fixed-point integer units
 * (milliseconds for durations, USD cents for BTC prices/volatility, and millicents {@code 0..1000}
 * for binary contract prices) to guarantee a zero-allocation, floating-point-free hot path.
 */
@ApplicationScoped
public class UtterParameciumStrategyConfig {

    private final long minTauMs;
    private final long maxTauMs;
    private final long cooldownMs;
    private final int minEntryPriceMc;
    private final int maxEntryPriceMc;
    private final int maxSpreadMc;
    private final int minModelProbMc;
    private final int minEdgeMc;
    private final int minOrderShares;
    private final int maxOrderShares;
    private final int initialBurstShares;
    private final int edgeScalingFactor;
    private final int maxInventorySharesPerSide;
    private final long defaultSigma1sCents;
    private final long minSigma1sCents;
    private final long maxSigma1sCents;
    private final long warmupSecsBeforeExpiry;
    private final int takerFeeRateBps;
    private final String defaultMarketId;
    private final String upToken;
    private final String downToken;

    /**
     * Constructs the Model V1 strategy configuration with injected MicroProfile properties.
     *
     * @param minTauMs minimum remaining time to candle expiry in milliseconds (default {@code 10000} = 10s)
     * @param maxTauMs maximum remaining time to candle expiry in milliseconds (default {@code 61000} = 61s)
     * @param cooldownMs minimum interval between consecutive batch signals in milliseconds (default {@code 200})
     * @param minEntryPriceMc minimum acceptable best-ask price in millicents (default {@code 300} = $0.30)
     * @param maxEntryPriceMc maximum acceptable best-ask price in millicents (default {@code 970} = $0.97)
     * @param maxSpreadMc maximum allowed BBO spread in millicents (default {@code 80} = $0.08)
     * @param minModelProbMc minimum model probability for favorite entries in millicents (default {@code 540} = 54.0%)
     * @param minEdgeMc minimum net price edge after taker fee in millicents (default {@code 15} = $0.015)
     * @param minOrderShares minimum order size in integer shares (default {@code 5})
     * @param maxOrderShares maximum single-order size in integer shares (default {@code 35})
     * @param initialBurstShares first-order burst size in shares when {@code tau >= 55s} (default {@code 15})
     * @param edgeScalingFactor scaling factor mapping net edge to additional shares (default {@code 70})
     * @param maxInventorySharesPerSide maximum cumulative shares per direction in one 15m market (default {@code 600})
     * @param defaultSigma1sCents fallback 1-second BTC volatility in USD cents when {@code M < 5} (default {@code 1000} = $10.00)
     * @param minSigma1sCents lower clamp for 1-second BTC volatility in USD cents (default {@code 500} = $5.00)
     * @param maxSigma1sCents upper clamp for 1-second BTC volatility in USD cents (default {@code 3500} = $35.00)
     * @param warmupSecsBeforeExpiry warm-up window before expiry in seconds (default {@code 120})
     * @param takerFeeRateBps Polymarket parabolic taker fee multiplier in basis points (default {@code 700} = 7.00%)
     * @param defaultMarketId default market identifier for order commands
     * @param upToken token identifier for the UP contract
     * @param downToken token identifier for the DOWN contract
     */
    @Inject
    public UtterParameciumStrategyConfig(
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.min-tau-ms", defaultValue = "10000") long minTauMs,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.max-tau-ms", defaultValue = "61000") long maxTauMs,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.cooldown-ms", defaultValue = "200") long cooldownMs,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.min-entry-price-mc", defaultValue = "300") int minEntryPriceMc,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.max-entry-price-mc", defaultValue = "970") int maxEntryPriceMc,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.max-spread-mc", defaultValue = "80") int maxSpreadMc,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.min-model-prob-mc", defaultValue = "540") int minModelProbMc,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.min-edge-mc", defaultValue = "15") int minEdgeMc,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.min-order-shares", defaultValue = "5") int minOrderShares,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.max-order-shares", defaultValue = "35") int maxOrderShares,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.initial-burst-shares", defaultValue = "15") int initialBurstShares,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.edge-scaling-factor", defaultValue = "70") int edgeScalingFactor,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.max-inventory-shares-per-side", defaultValue = "600") int maxInventorySharesPerSide,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.default-sigma-1s-cents", defaultValue = "1000") long defaultSigma1sCents,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.min-sigma-1s-cents", defaultValue = "500") long minSigma1sCents,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.max-sigma-1s-cents", defaultValue = "3500") long maxSigma1sCents,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.warmup-secs-before-expiry", defaultValue = "120") long warmupSecsBeforeExpiry,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.taker-fee-rate-bps", defaultValue = "700") int takerFeeRateBps,
            @ConfigProperty(name = "polymarket.strategy.default-market-id", defaultValue = "pm-btc-15m") String defaultMarketId,
            @ConfigProperty(name = "polymarket.strategy.up-token", defaultValue = "BTC-UP") String upToken,
            @ConfigProperty(name = "polymarket.strategy.down-token", defaultValue = "BTC-DOWN") String downToken) {

        if (minTauMs < 0L || maxTauMs <= minTauMs) {
            throw new IllegalArgumentException("Invalid tau window: minTauMs must be >= 0 and < maxTauMs");
        }
        if (minEntryPriceMc <= 0 || maxEntryPriceMc >= 1_000 || minEntryPriceMc > maxEntryPriceMc) {
            throw new IllegalArgumentException("Entry price bounds must be in (0, 1000) with minEntryPriceMc <= maxEntryPriceMc");
        }
        if (minOrderShares < 5 || maxOrderShares < minOrderShares) {
            throw new IllegalArgumentException("Order shares must satisfy 5 <= minOrderShares <= maxOrderShares");
        }
        if (minSigma1sCents <= 0L || maxSigma1sCents < minSigma1sCents) {
            throw new IllegalArgumentException("Volatility bounds must satisfy 0 < minSigma1sCents <= maxSigma1sCents");
        }

        this.minTauMs = minTauMs;
        this.maxTauMs = maxTauMs;
        this.cooldownMs = Math.max(0L, cooldownMs);
        this.minEntryPriceMc = minEntryPriceMc;
        this.maxEntryPriceMc = maxEntryPriceMc;
        this.maxSpreadMc = maxSpreadMc;
        this.minModelProbMc = minModelProbMc;
        this.minEdgeMc = minEdgeMc;
        this.minOrderShares = minOrderShares;
        this.maxOrderShares = maxOrderShares;
        this.initialBurstShares = initialBurstShares;
        this.edgeScalingFactor = edgeScalingFactor;
        this.maxInventorySharesPerSide = maxInventorySharesPerSide;
        this.defaultSigma1sCents = defaultSigma1sCents;
        this.minSigma1sCents = minSigma1sCents;
        this.maxSigma1sCents = maxSigma1sCents;
        this.warmupSecsBeforeExpiry = warmupSecsBeforeExpiry;
        this.takerFeeRateBps = takerFeeRateBps;
        this.defaultMarketId = (defaultMarketId != null && !defaultMarketId.isBlank()) ? defaultMarketId : "pm-btc-15m";
        this.upToken = (upToken != null && !upToken.isBlank()) ? upToken : "BTC-UP";
        this.downToken = (downToken != null && !downToken.isBlank()) ? downToken : "BTC-DOWN";
    }

    /**
     * Creates a default configuration instance calibrated to Model V1 empirical parameters.
     *
     * @return default Model V1 configuration
     */
    public static UtterParameciumStrategyConfig defaultModelV1() {
        return new UtterParameciumStrategyConfig(
                10_000L,
                61_000L,
                200L,
                300,
                970,
                80,
                540,
                15,
                5,
                35,
                15,
                70,
                600,
                1_000L,
                500L,
                3_500L,
                120L,
                700,
                "pm-btc-15m",
                "BTC-UP",
                "BTC-DOWN"
        );
    }

    /**
     * Minimum remaining time to candle expiry in milliseconds (default 10,000 ms).
     */
    public long minTauMs() {
        return minTauMs;
    }

    /**
     * Maximum remaining time to candle expiry in milliseconds (default 61,000 ms).
     */
    public long maxTauMs() {
        return maxTauMs;
    }

    /**
     * Minimum interval between consecutive signals in a batch in milliseconds (default 200 ms).
     */
    public long cooldownMs() {
        return cooldownMs;
    }

    /**
     * Minimum acceptable ask price in millicents (default 300 mc = $0.30).
     */
    public int minEntryPriceMc() {
        return minEntryPriceMc;
    }

    /**
     * Maximum acceptable ask price in millicents (default 970 mc = $0.97).
     */
    public int maxEntryPriceMc() {
        return maxEntryPriceMc;
    }

    /**
     * Maximum allowed BBO spread in millicents (default 80 mc = $0.08).
     */
    public int maxSpreadMc() {
        return maxSpreadMc;
    }

    /**
     * Minimum model probability in millicents (default 540 mc = 54.0%).
     */
    public int minModelProbMc() {
        return minModelProbMc;
    }

    /**
     * Minimum net price edge after taker fee in millicents (default 15 mc = $0.015).
     */
    public int minEdgeMc() {
        return minEdgeMc;
    }

    /**
     * Base order size in integer shares (default 5).
     */
    public int minOrderShares() {
        return minOrderShares;
    }

    /**
     * Maximum single-order size in integer shares (default 35).
     */
    public int maxOrderShares() {
        return maxOrderShares;
    }

    /**
     * Initial burst order size in integer shares when {@code tau >= 55s} (default 15).
     */
    public int initialBurstShares() {
        return initialBurstShares;
    }

    /**
     * Edge scaling coefficient for order sizing (default 70).
     */
    public int edgeScalingFactor() {
        return edgeScalingFactor;
    }

    /**
     * Maximum cumulative shares per side in a single 15m market (default 600).
     */
    public int maxInventorySharesPerSide() {
        return maxInventorySharesPerSide;
    }

    /**
     * Default 1-second BTC volatility in USD cents when fewer than 5 samples exist (default 1,000 = $10.00).
     */
    public long defaultSigma1sCents() {
        return defaultSigma1sCents;
    }

    /**
     * Minimum clamp for 1-second BTC volatility in USD cents (default 500 = $5.00).
     */
    public long minSigma1sCents() {
        return minSigma1sCents;
    }

    /**
     * Maximum clamp for 1-second BTC volatility in USD cents (default 3,500 = $35.00).
     */
    public long maxSigma1sCents() {
        return maxSigma1sCents;
    }

    /**
     * Warm-up window before expiry in seconds (default 120s).
     */
    public long warmupSecsBeforeExpiry() {
        return warmupSecsBeforeExpiry;
    }

    /**
     * Polymarket parabolic taker fee multiplier in basis points (default 700 = 7.00%).
     */
    public int takerFeeRateBps() {
        return takerFeeRateBps;
    }

    /**
     * Default market identifier for order commands.
     */
    public String defaultMarketId() {
        return defaultMarketId;
    }

    /**
     * Token identifier for UP outcome contract.
     */
    public String upToken() {
        return upToken;
    }

    /**
     * Token identifier for DOWN outcome contract.
     */
    public String downToken() {
        return downToken;
    }
}
