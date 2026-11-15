package cz.polymarket.bot.strategy;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Immutable configuration bean for {@link UtterParameciumStrategy}.
 * <p>
 * Parameters are calibrated to replicate the empirical execution, de-biased multi-exchange spot lead,
 * heavy-tail probability conservatism, micro-clip sizing, and fractional-Kelly position caps of
 * trader {@code 0x44832d0d2ec11187c1e77d786feb15f6a50254c6} ({@code Utter-Paramecium}).
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
    private final int maxSlippageMc;
    private final int minOrderShares;
    private final int maxOrderShares;
    private final int baseClipUsdCents;
    private final int edgeScalingFactor;
    private final int baseRiskUsdCents;
    private final int kellyRiskPerEdgeMcCents;
    private final int maxInventorySharesPerSide;
    private final long spotLeadLagMs;
    private final int spotLeadBetaBps;
    private final long defaultSigma1sCents;
    private final long minSigma1sCents;
    private final long maxSigma1sCents;
    private final long warmupSecsBeforeExpiry;
    private final int takerFeeRateBps;
    private final String defaultMarketId;
    private final String upToken;
    private final String downToken;

    /**
     * Constructs the {@link UtterParameciumStrategy} configuration with injected MicroProfile properties.
     *
     * @param minTauMs minimum remaining time to candle expiry in milliseconds (default {@code 1000} = 1s)
     * @param maxTauMs maximum remaining time to candle expiry in milliseconds (default {@code 61000} = 61s)
     * @param cooldownMs minimum interval between consecutive batch signals in milliseconds (default {@code 110})
     * @param minEntryPriceMc minimum acceptable best-ask price in millicents (default {@code 250} = $0.25)
     * @param maxEntryPriceMc maximum acceptable best-ask price in millicents (default {@code 970} = $0.97)
     * @param maxSpreadMc maximum allowed BBO spread on the traded token in millicents (default {@code 200} = $0.20)
     * @param minModelProbMc minimum model probability in millicents (default {@code 590} = 59.0%)
     * @param minEdgeMc minimum net price edge after taker fee in millicents (default {@code 22} = $0.022)
     * @param maxSlippageMc maximum edge-preserving limit price slippage above best ask in millicents (default {@code 30} = $0.03)
     * @param minOrderShares minimum single-order size in integer shares (default {@code 5})
     * @param maxOrderShares maximum single-order size in integer shares (default {@code 80})
     * @param baseClipUsdCents base single-order notional in USD cents before edge scaling (default {@code 1250} = $12.50)
     * @param edgeScalingFactor additional order notional in USD cents per 1 mc of excess edge (default {@code 18})
     * @param baseRiskUsdCents base unhedged risk budget per market in USD cents at full lock-in (default {@code 4500} = $45.00)
     * @param kellyRiskPerEdgeMcCents fractional-Kelly bankroll unit in USD cents (default {@code 760} = $760.00 bankroll)
     * @param maxInventorySharesPerSide absolute ceiling on cumulative shares per side in one 15m market (default {@code 2400})
     * @param spotLeadLagMs empirical lag of PolyBolt Chainlink behind direct USD spot exchanges in milliseconds (default {@code 1700})
     * @param spotLeadBetaBps de-biased spot innovation transfer coefficient in basis points (default {@code 7500} = 0.75)
     * @param defaultSigma1sCents fallback 1-second BTC volatility in USD cents when fewer than 5 samples exist (default {@code 150} = $1.50)
     * @param minSigma1sCents lower clamp for 1-second BTC volatility in USD cents (default {@code 85} = $0.85)
     * @param maxSigma1sCents upper clamp for 1-second BTC volatility in USD cents (default {@code 3500} = $35.00)
     * @param warmupSecsBeforeExpiry warm-up window before expiry in seconds (default {@code 120})
     * @param takerFeeRateBps Polymarket parabolic taker fee multiplier in basis points (default {@code 700} = 7.00%)
     * @param defaultMarketId default market identifier for order commands
     * @param upToken token identifier for the UP contract
     * @param downToken token identifier for the DOWN contract
     */
    @Inject
    public UtterParameciumStrategyConfig(
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.min-tau-ms", defaultValue = "1000") long minTauMs,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.max-tau-ms", defaultValue = "61000") long maxTauMs,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.cooldown-ms", defaultValue = "110") long cooldownMs,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.min-entry-price-mc", defaultValue = "250") int minEntryPriceMc,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.max-entry-price-mc", defaultValue = "970") int maxEntryPriceMc,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.max-spread-mc", defaultValue = "200") int maxSpreadMc,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.min-model-prob-mc", defaultValue = "590") int minModelProbMc,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.min-edge-mc", defaultValue = "22") int minEdgeMc,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.max-slippage-mc", defaultValue = "30") int maxSlippageMc,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.min-order-shares", defaultValue = "5") int minOrderShares,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.max-order-shares", defaultValue = "80") int maxOrderShares,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.base-clip-usd-cents", defaultValue = "1250") int baseClipUsdCents,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.edge-scaling-factor", defaultValue = "18") int edgeScalingFactor,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.base-risk-usd-cents", defaultValue = "4500") int baseRiskUsdCents,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.kelly-risk-per-edge-mc-cents", defaultValue = "760") int kellyRiskPerEdgeMcCents,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.max-inventory-shares-per-side", defaultValue = "2400") int maxInventorySharesPerSide,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.spot-lead-lag-ms", defaultValue = "1700") long spotLeadLagMs,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.spot-lead-beta-bps", defaultValue = "7500") int spotLeadBetaBps,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.default-sigma-1s-cents", defaultValue = "150") long defaultSigma1sCents,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.min-sigma-1s-cents", defaultValue = "85") long minSigma1sCents,
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
        if (maxSlippageMc < 0 || maxSlippageMc >= 1_000) {
            throw new IllegalArgumentException("maxSlippageMc must be in [0, 1000)");
        }
        if (minOrderShares < 5 || maxOrderShares < minOrderShares) {
            throw new IllegalArgumentException("Order shares must satisfy 5 <= minOrderShares <= maxOrderShares");
        }
        if (baseClipUsdCents <= 0 || baseRiskUsdCents < 0 || kellyRiskPerEdgeMcCents < 0) {
            throw new IllegalArgumentException("Notional clip and Kelly risk budgets must be non-negative");
        }
        if (spotLeadLagMs < 0L || spotLeadBetaBps < 0 || spotLeadBetaBps > 10_000) {
            throw new IllegalArgumentException("Spot lead parameters must satisfy lagMs >= 0 and betaBps in [0, 10000]");
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
        this.maxSlippageMc = maxSlippageMc;
        this.minOrderShares = minOrderShares;
        this.maxOrderShares = maxOrderShares;
        this.baseClipUsdCents = baseClipUsdCents;
        this.edgeScalingFactor = Math.max(0, edgeScalingFactor);
        this.baseRiskUsdCents = baseRiskUsdCents;
        this.kellyRiskPerEdgeMcCents = kellyRiskPerEdgeMcCents;
        this.maxInventorySharesPerSide = maxInventorySharesPerSide;
        this.spotLeadLagMs = spotLeadLagMs;
        this.spotLeadBetaBps = spotLeadBetaBps;
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
     * Creates a default configuration instance calibrated to empirical {@code Utter-Paramecium} parameters.
     *
     * @return calibrated default configuration
     */
    public static UtterParameciumStrategyConfig defaultModelV1() {
        return new UtterParameciumStrategyConfig(
                1_000L,
                61_000L,
                110L,
                250,
                970,
                200,
                590,
                22,
                30,
                5,
                80,
                1_250,
                18,
                4_500,
                760,
                2_400,
                1_700L,
                7_500,
                150L,
                85L,
                3_500L,
                120L,
                700,
                "pm-btc-15m",
                "BTC-UP",
                "BTC-DOWN"
        );
    }

    /**
     * Minimum remaining time to candle expiry in milliseconds (default 1,000 ms).
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
     * Minimum interval between consecutive batch orders in milliseconds (default 110 ms).
     */
    public long cooldownMs() {
        return cooldownMs;
    }

    /**
     * Minimum acceptable ask price in millicents (default 250 mc = $0.25).
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
     * Maximum allowed BBO spread on the traded side in millicents (default 200 mc = $0.20).
     */
    public int maxSpreadMc() {
        return maxSpreadMc;
    }

    /**
     * Minimum model probability in millicents (default 590 mc = 59.0%).
     */
    public int minModelProbMc() {
        return minModelProbMc;
    }

    /**
     * Minimum net price edge after taker fee in millicents (default 22 mc = $0.022).
     */
    public int minEdgeMc() {
        return minEdgeMc;
    }

    /**
     * Maximum allowed edge-preserving limit price slippage above best ask in millicents (default 30 mc = $0.03).
     */
    public int maxSlippageMc() {
        return maxSlippageMc;
    }

    /**
     * Minimum single-order size in integer shares (default 5).
     */
    public int minOrderShares() {
        return minOrderShares;
    }

    /**
     * Maximum single-order size in integer shares (default 80).
     */
    public int maxOrderShares() {
        return maxOrderShares;
    }

    /**
     * Base single-order notional in USD cents before excess-edge scaling (default 1,250 = $12.50).
     */
    public int baseClipUsdCents() {
        return baseClipUsdCents;
    }

    /**
     * Additional single-order notional in USD cents per 1 mc of excess edge (default 18 = $0.18/mc).
     */
    public int edgeScalingFactor() {
        return edgeScalingFactor;
    }

    /**
     * Base unhedged risk budget per market in USD cents (default 4,500 = $45.00).
     */
    public int baseRiskUsdCents() {
        return baseRiskUsdCents;
    }

    /**
     * Fractional-Kelly bankroll unit in USD cents per 0.001 Kelly fraction (default 760 = $760.00 bankroll).
     */
    public int kellyRiskPerEdgeMcCents() {
        return kellyRiskPerEdgeMcCents;
    }

    /**
     * Absolute ceiling on cumulative shares per side in a single 15m market (default 2,400).
     */
    public int maxInventorySharesPerSide() {
        return maxInventorySharesPerSide;
    }

    /**
     * Empirical lag of PolyBolt Chainlink behind direct USD spot exchanges in milliseconds (default 1,700 ms).
     */
    public long spotLeadLagMs() {
        return spotLeadLagMs;
    }

    /**
     * De-biased spot innovation transfer coefficient in basis points (default 7,500 = 0.75).
     */
    public int spotLeadBetaBps() {
        return spotLeadBetaBps;
    }

    /**
     * Default 1-second BTC volatility in USD cents when fewer than 5 samples exist (default 150 = $1.50).
     */
    public long defaultSigma1sCents() {
        return defaultSigma1sCents;
    }

    /**
     * Minimum clamp for 1-second BTC volatility in USD cents (default 85 = $0.85).
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
