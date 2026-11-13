package cz.polymarket.bot.strategy;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Immutable configuration bean for {@link UtterParameciumStrategy}.
 * <p>
 * Encapsulates settlement-window timing, entry price bounds, fat-tail model probability ceiling,
 * progressive TWAP lock-in and conviction-scaled inventory staging, single-direction market commitment,
 * adverse Chainlink and supplementary multi-exchange {@code onCalculatedSpotTwap} guards, CLOB toxic-flow
 * protection, multi-horizon realized volatility bounds, and parabolic taker fee settings.
 */
@ApplicationScoped
public class UtterParameciumStrategyConfig {

    private static final long MAX_ALLOWED_TAU_MS = 65_000L;
    private static final long TWAP_SETTLEMENT_WINDOW_MS = 60_000L;

    private final long minTauMs;
    private final long maxTauMs;
    private final long cooldownMs;
    private final int minEntryPriceMc;
    private final int maxEntryPriceMc;
    private final int maxSpreadMc;
    private final int minModelProbMc;
    private final int maxModelProbMc;
    private final int minEdgeMc;
    private final int maxSlippageMc;
    private final int minOrderShares;
    private final int maxOrderShares;
    private final int edgeScalingFactor;
    private final int maxInventorySharesPerSide;
    private final int earlyInventoryCapBps;
    private final long fullInventoryTauMs;
    private final boolean allowDirectionFlip;
    private final long adverseDrift10sBlockCents;
    private final int adverseAskDrop8sBlockMc;
    private final int calcLeadWeightBps;
    private final long calcAdverseLeadBlockCents;
    private final long calcAdverseDrift4sBlockCents;
    private final long defaultSigma1sCents;
    private final long minSigma1sCents;
    private final long maxSigma1sCents;
    private final long warmupSecsBeforeExpiry;
    private final int takerFeeRateBps;
    private final String defaultMarketId;
    private final String upToken;
    private final String downToken;

    /**
     * Constructs the strategy configuration with injected MicroProfile properties.
     *
     * @param minTauMs minimum remaining time to candle expiry in milliseconds (default {@code 5000} = 5s)
     * @param maxTauMs maximum remaining time to candle expiry in milliseconds, strictly {@code < 65000} (default {@code 64000} = 64s)
     * @param cooldownMs minimum interval between consecutive batch signals in milliseconds (default {@code 300} = 0.30s)
     * @param minEntryPriceMc minimum acceptable best-ask price in millicents (default {@code 380} = $0.38)
     * @param maxEntryPriceMc maximum acceptable best-ask price in millicents for locked-in late entries (default {@code 920} = $0.92; unlocked entries cap at {@code 840})
     * @param maxSpreadMc maximum allowed BBO spread in millicents (default {@code 80} = $0.08)
     * @param minModelProbMc minimum model probability for entries in millicents (default {@code 650} = 65.0%)
     * @param maxModelProbMc fat-tail ceiling on model probability in millicents (default {@code 985} = 98.5%)
     * @param minEdgeMc minimum net price edge after taker fee in millicents (default {@code 25} = $0.025)
     * @param maxSlippageMc maximum allowed limit price slippage above best ask in millicents (default {@code 50} = $0.05)
     * @param minOrderShares minimum order size in integer shares (default {@code 5})
     * @param maxOrderShares base maximum single-order size in integer shares (default {@code 45})
     * @param edgeScalingFactor scaling factor mapping net edge to additional shares (default {@code 90})
     * @param maxInventorySharesPerSide base maximum cumulative shares per direction in one 15m market (default {@code 600})
     * @param earlyInventoryCapBps fraction of {@code maxInventorySharesPerSide} allowed at {@code tau >= 60s} in bps (default {@code 5000} = 50%)
     * @param fullInventoryTauMs remaining time in milliseconds at or below which 100% of time-staged inventory is unlocked (default {@code 36000} = 36s)
     * @param allowDirectionFlip whether opposite-side entries are permitted after opening a position in the market (default {@code false})
     * @param adverseDrift10sBlockCents 10-second adverse Chainlink spot drift in USD cents blocking pre-lock-in entries (default {@code 950} = $9.50)
     * @param adverseAskDrop8sBlockMc 8-second sustained CLOB BBO drop in millicents blocking pre-lock-in entries (default {@code 100} = $0.10)
     * @param calcLeadWeightBps conservative haircut weight applied to adverse basis-aligned multi-exchange spot divergence beyond noise deadband in bps (default {@code 7000} = 70%)
     * @param calcAdverseLeadBlockCents adverse basis-aligned multi-exchange spot lead in USD cents blocking non-locked-in entries (default {@code 650} = $6.50)
     * @param calcAdverseDrift4sBlockCents 4-second adverse multi-exchange spot drift in USD cents blocking non-locked-in entries (default {@code 950} = $9.50)
     * @param defaultSigma1sCents fallback 1-second BTC volatility in USD cents during warm-up (default {@code 160} = $1.60)
     * @param minSigma1sCents lower clamp for 1-second BTC volatility in USD cents (default {@code 105} = $1.05)
     * @param maxSigma1sCents upper clamp for 1-second BTC volatility in USD cents (default {@code 3500} = $35.00)
     * @param warmupSecsBeforeExpiry warm-up window before expiry in seconds (default {@code 120})
     * @param takerFeeRateBps Polymarket parabolic taker fee multiplier in basis points (default {@code 700} = 7.00%)
     * @param defaultMarketId default market identifier for order commands
     * @param upToken token identifier for the UP contract
     * @param downToken token identifier for the DOWN contract
     */
    @Inject
    public UtterParameciumStrategyConfig(
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.min-tau-ms", defaultValue = "5000") long minTauMs,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.max-tau-ms", defaultValue = "64000") long maxTauMs,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.cooldown-ms", defaultValue = "300") long cooldownMs,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.min-entry-price-mc", defaultValue = "380") int minEntryPriceMc,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.max-entry-price-mc", defaultValue = "920") int maxEntryPriceMc,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.max-spread-mc", defaultValue = "80") int maxSpreadMc,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.min-model-prob-mc", defaultValue = "650") int minModelProbMc,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.max-model-prob-mc", defaultValue = "985") int maxModelProbMc,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.min-edge-mc", defaultValue = "25") int minEdgeMc,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.max-slippage-mc", defaultValue = "50") int maxSlippageMc,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.min-order-shares", defaultValue = "5") int minOrderShares,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.max-order-shares", defaultValue = "45") int maxOrderShares,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.edge-scaling-factor", defaultValue = "90") int edgeScalingFactor,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.max-inventory-shares-per-side", defaultValue = "600") int maxInventorySharesPerSide,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.early-inventory-cap-bps", defaultValue = "5000") int earlyInventoryCapBps,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.full-inventory-tau-ms", defaultValue = "36000") long fullInventoryTauMs,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.allow-direction-flip", defaultValue = "false") boolean allowDirectionFlip,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.adverse-drift-10s-block-cents", defaultValue = "950") long adverseDrift10sBlockCents,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.adverse-ask-drop-8s-block-mc", defaultValue = "100") int adverseAskDrop8sBlockMc,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.calc-lead-weight-bps", defaultValue = "7000") int calcLeadWeightBps,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.calc-adverse-lead-block-cents", defaultValue = "650") long calcAdverseLeadBlockCents,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.calc-adverse-drift-4s-block-cents", defaultValue = "950") long calcAdverseDrift4sBlockCents,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.default-sigma-1s-cents", defaultValue = "160") long defaultSigma1sCents,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.min-sigma-1s-cents", defaultValue = "105") long minSigma1sCents,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.max-sigma-1s-cents", defaultValue = "3500") long maxSigma1sCents,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.warmup-secs-before-expiry", defaultValue = "120") long warmupSecsBeforeExpiry,
            @ConfigProperty(name = "polymarket.strategy.utter-paramecium.taker-fee-rate-bps", defaultValue = "700") int takerFeeRateBps,
            @ConfigProperty(name = "polymarket.strategy.default-market-id", defaultValue = "pm-btc-15m") String defaultMarketId,
            @ConfigProperty(name = "polymarket.strategy.up-token", defaultValue = "BTC-UP") String upToken,
            @ConfigProperty(name = "polymarket.strategy.down-token", defaultValue = "BTC-DOWN") String downToken) {

        if (minTauMs < 0L || maxTauMs <= minTauMs || maxTauMs >= MAX_ALLOWED_TAU_MS) {
            throw new IllegalArgumentException("Invalid tau window: 0 <= minTauMs < maxTauMs < 65000 required");
        }
        if (minEntryPriceMc <= 0 || maxEntryPriceMc >= 1_000 || minEntryPriceMc > maxEntryPriceMc) {
            throw new IllegalArgumentException("Entry price bounds must be in (0, 1000) with minEntryPriceMc <= maxEntryPriceMc");
        }
        if (minModelProbMc <= 0 || maxModelProbMc >= 1_000 || minModelProbMc > maxModelProbMc) {
            throw new IllegalArgumentException("Model probability bounds must be in (0, 1000) with minModelProbMc <= maxModelProbMc");
        }
        if (maxSlippageMc < 0 || maxSlippageMc >= 1_000) {
            throw new IllegalArgumentException("maxSlippageMc must be in [0, 1000)");
        }
        if (minOrderShares < 5 || maxOrderShares < minOrderShares) {
            throw new IllegalArgumentException("Order shares must satisfy 5 <= minOrderShares <= maxOrderShares");
        }
        if (earlyInventoryCapBps <= 0 || earlyInventoryCapBps > 10_000) {
            throw new IllegalArgumentException("earlyInventoryCapBps must be in (0, 10000]");
        }
        if (fullInventoryTauMs < 0L || fullInventoryTauMs >= TWAP_SETTLEMENT_WINDOW_MS) {
            throw new IllegalArgumentException("fullInventoryTauMs must be in [0, 60000)");
        }
        if (adverseDrift10sBlockCents < 0L) {
            throw new IllegalArgumentException("adverseDrift10sBlockCents must be >= 0");
        }
        if (adverseAskDrop8sBlockMc < 0) {
            throw new IllegalArgumentException("adverseAskDrop8sBlockMc must be >= 0");
        }
        if (calcLeadWeightBps < 0 || calcLeadWeightBps > 10_000) {
            throw new IllegalArgumentException("calcLeadWeightBps must be in [0, 10000]");
        }
        if (calcAdverseLeadBlockCents < 0L) {
            throw new IllegalArgumentException("calcAdverseLeadBlockCents must be >= 0");
        }
        if (calcAdverseDrift4sBlockCents < 0L) {
            throw new IllegalArgumentException("calcAdverseDrift4sBlockCents must be >= 0");
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
        this.maxModelProbMc = maxModelProbMc;
        this.minEdgeMc = minEdgeMc;
        this.maxSlippageMc = maxSlippageMc;
        this.minOrderShares = minOrderShares;
        this.maxOrderShares = maxOrderShares;
        this.edgeScalingFactor = edgeScalingFactor;
        this.maxInventorySharesPerSide = maxInventorySharesPerSide;
        this.earlyInventoryCapBps = earlyInventoryCapBps;
        this.fullInventoryTauMs = fullInventoryTauMs;
        this.allowDirectionFlip = allowDirectionFlip;
        this.adverseDrift10sBlockCents = adverseDrift10sBlockCents;
        this.adverseAskDrop8sBlockMc = adverseAskDrop8sBlockMc;
        this.calcLeadWeightBps = calcLeadWeightBps;
        this.calcAdverseLeadBlockCents = calcAdverseLeadBlockCents;
        this.calcAdverseDrift4sBlockCents = calcAdverseDrift4sBlockCents;
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
     * Creates a default configuration instance calibrated with Iteration 4 pre-window activation ({@code 64s}),
     * two-tier locked-in ceiling ({@code 840 / 920 mc}), bidirectional {@code onCalculatedSpotTwap} breakout
     * confirmation, recalibrated Z-curve, and dynamic conviction batch/inventory sizing.
     *
     * @return default strategy configuration
     */
    public static UtterParameciumStrategyConfig defaultModelV1() {
        return new UtterParameciumStrategyConfig(
                5_000L,
                64_000L,
                300L,
                380,
                920,
                80,
                650,
                985,
                25,
                50,
                5,
                45,
                90,
                600,
                5_000,
                36_000L,
                false,
                950L,
                100,
                7_000,
                650L,
                950L,
                160L,
                105L,
                3_500L,
                120L,
                700,
                "pm-btc-15m",
                "BTC-UP",
                "BTC-DOWN"
        );
    }

    /**
     * Minimum remaining time to candle expiry in milliseconds (default 5,000 ms).
     */
    public long minTauMs() {
        return minTauMs;
    }

    /**
     * Maximum remaining time to candle expiry in milliseconds (default 64,000 ms).
     */
    public long maxTauMs() {
        return maxTauMs;
    }

    /**
     * Minimum interval between consecutive signals in milliseconds (default 300 ms).
     */
    public long cooldownMs() {
        return cooldownMs;
    }

    /**
     * Minimum acceptable ask price in millicents (default 380 mc = $0.38).
     */
    public int minEntryPriceMc() {
        return minEntryPriceMc;
    }

    /**
     * Maximum acceptable ask price in millicents for locked-in late entries (default 920 mc = $0.92).
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
     * Minimum model probability in millicents (default 650 mc = 65.0%).
     */
    public int minModelProbMc() {
        return minModelProbMc;
    }

    /**
     * Maximum fat-tail model probability cap in millicents (default 985 mc = 98.5%).
     */
    public int maxModelProbMc() {
        return maxModelProbMc;
    }

    /**
     * Minimum net price edge after taker fee in millicents (default 25 mc = $0.025).
     */
    public int minEdgeMc() {
        return minEdgeMc;
    }

    /**
     * Maximum allowed limit price slippage above best ask in millicents (default 50 mc = $0.05).
     */
    public int maxSlippageMc() {
        return maxSlippageMc;
    }

    /**
     * Base minimum order size in integer shares (default 5).
     */
    public int minOrderShares() {
        return minOrderShares;
    }

    /**
     * Base maximum single-order size in integer shares (default 45).
     */
    public int maxOrderShares() {
        return maxOrderShares;
    }

    /**
     * Edge scaling coefficient for order sizing (default 90).
     */
    public int edgeScalingFactor() {
        return edgeScalingFactor;
    }

    /**
     * Base maximum cumulative shares per side in a single 15m market (default 600).
     */
    public int maxInventorySharesPerSide() {
        return maxInventorySharesPerSide;
    }

    /**
     * Early-window fraction of maximum inventory allowed at {@code tau >= 60s} in basis points (default 5,000 = 50%).
     */
    public int earlyInventoryCapBps() {
        return earlyInventoryCapBps;
    }

    /**
     * Remaining time in milliseconds at or below which 100% of time-staged inventory is unlocked (default 36,000 ms).
     */
    public long fullInventoryTauMs() {
        return fullInventoryTauMs;
    }

    /**
     * Whether opposite-direction entries are allowed once a position is held in the active market (default false).
     */
    public boolean allowDirectionFlip() {
        return allowDirectionFlip;
    }

    /**
     * Adverse 10-second Chainlink spot drift threshold in USD cents blocking pre-lock-in entries (default 950 = $9.50).
     */
    public long adverseDrift10sBlockCents() {
        return adverseDrift10sBlockCents;
    }

    /**
     * Adverse 8-second sustained CLOB BBO drop in millicents blocking pre-lock-in entries (default 100 = $0.10).
     */
    public int adverseAskDrop8sBlockMc() {
        return adverseAskDrop8sBlockMc;
    }

    /**
     * Conservative haircut weight applied to adverse basis-aligned {@code onCalculatedSpotTwap} divergence beyond noise deadband in bps (default 7,000 = 70%).
     */
    public int calcLeadWeightBps() {
        return calcLeadWeightBps;
    }

    /**
     * Adverse basis-aligned {@code onCalculatedSpotTwap} divergence in USD cents blocking non-locked-in entries (default 650 = $6.50).
     */
    public long calcAdverseLeadBlockCents() {
        return calcAdverseLeadBlockCents;
    }

    /**
     * Adverse 4-second {@code onCalculatedSpotTwap} drift in USD cents blocking non-locked-in entries (default 950 = $9.50).
     */
    public long calcAdverseDrift4sBlockCents() {
        return calcAdverseDrift4sBlockCents;
    }

    /**
     * Default 1-second BTC volatility in USD cents during warm-up (default 160 = $1.60).
     */
    public long defaultSigma1sCents() {
        return defaultSigma1sCents;
    }

    /**
     * Minimum clamp for 1-second BTC volatility in USD cents (default 105 = $1.05).
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
