package cz.polymarket.bot.strategy;

import cz.polymarket.bot.domain.OrderBookQuote;
import cz.polymarket.bot.domain.Timeframe;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.math.BigDecimal;
import java.util.Set;

/**
 * Reconstructed {@code Utter-Paramecium} final-minute 60s TWAP settlement strategy for 15-minute
 * binary prediction markets.
 * <p>
 * Combines Polymarket PolyBolt oracle streams ({@code price.crypto} and {@code price.crypto.twap})
 * with de-biased real-time multi-exchange USD spot innovations ({@link #onCalculatedSpotTwap}) to
 * anticipate the ~1.7s Chainlink aggregation lag without cross-venue basis drift.
 * <p>
 * Key quantitative components:
 * <ul>
 *   <li><b>Live 60s TWAP rolling-shift projection</b>: Anchors inside the settlement window on the latest
 *       PolyBolt {@code price.crypto.twap} tick and projects exact roll-out/roll-in dynamics with de-biased
 *       spot innovations {@code beta * (S_calc(t) - S_calc(t_CL - lagMs))}.</li>
 *   <li><b>Heavy-tail Student-t probability</b>: Replaces Gaussian tail overconfidence in {@code [1.6, 5.0]}
 *       sigma with an empirically calibrated two-regime heavy-tail CDF lookup table.</li>
 *   <li><b>Banked-TWAP surplus &amp; flip discipline</b>: Caps deficit-chasing exposure when the already-banked
 *       settlement TWAP opposes current spot, while allowing decisive directional reversals with banked TWAP support.</li>
 *   <li><b>Fractional-Kelly sizing &amp; micro-clipping</b>: Scales per-order notional clips and tail-adjusted
 *       market exposure strictly with binary fractional-Kelly edge and a volatility-compression sizing floor.</li>
 * </ul>
 * <p>
 * All hot-path calculations use deterministic integer/fixed-point arithmetic with pre-allocated ring buffers
 * and zero heap allocations per market tick.
 */
@ApplicationScoped
public class UtterParameciumStrategy implements TradingStrategy {

    /**
     * Pre-computed remaining 60s TWAP standard deviation weights
     * {@code W_bps(tau) = round(10_000 * sqrt(tau*(tau+1)*(2*tau+1)/6) / 60)} for {@code tau = 0..61} seconds.
     */
    public static final int[] TWAP_REM_WEIGHT_BPS = {
                0,   167,   373,   624,   913,  1236,  1591,  1974,  2385,  2822,
             3284,  3770,  4279,  4810,  5363,  5937,  6531,  7145,  7778,  8430,
             9100,  9788, 10494, 11217, 11957, 12713, 13485, 14273, 15077, 15896,
            16730, 17579, 18442, 19319, 20211, 21116, 22035, 22967, 23913, 24872,
            25843, 26827, 27824, 28833, 29854, 30887, 31932, 32989, 34057, 35137,
            36228, 37330, 38443, 39567, 40702, 41847, 43003, 44169, 45346, 46533,
            47730, 48937
    };

    /**
     * Empirically calibrated early-window heavy-tail cumulative distribution table
     * in millicents ({@code 500..995 mc}) for {@code z = 0.00, 0.10, 0.20, ..., 6.00} sigma (61 points).
     */
    public static final int[] HEAVY_TAIL_CDF_HALF_TABLE_MC = {
            500, 540, 579, 617, 654, 688, 720, 749, 776, 801,
            823, 842, 859, 874, 887, 898, 908, 917, 925, 932,
            938, 944, 949, 954, 958, 962, 965, 968, 971, 973,
            975, 977, 979, 980, 982, 983, 984, 985, 986, 987,
            988, 989, 989, 990, 991, 991, 992, 992, 993, 993,
            993, 994, 994, 994, 994, 995, 995, 995, 995, 995,
            995
    };

    /**
     * Empirically calibrated late lock-in ({@code tau <= 15s}) cumulative distribution table
     * in millicents ({@code 500..998 mc}) reflecting reduced jump risk once most of the 60s TWAP is locked.
     */
    private static final int[] LATE_LOCKIN_CDF_HALF_TABLE_MC = {
            500, 544, 588, 630, 671, 709, 745, 778, 808, 835,
            858, 878, 896, 911, 924, 936, 946, 955, 962, 968,
            973, 977, 980, 983, 985, 987, 989, 990, 991, 992,
            993, 994, 994, 995, 995, 996, 996, 996, 997, 997,
            997, 997, 997, 997, 998, 998, 998, 998, 998, 998,
            998, 998, 998, 998, 998, 998, 998, 998, 998, 998,
            998
    };

    private static final int MAX_MC = 1_000;
    private static final int TICK_STEP_MC = 10;
    private static final long MIN_SIGMA_REM_CENTS = 90L;
    private static final long CANDLE_DURATION_MS = 900_000L;
    private static final long MAX_TARGET_PRICE_OFFSET_MS = 10_000L;
    private static final long TWAP_WINDOW_MS = 60_000L;
    private static final long BUFFER_RETENTION_MS = 130_000L;
    private static final long PRE_ACTIVATION_BALANCE_SYNC_LEAD_MS = 10_000L;
    private static final long LIVE_TWAP_MAX_AGE_MS = 3_500L;
    private static final long LIVE_TWAP_MAX_EFFECTIVE_LAG_MS = 4_500L;
    private static final long DEFICIT_CHASE_MAX_TAU_MS = 52_000L;
    private static final long DEFICIT_CHASE_TRIGGER_CENTS = -50L;
    private static final long DEFICIT_CHASE_MIN_PROJ_CENTS = 250L;
    private static final int DEFICIT_CHASE_MAX_SHARES = 320;
    private static final long EARLY_PHASE_MIN_TAU_MS = 44_000L;
    private static final long EARLY_PHASE_MIN_COOLDOWN_MS = 320L;
    private static final int EARLY_PHASE_MAX_CLIP_SHARES = 28;
    private static final long LATE_LOCKIN_MAX_TAU_MS = 28_000L;
    private static final long LATE_LOCKIN_MIN_BANKED_DIST_CENTS = 800L;
    private static final long LATE_LOCKIN_MIN_PROJ_DIST_CENTS = 280L;
    private static final long LATE_LOCKIN_MIN_NET_DIST_CENTS = 180L;
    private static final long FLIP_MAX_ADVERSE_DELTA_CENTS = -120L;
    private static final long EARLY_NOISE_DIST_CENTS = 15L;
    private static final int DEEP_UNDERDOG_ASK_MC = 360;
    private static final long DEEP_UNDERDOG_MIN_Z_CENTI = 250L;
    private static final int HIGH_CONVICTION_FLIP_MIN_ASK_MC = 300;
    private static final long HIGH_CONVICTION_FLIP_MIN_Z_CENTI = 240L;
    private static final int FLIP_MIN_ASK_MC = 480;
    private static final int FLIP_EXTRA_EDGE_MC = 10;
    private static final long FLIP_MIN_Z_CENTI = 165L;
    private static final int FLIP_MAX_SHARES = 900;
    private static final int DOUBLE_FLIP_MIN_ASK_MC = 780;
    private static final int DOUBLE_FLIP_EXTRA_EDGE_MC = 18;
    private static final long DOUBLE_FLIP_MIN_Z_CENTI = 240L;
    private static final long SIZING_MIN_SIGMA_1S_CENTS = 130L;
    private static final int TOXIC_DROP_FROM_PEAK_MC = 70;
    private static final int SIZING_CAP_DROP_FROM_PEAK_MC = 70;
    private static final int SIZING_CAPPED_EDGE_MC = 55;
    private static final long SPOT_MOMENTUM_WINDOW_MS = 4_000L;
    private static final long ADVERSE_SPOT_DELTA_CENTS = -80L;
    private static final long ACTIVE_INVENTORY_ADVERSE_DELTA_CENTS = -350L;
    private static final long SEVERE_ADVERSE_IMPULSE_CENTS = -900L;
    private static final long SPOT_LEAD_DEADBAND_CENTS = 150L;
    private static final long MAX_SPOT_INNOVATION_CENTS = 25_000L;

    private final UtterParameciumStrategyConfig config;
    private final ChainlinkTwapBuffer chainlinkTwapBuffer = new ChainlinkTwapBuffer();
    private final CalculatedSpotRingBuffer calcSpotBuffer = new CalculatedSpotRingBuffer();
    private final SpotVolatilityBuffer spotVolatilityBuffer = new SpotVolatilityBuffer();

    private StrategyContext context;

    // Active 15-minute candle session state
    private long activeCandleStartMs = 0L;
    private long activeCandleEndMs = 0L;
    private long targetPriceCents = 0L;
    private long nextCandleStartMs = 0L;
    private long nextCandleTargetPriceCents = 0L;
    private boolean missingTargetPriceWarned = false;

    // Real-time PolyBolt price.crypto, live 60s TWAP, & direct multi-exchange USD spot state in USD cents
    private long lastChainlinkPriceCents = 0L;
    private long lastChainlinkTickMs = 0L;
    private long lastLiveTwapCents = 0L;
    private long lastLiveTwapEffectiveMs = 0L;
    private long lastLiveTwapArrivalMs = 0L;
    private long lastCalcSpotCents = 0L;
    private long lastCalcSpotMs = 0L;
    private long lastEvalCalcSpotCents = 0L;
    private long lastCalcEvalMs = 0L;

    // Order book & per-market execution state
    private OrderBookQuote latestValidQuote = null;
    private int netSharesUp = 0;
    private int netSharesDown = 0;
    private int maxFilledAskUpMc = 0;
    private int maxFilledAskDownMc = 0;
    private long totalCostWithFeeUpMcShares = 0L;
    private long totalCostWithFeeDownMcShares = 0L;
    private int tradesInMarket = 0;
    private long lastSignalMs = 0L;
    private boolean hasEmittedSignal = false;
    private boolean balanceRefreshedForMarket = false;
    private boolean orderInFlight = false;
    private TradeDirection pendingDirection = TradeDirection.NO_TRADE;
    private TradeDirection firstDirectionalSide = TradeDirection.NO_TRADE;
    private TradeDirection lastDirectionalSide = TradeDirection.NO_TRADE;
    private int directionalFlipCount = 0;
    private int pendingRequestedShares = 0;
    private int pendingLimitPriceMc = 0;
    private String pendingClientOrderId = null;
    private long orderSequence = 0L;
    private int lastModelProbUpMc = 500;

    /**
     * Constructs {@link UtterParameciumStrategy} with the supplied configuration bean,
     * unwrapping any CDI client proxy for zero-overhead field access on the hot path.
     *
     * @param config strategy configuration
     */
    @Inject
    public UtterParameciumStrategy(UtterParameciumStrategyConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("config cannot be null");
        }
        this.config = config.getClass() == UtterParameciumStrategyConfig.class
                ? config
                : new UtterParameciumStrategyConfig(
                        config.minTauMs(),
                        config.maxTauMs(),
                        config.cooldownMs(),
                        config.minEntryPriceMc(),
                        config.maxEntryPriceMc(),
                        config.maxSpreadMc(),
                        config.minModelProbMc(),
                        config.minEdgeMc(),
                        config.maxSlippageMc(),
                        config.minOrderShares(),
                        config.maxOrderShares(),
                        config.baseClipUsdCents(),
                        config.edgeScalingFactor(),
                        config.baseRiskUsdCents(),
                        config.kellyRiskPerEdgeMcCents(),
                        config.maxInventorySharesPerSide(),
                        config.spotLeadLagMs(),
                        config.spotLeadBetaBps(),
                        config.defaultSigma1sCents(),
                        config.minSigma1sCents(),
                        config.maxSigma1sCents(),
                        config.warmupSecsBeforeExpiry(),
                        config.takerFeeRateBps(),
                        config.defaultMarketId(),
                        config.upToken(),
                        config.downToken());
    }

    /**
     * Evaluates the empirically calibrated heavy-tail (Student-t) cumulative distribution
     * in millicents ({@code 2..998 mc}) using deterministic integer piecewise-linear interpolation.
     *
     * @param zCenti Z-score in hundredths of a standard deviation (e.g. {@code 180 = +1.80 sigma})
     * @return model win probability in millicents between {@code 2} and {@code 998}
     */
    public static int heavyTailCdfMc(long zCenti) {
        return interpolateHalfTableMc(zCenti, HEAVY_TAIL_CDF_HALF_TABLE_MC, 995, 998);
    }

    /**
     * Evaluates the tau-aware model win probability in millicents ({@code 2..998 mc}), smoothly
     * transitioning from the early heavy-tail jump table ({@code tau >= 45s}) to the late TWAP lock-in
     * table ({@code tau <= 15s}) as accumulated Chainlink ticks lock in the settlement average.
     *
     * @param zCenti Z-score in hundredths of a standard deviation
     * @param tauMs remaining time to candle expiry in milliseconds
     * @return model win probability in millicents between {@code 2} and {@code 998}
     */
    public static int modelProbCdfMc(long zCenti, long tauMs) {
        int earlyMc = interpolateHalfTableMc(zCenti, HEAVY_TAIL_CDF_HALF_TABLE_MC, 995, 998);
        if (tauMs >= 45_000L) {
            return earlyMc;
        }
        int lateMc = interpolateHalfTableMc(zCenti, LATE_LOCKIN_CDF_HALF_TABLE_MC, 998, 999);
        if (tauMs <= 15_000L) {
            return lateMc;
        }
        long lateWeightMs = 45_000L - tauMs;
        return (int) ((earlyMc * (30_000L - lateWeightMs) + lateMc * lateWeightMs + 15_000L) / 30_000L);
    }

    private static int interpolateHalfTableMc(long zCenti, int[] halfTableMc, int atSixSigmaMc, int maxPosMc) {
        long absZ = zCenti < 0L ? -zCenti : zCenti;
        int posMc;
        if (absZ >= 1_200L) {
            posMc = maxPosMc;
        } else if (absZ >= 600L) {
            posMc = atSixSigmaMc + (int) (((absZ - 600L) * (maxPosMc - atSixSigmaMc) + 300L) / 600L);
        } else {
            int idx = (int) (absZ / 10L);
            int rem = (int) (absZ % 10L);
            int y0 = halfTableMc[idx];
            int y1 = halfTableMc[idx + 1];
            posMc = y0 + ((y1 - y0) * rem + 5) / 10;
        }
        return zCenti >= 0L ? posMc : (MAX_MC - posMc);
    }

    /**
     * Rounds a sub-cent price in millicents down to the nearest valid 10-mc ($0.01) tick (for Best Bid).
     *
     * @param millicents option price in millicents ({@code 0..1000})
     * @return floor-rounded option price in millicents
     */
    public static int fromMcFloorTick(int millicents) {
        if (millicents < 0 || millicents > MAX_MC) {
            throw new IllegalArgumentException("Option price out of bounds [0, 1000]: " + millicents);
        }
        return (millicents / TICK_STEP_MC) * TICK_STEP_MC;
    }

    /**
     * Rounds a sub-cent price in millicents up to the nearest valid 10-mc ($0.01) tick (for Best Ask).
     *
     * @param millicents option price in millicents ({@code 0..1000})
     * @return ceiling-rounded option price in millicents
     */
    public static int fromMcCeilTick(int millicents) {
        if (millicents < 0 || millicents > MAX_MC) {
            throw new IllegalArgumentException("Option price out of bounds [0, 1000]: " + millicents);
        }
        int rounded = ((millicents + TICK_STEP_MC - 1) / TICK_STEP_MC) * TICK_STEP_MC;
        if (rounded > MAX_MC) {
            throw new IllegalArgumentException("Rounded option price exceeds 1000 mc: " + rounded);
        }
        return rounded;
    }

    /**
     * Computes the parabolic Polymarket taker fee per share in millicents:
     * {@code fee_mc = round(feeRate * ask * (1 - ask))}.
     *
     * @param askMc ask price in millicents ({@code 0..1000})
     * @param feeRateBps taker fee rate in basis points (e.g. {@code 700 = 7.00%})
     * @return taker fee per share in millicents
     */
    public static int takerFeeMc(int askMc, int feeRateBps) {
        if (askMc <= 0 || askMc >= MAX_MC || feeRateBps <= 0) {
            return 0;
        }
        return (int) (((long) feeRateBps * askMc * (MAX_MC - askMc) + 5_000_000L) / 10_000_000L);
    }

    @Override
    public String getName() {
        return "UtterParameciumStrategy";
    }

    @Override
    public Set<IndicatorType> getRequiredIndicators() {
        return Set.of();
    }

    @Override
    public Set<ExternalStreamType> getRequiredExternalStreams() {
        return Set.of();
    }

    @Override
    public long getRequiredClobLookbackMs() {
        return config.maxTauMs() + PRE_ACTIVATION_BALANCE_SYNC_LEAD_MS + 5_000L;
    }

    @Override
    public Timeframe getTimeframe() {
        return Timeframe.FIFTEEN_MINUTES;
    }

    @Override
    public TradingStrategy createInstance() {
        return new UtterParameciumStrategy(config);
    }

    @Override
    public void init(StrategyContext context) {
        if (context == null) {
            throw new IllegalArgumentException("context cannot be null");
        }
        this.context = context;
        Log.infof("Initialized %s (tau=[%d..%d ms], cooldown=%d ms, entryPrice=[%d..%d mc], maxSpread=%d mc, minEdge=%d mc, maxSlippage=%d mc, shares=[%d..%d], maxInventory=%d)",
                getName(),
                config.minTauMs(),
                config.maxTauMs(),
                config.cooldownMs(),
                config.minEntryPriceMc(),
                config.maxEntryPriceMc(),
                config.maxSpreadMc(),
                config.minEdgeMc(),
                config.maxSlippageMc(),
                config.minOrderShares(),
                config.maxOrderShares(),
                config.maxInventorySharesPerSide());
    }

    @Override
    public void reset() {
        chainlinkTwapBuffer.clear();
        calcSpotBuffer.clear();
        spotVolatilityBuffer.clear();
        activeCandleStartMs = 0L;
        activeCandleEndMs = 0L;
        targetPriceCents = 0L;
        nextCandleStartMs = 0L;
        nextCandleTargetPriceCents = 0L;
        missingTargetPriceWarned = false;
        lastChainlinkPriceCents = 0L;
        lastChainlinkTickMs = 0L;
        lastLiveTwapCents = 0L;
        lastLiveTwapEffectiveMs = 0L;
        lastLiveTwapArrivalMs = 0L;
        lastCalcSpotCents = 0L;
        lastCalcSpotMs = 0L;
        resetMarketExecutionState();
    }

    private void resetMarketExecutionState() {
        latestValidQuote = null;
        netSharesUp = 0;
        netSharesDown = 0;
        maxFilledAskUpMc = 0;
        maxFilledAskDownMc = 0;
        totalCostWithFeeUpMcShares = 0L;
        totalCostWithFeeDownMcShares = 0L;
        tradesInMarket = 0;
        lastSignalMs = 0L;
        hasEmittedSignal = false;
        balanceRefreshedForMarket = false;
        orderInFlight = false;
        pendingDirection = TradeDirection.NO_TRADE;
        firstDirectionalSide = TradeDirection.NO_TRADE;
        lastDirectionalSide = TradeDirection.NO_TRADE;
        directionalFlipCount = 0;
        pendingRequestedShares = 0;
        pendingLimitPriceMc = 0;
        pendingClientOrderId = null;
        lastModelProbUpMc = 500;
        lastLiveTwapCents = 0L;
        lastLiveTwapEffectiveMs = 0L;
        lastLiveTwapArrivalMs = 0L;
        lastEvalCalcSpotCents = 0L;
        lastCalcEvalMs = 0L;
    }

    private void ensureActiveCandle(long timestampMs) {
        if (timestampMs <= 0L) {
            return;
        }
        if (activeCandleStartMs > 0L && timestampMs >= activeCandleStartMs && timestampMs <= activeCandleEndMs) {
            return;
        }
        long alignedStartMs = (timestampMs / CANDLE_DURATION_MS) * CANDLE_DURATION_MS;
        if (activeCandleStartMs > 0L && alignedStartMs < activeCandleStartMs) {
            return;
        }
        long alignedEndMs = alignedStartMs + CANDLE_DURATION_MS;
        if (alignedStartMs != activeCandleStartMs) {
            if (lastChainlinkTickMs > 0L && (alignedStartMs < lastChainlinkTickMs || alignedStartMs - lastChainlinkTickMs > 120_000L)) {
                chainlinkTwapBuffer.clear();
                calcSpotBuffer.clear();
                spotVolatilityBuffer.clear();
                lastChainlinkPriceCents = 0L;
                lastChainlinkTickMs = 0L;
                lastCalcSpotCents = 0L;
                lastCalcSpotMs = 0L;
            }
            activeCandleStartMs = alignedStartMs;
            activeCandleEndMs = alignedEndMs;
            targetPriceCents = (alignedStartMs == nextCandleStartMs && nextCandleTargetPriceCents > 0L)
                    ? nextCandleTargetPriceCents
                    : 0L;
            if (targetPriceCents > 0L) {
                Log.infof("Market [%d, %d] target price locked from boundary PolyBolt 60s TWAP: $%.2f",
                        activeCandleStartMs, activeCandleEndMs, targetPriceCents / 100.0);
            }
            nextCandleStartMs = 0L;
            nextCandleTargetPriceCents = 0L;
            missingTargetPriceWarned = false;
            resetMarketExecutionState();
        }
    }

    @Override
    public void onPolymarketCryptoPrice(
            long timestampMs,
            String symbol,
            String source,
            double price,
            long priceTimestampMs) {
        if (price <= 0.0 || Double.isNaN(price) || timestampMs <= 0L) {
            return;
        }
        if (lastChainlinkTickMs > 0L && timestampMs <= lastChainlinkTickMs) {
            return;
        }
        ensureActiveCandle(timestampMs);
        maybeRefreshBalanceBeforeActivation(timestampMs);
        long priceCents = Math.round(price * 100.0);
        recordChainlinkSample(timestampMs, priceCents);

        OrderBookQuote quote = this.latestValidQuote;
        if (quote != null && (quote.timestampMs() <= 0L || timestampMs + 2_000L >= quote.timestampMs())) {
            evaluateEntry(timestampMs, quote);
        }
    }

    private void recordChainlinkSample(long timestampMs, long priceCents) {
        long retentionMs = Math.max(BUFFER_RETENTION_MS, config.warmupSecsBeforeExpiry() * 1_000L + 10_000L);
        long minRetainMs = Math.max(0L, timestampMs - retentionMs);
        chainlinkTwapBuffer.pushAndEvict(timestampMs, priceCents, minRetainMs);
        spotVolatilityBuffer.onTick(timestampMs, priceCents);

        lastChainlinkPriceCents = priceCents;
        lastChainlinkTickMs = timestampMs;
    }

    @Override
    public void onCalculatedSpotTwap(
            long timestampMs,
            String symbol,
            double spotPrice,
            double twap60sPrice) {
        if (spotPrice <= 0.0 || Double.isNaN(spotPrice) || timestampMs <= 0L) {
            return;
        }
        if (lastCalcSpotMs > 0L && timestampMs < lastCalcSpotMs) {
            return;
        }
        ensureActiveCandle(timestampMs);
        maybeRefreshBalanceBeforeActivation(timestampMs);
        long spotCents = Math.round(spotPrice * 100.0);
        calcSpotBuffer.record(timestampMs, spotCents);
        lastCalcSpotCents = spotCents;
        lastCalcSpotMs = timestampMs;

        if (activeCandleEndMs <= 0L || lastChainlinkPriceCents <= 0L) {
            return;
        }
        long tauMs = activeCandleEndMs - timestampMs;
        if (tauMs < config.minTauMs() || tauMs > config.maxTauMs()) {
            return;
        }
        OrderBookQuote quote = this.latestValidQuote;
        if (quote != null
                && (quote.timestampMs() <= 0L
                || (timestampMs >= quote.timestampMs() && timestampMs - quote.timestampMs() <= 5_000L))) {
            if (Math.abs(spotCents - lastEvalCalcSpotCents) >= 15L || (timestampMs - lastCalcEvalMs) >= 50L) {
                lastEvalCalcSpotCents = spotCents;
                lastCalcEvalMs = timestampMs;
                evaluateEntry(timestampMs, quote);
            }
        }
    }

    @Override
    public void onPolymarketCryptoTwap60s(
            long timestampMs,
            String symbol,
            String source,
            double twapPrice,
            int windowSeconds,
            long twapTimestampMs) {
        if (twapPrice <= 0.0 || Double.isNaN(twapPrice) || timestampMs <= 0L) {
            return;
        }
        long effectiveTsMs = twapTimestampMs > 0L ? twapTimestampMs : timestampMs;
        long priceCents = Math.round(twapPrice * 100.0);

        if (activeCandleStartMs > 0L && effectiveTsMs == activeCandleEndMs) {
            nextCandleStartMs = activeCandleEndMs;
            nextCandleTargetPriceCents = priceCents;
            return;
        }

        ensureActiveCandle(timestampMs);
        long offsetFromStartMs = effectiveTsMs - activeCandleStartMs;
        if (offsetFromStartMs == 0L
                || (targetPriceCents <= 0L && offsetFromStartMs > 0L && offsetFromStartMs <= MAX_TARGET_PRICE_OFFSET_MS)) {
            boolean newlyLocked = targetPriceCents <= 0L;
            targetPriceCents = priceCents;
            if (newlyLocked) {
                Log.infof("Market [%d, %d] target price locked from PolyBolt 60s TWAP: $%.2f (offsetMs=%d)",
                        activeCandleStartMs, activeCandleEndMs, priceCents / 100.0, offsetFromStartMs);
            }
        }
        if (windowSeconds == 60
                && effectiveTsMs > activeCandleStartMs
                && effectiveTsMs < activeCandleEndMs
                && effectiveTsMs >= lastLiveTwapEffectiveMs) {
            lastLiveTwapCents = priceCents;
            lastLiveTwapEffectiveMs = effectiveTsMs;
            lastLiveTwapArrivalMs = timestampMs;
            long tauMs = activeCandleEndMs - timestampMs;
            if (tauMs >= config.minTauMs() && tauMs <= config.maxTauMs() && lastChainlinkPriceCents > 0L) {
                OrderBookQuote quote = this.latestValidQuote;
                if (quote != null && (quote.timestampMs() <= 0L || timestampMs + 2_000L >= quote.timestampMs())) {
                    evaluateEntry(timestampMs, quote);
                }
            }
        }
    }

    @Override
    public void onOrderBookQuote(OrderBookQuote quote) {
        if (quote == null || !isQuoteTradable(quote)) {
            latestValidQuote = null;
            return;
        }
        latestValidQuote = quote;
        long nowMs = quote.timestampMs() > 0L ? quote.timestampMs() : lastChainlinkTickMs;
        if (nowMs > 0L) {
            ensureActiveCandle(nowMs);
            maybeRefreshBalanceBeforeActivation(nowMs);
            evaluateEntry(nowMs, quote);
        }
    }

    @Override
    public void onExecutionReport(ExecutionReport report) {
        if (report == null || report.clientOrderId() == null) {
            return;
        }
        if (pendingClientOrderId != null && !pendingClientOrderId.equals(report.clientOrderId())) {
            return;
        }
        String status = report.status();
        if ("FILLED".equalsIgnoreCase(status) || "PARTIALLY_FILLED".equalsIgnoreCase(status)) {
            int filledShares = report.executedSize() != null
                    ? (int) Math.round(report.executedSize().doubleValue())
                    : pendingRequestedShares;
            if (filledShares > 0) {
                int fillPriceMc = report.executedPrice() != null
                        ? (int) Math.round(report.executedPrice().doubleValue() * 1000.0)
                        : pendingLimitPriceMc;
                fillPriceMc = Math.clamp(fillPriceMc, 1, MAX_MC - 1);
                long unitCostMc = (long) fillPriceMc + takerFeeMc(fillPriceMc, config.takerFeeRateBps());
                if (pendingDirection == TradeDirection.UP) {
                    netSharesUp += filledShares;
                    maxFilledAskUpMc = Math.max(maxFilledAskUpMc, fillPriceMc);
                    totalCostWithFeeUpMcShares += unitCostMc * filledShares;
                } else if (pendingDirection == TradeDirection.DOWN) {
                    netSharesDown += filledShares;
                    maxFilledAskDownMc = Math.max(maxFilledAskDownMc, fillPriceMc);
                    totalCostWithFeeDownMcShares += unitCostMc * filledShares;
                }
                if (pendingDirection == TradeDirection.UP || pendingDirection == TradeDirection.DOWN) {
                    if (firstDirectionalSide == TradeDirection.NO_TRADE) {
                        firstDirectionalSide = pendingDirection;
                    }
                    if (lastDirectionalSide != TradeDirection.NO_TRADE && lastDirectionalSide != pendingDirection) {
                        directionalFlipCount++;
                    }
                    lastDirectionalSide = pendingDirection;
                }
                tradesInMarket++;
            }
            orderInFlight = false;
            pendingClientOrderId = null;
        } else if ("REJECTED".equalsIgnoreCase(status) || "CANCELLED".equalsIgnoreCase(status)) {
            orderInFlight = false;
            pendingClientOrderId = null;
        }
    }

    @Override
    public void onCandleResolution(TradeDirection actualOutcome) {
        orderInFlight = false;
        pendingClientOrderId = null;
    }

    private void maybeRefreshBalanceBeforeActivation(long nowMs) {
        if (balanceRefreshedForMarket || context == null || activeCandleEndMs <= 0L || nowMs <= 0L) {
            return;
        }
        long tauMs = activeCandleEndMs - nowMs;
        long preActivationTauMs = config.maxTauMs() + PRE_ACTIVATION_BALANCE_SYNC_LEAD_MS;
        if (tauMs <= preActivationTauMs && tauMs >= config.minTauMs()) {
            balanceRefreshedForMarket = true;
            context.refreshAccountBalance();
        }
    }

    /**
     * Evaluates whether a new batch Taker BUY order should be emitted at {@code nowMs}.
     *
     * @param nowMs current event timestamp in milliseconds UTC
     * @param quote current validated order book quote
     */
    private void evaluateEntry(long nowMs, OrderBookQuote quote) {
        if (context == null || activeCandleEndMs <= 0L || nowMs <= 0L || orderInFlight) {
            return;
        }

        long tauMs = activeCandleEndMs - nowMs;
        if (tauMs < config.minTauMs() || tauMs > config.maxTauMs()) {
            return;
        }

        if (targetPriceCents <= 0L) {
            if (!missingTargetPriceWarned) {
                missingTargetPriceWarned = true;
                Log.warnf("Skipping market [%d, %d]: target price is missing from PolyBolt 60s TWAP stream",
                        activeCandleStartMs, activeCandleEndMs);
            }
            return;
        }

        if (lastChainlinkPriceCents <= 0L) {
            return;
        }

        if (hasEmittedSignal && (nowMs - lastSignalMs) < config.cooldownMs()) {
            return;
        }

        // Extract and conservatively round UP/DOWN BBO prices to 10-mc ($0.01) ticks
        int bidUpMc = extractFloorTickMc(quote.bestBidUp());
        int askUpMc = extractCeilTickMc(quote.bestAskUp());
        boolean upQuoteValid = bidUpMc > 0 && askUpMc > bidUpMc && askUpMc <= MAX_MC
                && (askUpMc - bidUpMc) <= config.maxSpreadMc();

        int askDownMc;
        if (!Double.isNaN(quote.bestAskDown()) && quote.bestAskDown() > 0.0 && quote.bestAskDown() <= 1.0) {
            askDownMc = extractCeilTickMc(quote.bestAskDown());
        } else {
            askDownMc = (bidUpMc > 0 && bidUpMc < MAX_MC) ? (MAX_MC - bidUpMc) : 0;
        }
        int bidDownMc;
        if (!Double.isNaN(quote.bestBidDown()) && quote.bestBidDown() > 0.0 && quote.bestBidDown() < 1.0) {
            bidDownMc = extractFloorTickMc(quote.bestBidDown());
        } else {
            bidDownMc = (askUpMc > 0 && askUpMc < MAX_MC) ? (MAX_MC - askUpMc) : 0;
        }
        boolean downQuoteValid = bidDownMc > 0 && askDownMc > bidDownMc && askDownMc <= MAX_MC
                && (askDownMc - bidDownMc) <= config.maxSpreadMc();

        if (!upQuoteValid && !downQuoteValid) {
            return;
        }

        // 1. De-biased multi-exchange USD spot innovation anchored on Chainlink
        long clSpotCents = lastChainlinkPriceCents;
        long leadDeltaCents = computeDeBiasedSpotLeadCents(nowMs);
        long projectedSpotCents = clSpotCents + leadDeltaCents;

        // 2. Project final 60s Chainlink TWAP anchored on live PolyBolt 60s TWAP rolling-shift when fresh
        long settlementWindowStartMs = activeCandleEndMs - TWAP_WINDOW_MS;
        long lockedMeanCents = (tauMs <= TWAP_WINDOW_MS)
                ? chainlinkTwapBuffer.meanCentsInWindow(settlementWindowStartMs, nowMs)
                : 0L;

        long effectiveLockedCents;
        long projectedTwapCents;
        if (tauMs >= TWAP_WINDOW_MS) {
            effectiveLockedCents = projectedSpotCents;
            projectedTwapCents = projectedSpotCents;
        } else {
            long elapsedMs = TWAP_WINDOW_MS - tauMs;
            long rampMs = leadDeltaCents != 0L ? Math.min(tauMs, config.spotLeadLagMs()) : 0L;
            long futureIntegralFromNow = rampMs * ((clSpotCents + projectedSpotCents) / 2L)
                    + (tauMs - rampMs) * projectedSpotCents;

            boolean liveTwapFresh = lastLiveTwapCents > 0L
                    && lastLiveTwapEffectiveMs > settlementWindowStartMs
                    && lastLiveTwapEffectiveMs <= nowMs
                    && nowMs >= lastLiveTwapArrivalMs
                    && (nowMs - lastLiveTwapArrivalMs) <= LIVE_TWAP_MAX_AGE_MS
                    && (nowMs - lastLiveTwapEffectiveMs) <= LIVE_TWAP_MAX_EFFECTIVE_LAG_MS;
            long remFromTwapMs = liveTwapFresh ? (activeCandleEndMs - lastLiveTwapEffectiveMs) : 0L;
            long evictMeanCents = 0L;
            if (liveTwapFresh && remFromTwapMs > 0L && remFromTwapMs < TWAP_WINDOW_MS) {
                evictMeanCents = chainlinkTwapBuffer.meanCentsInWindow(
                        lastLiveTwapEffectiveMs - TWAP_WINDOW_MS, settlementWindowStartMs);
                if (evictMeanCents <= 0L && remFromTwapMs <= 2_500L) {
                    evictMeanCents = chainlinkTwapBuffer.priceAtOrBefore(settlementWindowStartMs, 3_000L);
                }
            }

            if (evictMeanCents > 0L) {
                long gapMs = nowMs - lastLiveTwapEffectiveMs;
                long futureIntegralFromTwap = gapMs * clSpotCents + futureIntegralFromNow;
                projectedTwapCents = lastLiveTwapCents
                        + (futureIntegralFromTwap - remFromTwapMs * evictMeanCents) / TWAP_WINDOW_MS;
                long elapsedFromTwapMs = TWAP_WINDOW_MS - remFromTwapMs;
                if (elapsedFromTwapMs >= 12_000L && elapsedMs > 0L) {
                    long lockedIntegralToNow = TWAP_WINDOW_MS * lastLiveTwapCents
                            - remFromTwapMs * evictMeanCents + gapMs * clSpotCents;
                    effectiveLockedCents = lockedIntegralToNow / elapsedMs;
                } else {
                    effectiveLockedCents = lockedMeanCents > 0L ? lockedMeanCents : clSpotCents;
                }
            } else {
                effectiveLockedCents = lockedMeanCents > 0L ? lockedMeanCents : clSpotCents;
                projectedTwapCents = (elapsedMs * effectiveLockedCents + futureIntegralFromNow) / TWAP_WINDOW_MS;
            }
        }

        // 3. Remaining 60s TWAP volatility sigma_rem(tau) with jump-shock and sizing-compression floors
        long baseSigma1sCents = spotVolatilityBuffer.sigma1sCents(config);
        long shockFloorCents = Math.abs(leadDeltaCents) / 6L;
        long sigma1sCents = Math.clamp(Math.max(baseSigma1sCents, shockFloorCents),
                config.minSigma1sCents(), config.maxSigma1sCents());
        int tauSec = (int) Math.clamp((tauMs + 500L) / 1000L, 0L, 61L);
        long weightBps = TWAP_REM_WEIGHT_BPS[tauSec];
        long twapPhaseFloorCents = Math.max(MIN_SIGMA_REM_CENTS, sigma1sCents / 3L);
        long sigmaRemCents = Math.max(twapPhaseFloorCents, (sigma1sCents * weightBps) / 10_000L);

        long sizingSigma1sCents = Math.max(sigma1sCents, SIZING_MIN_SIGMA_1S_CENTS);
        long sizingPhaseFloorCents = Math.max(MIN_SIGMA_REM_CENTS, sizingSigma1sCents / 3L);
        long sizingSigmaRemCents = Math.max(sizingPhaseFloorCents, (sizingSigma1sCents * weightBps) / 10_000L);

        long zMidCenti = (100L * (projectedTwapCents - targetPriceCents)) / sigmaRemCents;
        this.lastModelProbUpMc = modelProbCdfMc(zMidCenti, tauMs);
        long spotDelta4sCents = computeRecentSpotDeltaCents(nowMs, clSpotCents);

        // 4. Evaluate directional edge on UP and DOWN incorporating banked-TWAP surplus/deficit state
        long distProjUp = projectedTwapCents - targetPriceCents;
        long distSpotUp = projectedSpotCents - targetPriceCents;
        long clDistUp = clSpotCents - targetPriceCents;
        long lockedDistUp = (tauMs < DEFICIT_CHASE_MAX_TAU_MS)
                ? (effectiveLockedCents - targetPriceCents)
                : distSpotUp;

        int dirEdgeUpMc = upQuoteValid
                ? evaluateDirectionalSideEdge(
                        TradeDirection.UP, askUpMc, distProjUp, distSpotUp, clDistUp, lockedDistUp,
                        spotDelta4sCents, tauMs, sigmaRemCents, sizingSigmaRemCents,
                        netSharesUp, netSharesDown, maxFilledAskUpMc)
                : Integer.MIN_VALUE;
        int dirEdgeDownMc = downQuoteValid
                ? evaluateDirectionalSideEdge(
                        TradeDirection.DOWN, askDownMc, -distProjUp, -distSpotUp, -clDistUp, -lockedDistUp,
                        -spotDelta4sCents, tauMs, sigmaRemCents, sizingSigmaRemCents,
                        netSharesDown, netSharesUp, maxFilledAskDownMc)
                : Integer.MIN_VALUE;

        TradeDirection chosenDirection = TradeDirection.NO_TRADE;
        int chosenAskMc = 0;
        int chosenEdgeMc = Integer.MIN_VALUE;

        if (dirEdgeUpMc >= config.minEdgeMc() && dirEdgeUpMc >= dirEdgeDownMc) {
            chosenDirection = TradeDirection.UP;
            chosenAskMc = askUpMc;
            chosenEdgeMc = dirEdgeUpMc;
        } else if (dirEdgeDownMc >= config.minEdgeMc()) {
            chosenDirection = TradeDirection.DOWN;
            chosenAskMc = askDownMc;
            chosenEdgeMc = dirEdgeDownMc;
        }

        if (chosenDirection == TradeDirection.NO_TRADE) {
            if (Log.isTraceEnabled()) {
                Log.tracef("Strategy evaluateEntry NO_TRADE: tauMs=%d, spotCents=%d, projSpotCents=%d, projTwapCents=%d, targetCents=%d, sigmaRemCents=%d, askUpMc=%d, askDownMc=%d",
                        tauMs, clSpotCents, projectedSpotCents, projectedTwapCents, targetPriceCents, sigmaRemCents, askUpMc, askDownMc);
            }
            return;
        }

        int sameShares = (chosenDirection == TradeDirection.UP) ? netSharesUp : netSharesDown;
        int oppShares = (chosenDirection == TradeDirection.UP) ? netSharesDown : netSharesUp;
        int peakSideAskMc = (chosenDirection == TradeDirection.UP) ? maxFilledAskUpMc : maxFilledAskDownMc;
        long favorableSpotDeltaCents = (chosenDirection == TradeDirection.UP) ? spotDelta4sCents : -spotDelta4sCents;

        long chosenDistProjCents = (chosenDirection == TradeDirection.UP) ? distProjUp : -distProjUp;
        long chosenDistSpotCents = (chosenDirection == TradeDirection.UP) ? distSpotUp : -distSpotUp;
        long chosenClDistCents = (chosenDirection == TradeDirection.UP) ? clDistUp : -clDistUp;
        long chosenLockedDistCents = (chosenDirection == TradeDirection.UP) ? lockedDistUp : -lockedDistUp;
        boolean isDeficitChasing = tauMs < DEFICIT_CHASE_MAX_TAU_MS
                && chosenLockedDistCents < DEFICIT_CHASE_TRIGGER_CENTS;

        long chosenEffDistCents = computeEffectiveDistCents(
                chosenDistProjCents, chosenDistSpotCents, chosenLockedDistCents, tauMs);
        long chosenSizingZCenti = (100L * chosenEffDistCents) / sizingSigmaRemCents;

        boolean highConvictionSweep = !isDeficitChasing
                && chosenAskMc < 820
                && chosenSizingZCenti >= 220L
                && chosenClDistCents > 0L
                && chosenDistSpotCents > 0L
                && favorableSpotDeltaCents >= -100L;
        if (tauMs > EARLY_PHASE_MIN_TAU_MS
                && !highConvictionSweep
                && hasEmittedSignal
                && (nowMs - lastSignalMs) < EARLY_PHASE_MIN_COOLDOWN_MS) {
            return;
        }

        // 5. Tau-paced Binary Fractional-Kelly budget with multi-step Kelly tail-drag inventory accounting
        int askCents = Math.max(1, (chosenAskMc + 5) / 10);
        int feeMc = takerFeeMc(chosenAskMc, config.takerFeeRateBps());
        int unitCostMc = Math.min(MAX_MC - 10, chosenAskMc + feeMc);
        int pSideMc = Math.min(999, unitCostMc + chosenEdgeMc);

        int dropFromPeakMc = sameShares > 0 ? Math.max(0, peakSideAskMc - chosenAskMc) : 0;
        int sizingEdgeMc = chosenEdgeMc;
        if (sameShares > 0
                && dropFromPeakMc >= SIZING_CAP_DROP_FROM_PEAK_MC
                && (chosenSizingZCenti < 220L || favorableSpotDeltaCents < -50L)) {
            sizingEdgeMc = Math.min(chosenEdgeMc, SIZING_CAPPED_EDGE_MC);
        }

        int minPayoffDenomMc = tauMs > 35_000L
                ? 320
                : (tauMs <= 12_000L ? 130 : (int) (130L + (190L * (tauMs - 12_000L)) / 23_000L));
        int kellyPayoffDenomMc = Math.max(minPayoffDenomMc, MAX_MC - unitCostMc);
        long kellyPermille = Math.min(1_450L, (1_000L * sizingEdgeMc) / kellyPayoffDenomMc);
        long fullRiskBudgetCents = (long) config.baseRiskUsdCents()
                + ((long) config.kellyRiskPerEdgeMcCents() * 100L * kellyPermille) / 1_000L;

        long lockedMs = Math.clamp(TWAP_WINDOW_MS - tauMs, 0L, 40_000L);
        long tauPaceBps = 2_800L + (7_200L * lockedMs) / 40_000L;
        if (highConvictionSweep) {
            long convictionPaceBps = chosenSizingZCenti >= 300L ? 8_800L : 7_200L;
            tauPaceBps = Math.max(tauPaceBps, convictionPaceBps);
        }
        if (!isDeficitChasing
                && chosenLockedDistCents >= 400L
                && tauMs <= LATE_LOCKIN_MAX_TAU_MS
                && chosenSizingZCenti >= 260L
                && favorableSpotDeltaCents >= -100L) {
            tauPaceBps = 12_500L;
        }
        if (isDeficitChasing) {
            tauPaceBps = Math.min(tauPaceBps, 5_500L);
        }
        long pacedRiskBudgetCents = (fullRiskBudgetCents * tauPaceBps) / 10_000L;

        int marginalCostCents = Math.max(25, (unitCostMc + 5) / 10);
        long sideTotalCostMcShares = (chosenDirection == TradeDirection.UP)
                ? totalCostWithFeeUpMcShares
                : totalCostWithFeeDownMcShares;
        long spentSideCents = sideTotalCostMcShares / 10L;
        long effectiveRiskUsedCents = spentSideCents;
        if (!isDeficitChasing
                && tauMs <= 42_000L
                && chosenLockedDistCents >= 400L
                && chosenSizingZCenti >= 190L
                && favorableSpotDeltaCents >= -100L
                && sameShares > 0) {
            int minTailProbMc = tauMs <= 15_000L
                    ? 32
                    : (int) (32L + (108L * (tauMs - 15_000L)) / 27_000L);
            int effectiveTailProbMc = Math.max(MAX_MC - pSideMc, minTailProbMc);
            long markExposureCents = Math.max(spentSideCents, (long) sameShares * marginalCostCents);
            long kellyTailDragCents = (markExposureCents * effectiveTailProbMc) / kellyPayoffDenomMc;
            effectiveRiskUsedCents = Math.min(spentSideCents, Math.max(spentSideCents / 6L, kellyTailDragCents));
        }
        long remainingRiskCents = Math.max(0L, pacedRiskBudgetCents - effectiveRiskUsedCents);

        int unhedgedOppShares = Math.max(0, oppShares - sameShares);
        int additionalAllowedShares = unhedgedOppShares + (int) (remainingRiskCents / marginalCostCents);

        int tauShareCeiling = computeTauShareCeiling(tauMs, config.maxInventorySharesPerSide(), highConvictionSweep);
        boolean isFlippedLeg = firstDirectionalSide != TradeDirection.NO_TRADE
                && chosenDirection != firstDirectionalSide;
        int legCeiling = tauShareCeiling;
        if (oppShares > 0 || isFlippedLeg) {
            boolean decisiveReversalSurplus = chosenDistProjCents >= 750L && chosenSizingZCenti >= 260L;
            if (!decisiveReversalSurplus) {
                legCeiling = Math.min(legCeiling, oppShares + 140);
            } else if (chosenSizingZCenti < 300L) {
                legCeiling = Math.min(legCeiling, Math.max(FLIP_MAX_SHARES, oppShares + 400));
            }
        }
        if (isDeficitChasing) {
            legCeiling = Math.min(legCeiling, Math.max(oppShares, DEFICIT_CHASE_MAX_SHARES));
        }
        int remainingCap = Math.max(0, Math.min(legCeiling - sameShares, additionalAllowedShares));
        if (remainingCap < config.minOrderShares()) {
            return;
        }

        int qRaw = computeMicroClipShares(quote, chosenDirection, askCents, sizingEdgeMc, tauMs, highConvictionSweep);
        int qFinal = Math.min(qRaw, remainingCap);
        if (qFinal < config.minOrderShares()) {
            return;
        }

        int limitPriceMc = computeEdgePreservingLimitPriceMc(
                chosenDirection,
                chosenAskMc,
                chosenDistProjCents,
                chosenDistSpotCents,
                chosenClDistCents,
                chosenLockedDistCents,
                favorableSpotDeltaCents,
                tauMs,
                sigmaRemCents,
                sizingSigmaRemCents,
                sameShares,
                oppShares,
                peakSideAskMc
        );

        if (Log.isDebugEnabled()) {
            Log.debugf("Strategy signal %s: tauMs=%d, askMc=%d, limitPriceMc=%d, edgeMc=%d, shares=%d (netUp=%d, netDown=%d, leadDelta=%d)",
                    chosenDirection, tauMs, chosenAskMc, limitPriceMc, chosenEdgeMc, qFinal, netSharesUp, netSharesDown, leadDeltaCents);
        }

        submitBatchOrder(nowMs, chosenDirection, limitPriceMc, qFinal);
    }

    private long computeDeBiasedSpotLeadCents(long nowMs) {
        if (config.spotLeadBetaBps() <= 0 || config.spotLeadLagMs() <= 0L) {
            return 0L;
        }
        if (lastCalcSpotCents <= 0L
                || lastCalcSpotMs < lastChainlinkTickMs
                || (nowMs - lastCalcSpotMs) > 2_500L
                || (nowMs - lastChainlinkTickMs) > 5_000L) {
            return 0L;
        }
        long anchorTargetMs = lastChainlinkTickMs - config.spotLeadLagMs();
        long anchorCalcCents = calcSpotBuffer.priceAtOrBefore(anchorTargetMs, 5_000L);
        if (anchorCalcCents <= 0L) {
            return 0L;
        }
        long rawInnovationCents = lastCalcSpotCents - anchorCalcCents;
        long absRawCents = Math.abs(rawInnovationCents);
        if (absRawCents <= SPOT_LEAD_DEADBAND_CENTS) {
            return 0L;
        }
        long excessCents = Math.min(MAX_SPOT_INNOVATION_CENTS, absRawCents - SPOT_LEAD_DEADBAND_CENTS);
        long signedExcessCents = rawInnovationCents > 0L ? excessCents : -excessCents;
        return (signedExcessCents * config.spotLeadBetaBps()) / 10_000L;
    }

    private long computeRecentSpotDeltaCents(long nowMs, long clSpotCents) {
        if (lastCalcSpotCents > 0L && (nowMs - lastCalcSpotMs) <= 2_500L) {
            long prevCalc = calcSpotBuffer.priceAtOrBefore(nowMs - SPOT_MOMENTUM_WINDOW_MS, 5_000L);
            if (prevCalc > 0L) {
                return lastCalcSpotCents - prevCalc;
            }
        }
        long prevCl = chainlinkTwapBuffer.priceAtOrBefore(nowMs - SPOT_MOMENTUM_WINDOW_MS, 6_000L);
        return prevCl > 0L ? (clSpotCents - prevCl) : 0L;
    }

    private static long computeEffectiveDistCents(
            long distProjCents,
            long distSpotCents,
            long lockedDistSideCents,
            long tauMs) {
        if (distProjCents <= 0L) {
            return 0L;
        }
        long effectiveDistCents;
        if (distSpotCents > 0L) {
            long conservativeDist = Math.min(distProjCents, distSpotCents);
            if (tauMs > LATE_LOCKIN_MAX_TAU_MS) {
                effectiveDistCents = conservativeDist;
            } else {
                effectiveDistCents = (tauMs * conservativeDist + (LATE_LOCKIN_MAX_TAU_MS - tauMs) * distProjCents)
                        / LATE_LOCKIN_MAX_TAU_MS;
            }
        } else {
            if (tauMs > LATE_LOCKIN_MAX_TAU_MS
                    || lockedDistSideCents < LATE_LOCKIN_MIN_BANKED_DIST_CENTS
                    || distProjCents < LATE_LOCKIN_MIN_PROJ_DIST_CENTS) {
                return 0L;
            }
            effectiveDistCents = distProjCents - (-distSpotCents) / 6L;
            if (effectiveDistCents < LATE_LOCKIN_MIN_NET_DIST_CENTS) {
                return 0L;
            }
        }
        long minNoiseDistCents = (EARLY_NOISE_DIST_CENTS * Math.min(tauMs, TWAP_WINDOW_MS)) / TWAP_WINDOW_MS;
        return effectiveDistCents >= minNoiseDistCents ? effectiveDistCents : 0L;
    }

    private int evaluateDirectionalSideEdge(
            TradeDirection direction,
            int askMc,
            long distProjCents,
            long distSpotCents,
            long clDistCents,
            long lockedDistSideCents,
            long favorableSpotDeltaCents,
            long tauMs,
            long sigmaRemCents,
            long sizingSigmaRemCents,
            int sameShares,
            int oppShares,
            int peakSideAskMc) {
        if (askMc < config.minEntryPriceMc() || askMc > config.maxEntryPriceMc()) {
            return Integer.MIN_VALUE;
        }
        long effectiveDistCents = computeEffectiveDistCents(
                distProjCents, distSpotCents, lockedDistSideCents, tauMs);
        if (effectiveDistCents <= 0L) {
            return Integer.MIN_VALUE;
        }
        if (tauMs < DEFICIT_CHASE_MAX_TAU_MS && lockedDistSideCents < DEFICIT_CHASE_TRIGGER_CENTS) {
            long requiredProjCents = Math.max(DEFICIT_CHASE_MIN_PROJ_CENTS, (-lockedDistSideCents) / 5L);
            if (distProjCents < requiredProjCents) {
                return Integer.MIN_VALUE;
            }
        }

        // Block toxic averaging-down when CLOB drops sharply below our peak fill or BTC actively moves against inventory
        if (sameShares > 0) {
            if ((peakSideAskMc - askMc) >= TOXIC_DROP_FROM_PEAK_MC
                    && favorableSpotDeltaCents < ADVERSE_SPOT_DELTA_CENTS) {
                return Integer.MIN_VALUE;
            }
            if (tauMs > 15_000L && favorableSpotDeltaCents <= ACTIVE_INVENTORY_ADVERSE_DELTA_CENTS) {
                return Integer.MIN_VALUE;
            }
        }
        if (tauMs > 20_000L && favorableSpotDeltaCents < SEVERE_ADVERSE_IMPULSE_CENTS) {
            return Integer.MIN_VALUE;
        }

        long zSideCenti = (100L * effectiveDistCents) / sigmaRemCents;

        // Market-consistency conviction gate: protect against CLOB adverse selection on deep underdogs & late coinflips
        if (askMc < DEEP_UNDERDOG_ASK_MC
                && (clDistCents <= 0L || zSideCenti < DEEP_UNDERDOG_MIN_Z_CENTI || favorableSpotDeltaCents < -50L)) {
            return Integer.MIN_VALUE;
        }
        if (tauMs <= LATE_LOCKIN_MAX_TAU_MS && askMc < 540 && zSideCenti < 220L) {
            return Integer.MIN_VALUE;
        }

        // Decisive directional flip & anti-whipsaw discipline using compression-resistant sizingSigmaRemCents
        boolean isFlippedLeg = firstDirectionalSide != TradeDirection.NO_TRADE
                && direction != firstDirectionalSide;
        boolean switchingBackAfterFlip = directionalFlipCount > 0 && !isFlippedLeg;
        if (oppShares > 0 || isFlippedLeg || switchingBackAfterFlip) {
            if (clDistCents <= 0L || distSpotCents <= 0L || favorableSpotDeltaCents < FLIP_MAX_ADVERSE_DELTA_CENTS) {
                return Integer.MIN_VALUE;
            }
            long flipZCenti = (100L * effectiveDistCents) / sizingSigmaRemCents;
            long requiredZ = switchingBackAfterFlip ? DOUBLE_FLIP_MIN_Z_CENTI : FLIP_MIN_Z_CENTI;
            if (flipZCenti < requiredZ) {
                return Integer.MIN_VALUE;
            }
            int minFlipAskMc;
            if (switchingBackAfterFlip) {
                minFlipAskMc = DOUBLE_FLIP_MIN_ASK_MC;
            } else if (flipZCenti >= HIGH_CONVICTION_FLIP_MIN_Z_CENTI
                    && (lockedDistSideCents > 0L || flipZCenti >= 320L)) {
                minFlipAskMc = HIGH_CONVICTION_FLIP_MIN_ASK_MC;
            } else {
                minFlipAskMc = FLIP_MIN_ASK_MC;
            }
            if (askMc < minFlipAskMc) {
                return Integer.MIN_VALUE;
            }
        }

        int pModelMc = modelProbCdfMc(zSideCenti, tauMs);
        if (pModelMc < config.minModelProbMc()) {
            return Integer.MIN_VALUE;
        }
        int feeMc = takerFeeMc(askMc, config.takerFeeRateBps());
        int netEdgeMc = pModelMc - askMc - feeMc;
        if (askMc >= 850 && askMc <= 910 && tauMs > 25_000L && netEdgeMc < config.minEdgeMc() + 8) {
            return Integer.MIN_VALUE;
        }

        if (oppShares > 0 || isFlippedLeg || switchingBackAfterFlip) {
            int extraEdgeMc = switchingBackAfterFlip ? DOUBLE_FLIP_EXTRA_EDGE_MC : FLIP_EXTRA_EDGE_MC;
            if (netEdgeMc < config.minEdgeMc() + extraEdgeMc) {
                return Integer.MIN_VALUE;
            }
        }
        return netEdgeMc;
    }

    private static int computeTauShareCeiling(long tauMs, int maxInventoryShares, boolean highConvictionSweep) {
        if (tauMs <= 15_000L) {
            return maxInventoryShares;
        }
        int earlyBps = highConvictionSweep ? 1_900 : 850;
        int midBps = highConvictionSweep ? 3_800 : 2_200;
        int earlyFloor = Math.max(60, (maxInventoryShares * earlyBps) / 10_000);
        int midMilestone = Math.max(earlyFloor, (maxInventoryShares * midBps) / 10_000);
        if (tauMs >= TWAP_WINDOW_MS) {
            return earlyFloor;
        }
        if (tauMs >= 45_000L) {
            long lockedEarlyMs = TWAP_WINDOW_MS - tauMs;
            return earlyFloor + (int) (((long) (midMilestone - earlyFloor) * lockedEarlyMs) / 15_000L);
        }
        long lockedLateMs = 45_000L - tauMs;
        return midMilestone + (int) (((long) (maxInventoryShares - midMilestone) * lockedLateMs) / 30_000L);
    }

    private int computeMicroClipShares(
            OrderBookQuote quote,
            TradeDirection direction,
            int askCents,
            int edgeMc,
            long tauMs,
            boolean highConvictionSweep) {
        int excessEdgeMc = Math.max(0, edgeMc - config.minEdgeMc());
        long clipUsdCents = (long) config.baseClipUsdCents()
                + (long) config.edgeScalingFactor() * excessEdgeMc;
        int clipDenomCents = Math.max(38, askCents);
        int tauOrderCap = (tauMs > EARLY_PHASE_MIN_TAU_MS && !highConvictionSweep)
                ? Math.min(config.maxOrderShares(), EARLY_PHASE_MAX_CLIP_SHARES)
                : config.maxOrderShares();
        int rawShares = (int) Math.clamp(
                clipUsdCents / clipDenomCents,
                config.minOrderShares(),
                Math.max(config.minOrderShares(), tauOrderCap));

        double sideDepth = (direction == TradeDirection.UP) ? quote.depth1cUp() : quote.depth1cDown();
        if (!Double.isNaN(sideDepth) && sideDepth > 0.0) {
            int depthCap = Math.max(config.minOrderShares() * 2, (int) Math.floor(sideDepth * 1.60));
            rawShares = Math.min(rawShares, depthCap);
        }
        return rawShares;
    }

    /**
     * Computes the highest 10-mc ($0.01) tick limit price {@code >= askMc} that does not exceed
     * {@code askMc + config.maxSlippageMc()} or {@code config.maxEntryPriceMc()} and still preserves
     * at least {@code config.minEdgeMc()} net edge after the parabolic taker fee.
     */
    private int computeEdgePreservingLimitPriceMc(
            TradeDirection direction,
            int askMc,
            long distProjCents,
            long distSpotCents,
            long clDistCents,
            long lockedDistSideCents,
            long favorableSpotDeltaCents,
            long tauMs,
            long sigmaRemCents,
            long sizingSigmaRemCents,
            int sameShares,
            int oppShares,
            int peakSideAskMc) {
        int maxAllowedMc = Math.min(config.maxEntryPriceMc(), askMc + config.maxSlippageMc());
        int limitMc = askMc;
        for (int candidateMc = askMc + TICK_STEP_MC; candidateMc <= maxAllowedMc; candidateMc += TICK_STEP_MC) {
            int candidateEdgeMc = evaluateDirectionalSideEdge(
                    direction,
                    candidateMc,
                    distProjCents,
                    distSpotCents,
                    clDistCents,
                    lockedDistSideCents,
                    favorableSpotDeltaCents,
                    tauMs,
                    sigmaRemCents,
                    sizingSigmaRemCents,
                    sameShares,
                    oppShares,
                    peakSideAskMc
            );
            if (candidateEdgeMc < config.minEdgeMc()) {
                break;
            }
            limitMc = candidateMc;
        }
        return limitMc;
    }

    private void submitBatchOrder(long nowMs, TradeDirection direction, int limitPriceMc, int shares) {
        String clientOrderId = "up-v2-" + (activeCandleStartMs / 1000L) + "-" + (++orderSequence);
        String token = (direction == TradeDirection.UP) ? config.upToken() : config.downToken();
        BigDecimal price = BigDecimal.valueOf(limitPriceMc, 3);
        BigDecimal size = BigDecimal.valueOf(shares);

        this.orderInFlight = true;
        this.lastSignalMs = nowMs;
        this.hasEmittedSignal = true;
        this.pendingDirection = direction;
        this.pendingRequestedShares = shares;
        this.pendingLimitPriceMc = limitPriceMc;
        this.pendingClientOrderId = clientOrderId;

        OrderCommand command = new OrderCommand(
                clientOrderId,
                config.defaultMarketId(),
                token,
                Timeframe.FIFTEEN_MINUTES,
                "BUY",
                price,
                size
        );
        context.getExecutionRouter().submitOrder(command);
    }

    private static int extractFloorTickMc(double price) {
        if (Double.isNaN(price) || price <= 0.0 || price >= 1.0) {
            return 0;
        }
        int rawMc = (int) Math.round(price * 1000.0);
        if (rawMc <= 0 || rawMc >= MAX_MC) {
            return 0;
        }
        return fromMcFloorTick(rawMc);
    }

    private static int extractCeilTickMc(double price) {
        if (Double.isNaN(price) || price <= 0.0 || price > 1.0) {
            return 0;
        }
        int rawMc = (int) Math.round(price * 1000.0);
        if (rawMc <= 0 || rawMc > MAX_MC) {
            return 0;
        }
        return fromMcCeilTick(rawMc);
    }

    private static boolean isQuoteTradable(OrderBookQuote quote) {
        double bidUp = quote.bestBidUp();
        double askUp = quote.bestAskUp();
        boolean upTradable = !Double.isNaN(bidUp)
                && !Double.isNaN(askUp)
                && bidUp > 0.0
                && askUp > 0.0
                && bidUp < askUp
                && askUp < 1.0;
        if (upTradable) {
            return true;
        }
        double bidDown = quote.bestBidDown();
        double askDown = quote.bestAskDown();
        return !Double.isNaN(bidDown)
                && !Double.isNaN(askDown)
                && bidDown > 0.0
                && askDown > 0.0
                && bidDown < askDown
                && askDown < 1.0;
    }

    /**
     * Returns the explicit target price ({@code priceToBeat}) in USD cents determined from the
     * PolyBolt 60-second TWAP stream at market start.
     *
     * @return explicit target price in USD cents, or {@code 0} if missing
     */
    public long getTargetPriceCents() {
        return targetPriceCents;
    }

    /**
     * Returns whether the active market was skipped and warned due to a missing target price
     * from the PolyBolt 60-second TWAP stream.
     */
    public boolean isMissingTargetPriceWarned() {
        return missingTargetPriceWarned;
    }

    /**
     * Returns the last observed PolyBolt {@code price.crypto} price in USD cents.
     */
    public long getLastChainlinkPriceCents() {
        return lastChainlinkPriceCents;
    }

    /**
     * Returns the accumulated UP shares in the active 15m market.
     */
    public int getNetSharesUp() {
        return netSharesUp;
    }

    /**
     * Returns the accumulated DOWN shares in the active 15m market.
     */
    public int getNetSharesDown() {
        return netSharesDown;
    }

    /**
     * Returns the number of filled batch trades in the active 15m market.
     */
    public int getTradesInMarket() {
        return tradesInMarket;
    }

    /**
     * Returns the most recently evaluated UP model probability in millicents ({@code 2..998}).
     */
    public int getLastModelProbUpMc() {
        return lastModelProbUpMc;
    }

    /**
     * Returns the strategy configuration bean.
     */
    public UtterParameciumStrategyConfig getConfig() {
        return config;
    }

    /**
     * Pre-allocated circular buffer storing {@code (timestampMs, priceCents)} Chainlink samples
     * across a 130-second retention window for 60s TWAP rolling-shift and settlement projections.
     */
    static final class ChainlinkTwapBuffer {
        private static final int CAPACITY = 256;
        private static final int MASK = CAPACITY - 1;

        private final long[] timestampsMs = new long[CAPACITY];
        private final long[] pricesCents = new long[CAPACITY];
        private int head = 0;
        private int len = 0;

        void clear() {
            head = 0;
            len = 0;
        }

        void evictOlderThan(long minTimestampMs) {
            while (len > 0 && timestampsMs[head] < minTimestampMs) {
                head = (head + 1) & MASK;
                len--;
            }
        }

        void pushAndEvict(long timestampMs, long priceCents, long minTimestampMs) {
            evictOlderThan(minTimestampMs);
            if (len == CAPACITY) {
                head = (head + 1) & MASK;
                len--;
            }
            int tail = (head + len) & MASK;
            timestampsMs[tail] = timestampMs;
            pricesCents[tail] = priceCents;
            len++;
        }

        long meanCentsInWindow(long windowStartMs, long windowEndMs) {
            if (len == 0 || windowEndMs <= windowStartMs) {
                return 0L;
            }
            long elapsedMs = windowEndMs - windowStartMs;
            int minRequiredSamples = (int) Math.max(1L, elapsedMs / 2_500L);
            long sum = 0L;
            int count = 0;
            for (int i = 0; i < len; i++) {
                int idx = (head + i) & MASK;
                long ts = timestampsMs[idx];
                if (ts >= windowStartMs && ts <= windowEndMs) {
                    sum += pricesCents[idx];
                    count++;
                }
            }
            return count >= minRequiredSamples ? (sum / count) : 0L;
        }

        long priceAtOrBefore(long targetTimestampMs, long maxStalenessMs) {
            for (int i = len - 1; i >= 0; i--) {
                int idx = (head + i) & MASK;
                long ts = timestampsMs[idx];
                if (ts <= targetTimestampMs) {
                    return (targetTimestampMs - ts) <= maxStalenessMs ? pricesCents[idx] : 0L;
                }
            }
            return 0L;
        }
    }

    /**
     * Pre-allocated circular buffer storing coalesced (50ms resolution) direct multi-exchange USD spot
     * prices ({@code onCalculatedSpotTwap}) for {@code O(1)} lagged innovation lookups without allocations.
     */
    static final class CalculatedSpotRingBuffer {
        private static final int CAPACITY = 512;
        private static final int MASK = CAPACITY - 1;
        private static final long COALESCE_WINDOW_MS = 50L;

        private final long[] timestampsMs = new long[CAPACITY];
        private final long[] pricesCents = new long[CAPACITY];
        private int head = 0;
        private int len = 0;

        void clear() {
            head = 0;
            len = 0;
        }

        void record(long timestampMs, long spotCents) {
            if (len > 0) {
                int lastIdx = (head + len - 1) & MASK;
                long lastTs = timestampsMs[lastIdx];
                if (timestampMs < lastTs) {
                    return;
                }
                if (timestampMs - lastTs < COALESCE_WINDOW_MS) {
                    timestampsMs[lastIdx] = timestampMs;
                    pricesCents[lastIdx] = spotCents;
                    return;
                }
            }
            if (len == CAPACITY) {
                head = (head + 1) & MASK;
                len--;
            }
            int tail = (head + len) & MASK;
            timestampsMs[tail] = timestampMs;
            pricesCents[tail] = spotCents;
            len++;
        }

        long priceAtOrBefore(long targetTimestampMs, long maxStalenessMs) {
            for (int i = len - 1; i >= 0; i--) {
                int idx = (head + i) & MASK;
                long ts = timestampsMs[idx];
                if (ts <= targetTimestampMs) {
                    return (targetTimestampMs - ts) <= maxStalenessMs ? pricesCents[idx] : 0L;
                }
            }
            return 0L;
        }
    }

    /**
     * Pre-allocated circular buffer tracking 1-second absolute BTC price differences
     * {@code |Delta S_cents|} to estimate {@code sigma_1s_cents} via Gaussian-consistent
     * Mean Absolute Deviation ({@code MAD * 1.253}) without heap allocations.
     */
    static final class SpotVolatilityBuffer {
        private static final int CAPACITY = 64;
        private static final int MASK = CAPACITY - 1;
        private static final long MAD_TO_SIGMA_NUM = 1_253L;
        private static final long MAD_TO_SIGMA_DEN = 1_000L;

        private final long[] absDiffsCents = new long[CAPACITY];
        private int head = 0;
        private int len = 0;
        private long runningSumAbsCents = 0L;
        private long lastSamplePriceCents = 0L;
        private long lastSampleMs = 0L;

        void clear() {
            head = 0;
            len = 0;
            runningSumAbsCents = 0L;
            lastSamplePriceCents = 0L;
            lastSampleMs = 0L;
        }

        void onTick(long timestampMs, long priceCents) {
            if (lastSamplePriceCents <= 0L) {
                lastSamplePriceCents = priceCents;
                lastSampleMs = timestampMs;
                return;
            }
            long dtMs = timestampMs - lastSampleMs;
            if (dtMs < 800L) {
                return;
            }
            if (dtMs <= 2_500L) {
                long absDiff = Math.abs(priceCents - lastSamplePriceCents);
                if (len == CAPACITY) {
                    runningSumAbsCents -= absDiffsCents[head];
                    head = (head + 1) & MASK;
                    len--;
                }
                int tail = (head + len) & MASK;
                absDiffsCents[tail] = absDiff;
                runningSumAbsCents += absDiff;
                len++;
            }
            lastSamplePriceCents = priceCents;
            lastSampleMs = timestampMs;
        }

        long sigma1sCents(UtterParameciumStrategyConfig cfg) {
            if (len < 5) {
                return cfg.defaultSigma1sCents();
            }
            long rawSigma = (runningSumAbsCents * MAD_TO_SIGMA_NUM) / (len * MAD_TO_SIGMA_DEN);
            return Math.clamp(rawSigma, cfg.minSigma1sCents(), cfg.maxSigma1sCents());
        }
    }
}
