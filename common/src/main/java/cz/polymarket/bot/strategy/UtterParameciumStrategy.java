package cz.polymarket.bot.strategy;

import cz.polymarket.bot.domain.MarketCandle;
import cz.polymarket.bot.domain.OrderBookQuote;
import cz.polymarket.bot.domain.Timeframe;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.math.BigDecimal;
import java.util.Set;

/**
 * {@code Utter-Paramecium} final-minute Chainlink 60s TWAP lock-in strategy.
 * <p>
 * Operates exclusively in the final settlement window ({@code tau < 65s}) of 15-minute binary
 * prediction markets using Polymarket PolyBolt streams ({@code price.crypto} and
 * {@code price.crypto.twap}) as the primary settlement signal, Polymarket CLOB order book quotes,
 * and basis-aligned multi-exchange {@link #onCalculatedSpotTwap} updates as both a protective
 * early-reversal filter and a conservative breakout confirmation amplifier.
 * <p>
 * Key quantitative and risk-control components:
 * <ul>
 *   <li>Explicit target price ({@code targetPriceCents}) locked from PolyBolt 60s TWAP at candle start.</li>
 *   <li>Exact millisecond step-weighted TWAP integral over the locked portion of the 60s settlement window,
 *       with guarded pre-window activation ({@code 60s < tau <= 64s}) and mathematically locked-in late
 *       entries ({@code tau <= 30s}) up to {@code $0.92} (while keeping {@code $0.84} ceiling when unlocked).</li>
 *   <li>Oracle-timestamped multi-horizon robust MAD BTC volatility estimator ({@code 1s} burst MAD,
 *       {@code 1s} short MAD, {@code 5s} intra-candle regime MAD, and intra-candle {@code 14m} Parkinson
 *       wave-range floor) immune to arrival jitter, quiet-lull underestimation, and wave dispersion.</li>
 *   <li>Basis-aligned {@code onCalculatedSpotTwap} bidirectional signal (~1–1.5s ahead of Chainlink) that
 *       vetoes/haircuts adverse reversals and conservatively boosts confirmed Chainlink breakouts without
 *       ever fabricating direction against Chainlink.</li>
 *   <li>Recalibrated continuous minimum Z-score curve across {@code $0.67..$0.92}, multi-exchange-confirmed
 *       deep-cushion pullback exemption and {@code $0.80..$0.84} pre-lock-in unlock ({@code 38s < tau <= 46s}),
 *       smooth opening-window inventory ramp ({@code 57s..60s}) with contrarian CLOB ask protection,
 *       moderate-wave calm-tail Parkinson damping, persistent locked-debt and intra-settlement-window whipsaw
 *       guards, and cushion-decay / bid-drawdown stop-add protection.</li>
 * </ul>
 * <p>
 * All hot-path calculations use deterministic integer/fixed-point arithmetic ({@code long} USD cents
 * for BTC and {@code int} millicents {@code 0..1000} for option prices) with pre-allocated primitive
 * circular buffers and zero heap allocations per tick.
 */
@ApplicationScoped
public class UtterParameciumStrategy implements TradingStrategy {

    /**
     * Pre-computed remaining 60s TWAP standard deviation weights {@code W_bps(tau)} for {@code tau = 0..66} seconds,
     * including pre-settlement-window variance lead ({@code 61..66s}).
     */
    public static final int[] TWAP_REM_WEIGHT_BPS = {
                0,   167,   373,   624,   913,  1236,  1591,  1974,  2385,  2822,
             3284,  3770,  4279,  4810,  5363,  5937,  6531,  7145,  7778,  8430,
             9100,  9788, 10494, 11217, 11957, 12713, 13485, 14273, 15077, 15896,
            16730, 17579, 18442, 19319, 20211, 21116, 22035, 22967, 23913, 24872,
            25843, 26827, 27824, 28833, 29854, 30887, 31932, 32989, 34057, 35137,
            36228, 37330, 38443, 39567, 40702, 41847, 43003, 44169, 45346, 46533,
            47730, 48937, 50154, 51381, 52618, 53865, 55122
    };

    /**
     * Standard normal cumulative distribution table {@code Phi(z)} in millicents ({@code 500..999 mc})
     * for {@code z = 0.00, 0.10, 0.20, ..., 4.00} sigma (41 points).
     */
    public static final int[] NORMAL_CDF_HALF_TABLE_MC = {
            500, 540, 579, 618, 655, 691, 726, 758, 788, 816,
            841, 864, 885, 903, 919, 933, 945, 955, 964, 971,
            977, 982, 986, 989, 992, 994, 995, 997, 997, 998,
            999, 999, 999, 999, 999, 999, 999, 999, 999, 999,
            999
    };

    private static final int MAX_MC = 1_000;
    private static final int TICK_STEP_MC = 10;
    private static final long MIN_SIGMA_REM_CENTS = 40L;
    private static final long CANDLE_DURATION_MS = 900_000L;
    private static final long MAX_TARGET_PRICE_OFFSET_MS = 10_000L;
    private static final long TWAP_WINDOW_MS = 60_000L;
    private static final long BUFFER_RETENTION_MS = 75_000L;
    private static final long RECENT_QUOTE_WINDOW_MS = 8_000L;
    private static final long MAX_ORACLE_LAG_CLAMP_MS = 4_000L;
    private static final long PRE_ACTIVATION_BALANCE_SYNC_LEAD_MS = 12_000L;
    private static final int PRE_WINDOW_MIN_ASK_MC = 560;
    private static final int PRE_WINDOW_MAX_ASK_MC = 740;
    private static final long PRE_WINDOW_MIN_DIST_CENTS = 650L;
    private static final long PRE_WINDOW_MIN_Z_CENTI = 110L;
    private static final int PRE_WINDOW_MAX_INVENTORY_SHARES = 60;
    private static final long EARLY_OPENING_GUARD_TAU_MS = 52_000L;
    private static final int EARLY_OPENING_MAX_CONTRARIAN_ASK_MC = 500;
    private static final int EARLY_CONTRARIAN_MAX_BID_DROP_MC = 30;
    private static final long CALM_TAIL_CONTRARIAN_MIN_Z_CENTI = 50L;
    private static final long OPENING_WINDOW_RAMP_END_TAU_MS = 57_000L;
    private static final int OPENING_WINDOW_BASE_SHARES = 180;
    private static final int OPENING_RAMP_MIN_ASK_MC = 620;
    private static final long OPENING_RAMP_EXEMPT_Z_CENTI = 110L;
    private static final int NON_LOCKED_MAX_ENTRY_PRICE_MC = 840;
    private static final int LOCKED_MID_HIGH_ASK_CEILING_MC = 880;
    private static final long CRASH_IMMUNE_LOCKIN_MAX_TAU_MS = 25_000L;
    private static final long CRASH_IMMUNE_MIN_CONTRIB_CENTS = 1_200L;
    private static final long LATE_LOCKIN_MAX_TAU_MS = 30_000L;
    private static final long LATE_LOCKIN_MIN_CONTRIB_CENTS = 550L;
    private static final long LATE_LOCKIN_MIN_PROJ_LOCK_CENTS = 750L;
    private static final long LATE_LOCKIN_MIN_PROJ_DIST_CENTS = 1_200L;
    private static final long LATE_LOCKIN_MIN_NET_DIST_CENTS = 600L;
    private static final long LOCKED_HIGH_PRICE_MIN_CONTRIB_CENTS = 600L;
    private static final long LOCKED_HIGH_PRICE_MIN_SPOT_CENTS = 400L;
    private static final long LOCKED_HIGH_PRICE_MAX_ADVERSE_CALC_CENTS = 800L;
    private static final int HIGH_PRICE_ASK_THRESHOLD_MC = 800;
    private static final long HIGH_PRICE_MAX_TAU_MS = 38_000L;
    private static final long EARLY_HIGH_PRICE_MAX_TAU_MS = 46_000L;
    private static final long EARLY_HIGH_PRICE_MIN_DIST_CENTS = 1_150L;
    private static final long EARLY_HIGH_PRICE_MIN_Z_CENTI = 225L;

    // Supplementary onCalculatedSpotTwap basis, noise deadband, favorable breakout & freshness constants
    private static final long CALC_SPOT_RETENTION_MS = 15_000L;
    private static final long MAX_CALC_FRESHNESS_MS = 2_500L;
    private static final long CALC_EVAL_MIN_INTERVAL_MS = 200L;
    private static final int MIN_CALC_BASIS_SAMPLES = 3;
    private static final long CALC_BASIS_EMA_SCALE = 16L;
    private static final long MAX_CALC_BASIS_INNOVATION_CENTS = 1_500L;
    private static final long CALC_NOISE_DEADBAND_CENTS = 320L;
    private static final int CALC_FAVORABLE_LEAD_WEIGHT_BPS = 4_000;
    private static final long CALC_FAVORABLE_DRIFT_4S_MIN_CENTS = 450L;
    private static final long CALC_FAVORABLE_MAX_BOOST_CENTS = 800L;

    // Deep-cushion pullback exemption constants
    private static final long PULLBACK_DEEP_DIST_CENTS = 1_400L;
    private static final long PULLBACK_MOD_DIST_CENTS = 1_000L;
    private static final long PULLBACK_MIN_Z_CENTI = 130L;
    private static final long PULLBACK_MOD_MIN_Z_CENTI = 150L;

    // Price-risk & dynamic batch/conviction inventory scaling, intra-window whipsaw & stop-add constants
    private static final int CORE_CHEAP_ASK_CEILING_MC = 670;
    private static final int TRANSITION_ASK_CEILING_MC = 720;
    private static final int MID_ASK_CEILING_MC = 760;
    private static final int EXPENSIVE_ASK_CEILING_MC = 800;
    private static final long BONUS_CONVICTION_Z_CENTI = 145L;
    private static final int BONUS_CONVICTION_EDGE_MC = 55;
    private static final long BONUS_CONVICTION_MAX_TAU_MS = 36_000L;
    private static final long TRANSITION_UNLOCK_MAX_TAU_MS = 34_000L;
    private static final long TRANSITION_UNLOCK_MIN_Z_CENTI = 135L;
    private static final int TRANSITION_UNLOCK_MIN_EDGE_MC = 50;
    private static final long MID_UNLOCK_MAX_TAU_MS = 30_000L;
    private static final long MID_UNLOCK_MIN_Z_CENTI = 160L;
    private static final long EARLY_CAUTIOUS_BATCH_Z_CENTI = 105L;
    private static final int EARLY_CAUTIOUS_MAX_SHARES = 25;
    private static final int MID_HIGH_ASK_MAX_SHARES = 30;
    private static final long BOOST_BATCH_MIN_Z_CENTI = 145L;
    private static final int BOOST_BATCH_MIN_EDGE_MC = 60;
    private static final int BOOST_BATCH_MAX_SHARES_BPS = 13_334;
    private static final long PRE_LOCKIN_CONVICTION_MAX_TAU_MS = 38_000L;
    private static final long PRE_LOCKIN_CONVICTION_LOW_Z_CENTI = 55L;
    private static final long PRE_LOCKIN_CONVICTION_FULL_Z_CENTI = 85L;
    private static final long PRE_LOCKIN_MIN_CONVICTION_BPS = 4_000L;
    private static final long LOCKED_DEBT_EARLY_CUTOFF_TAU_MS = 30_000L;
    private static final long LOCKED_DEBT_MID_CUTOFF_TAU_MS = 35_000L;
    private static final long LOCKED_DEBT_EARLY_DEFICIT_CENTS = 350L;
    private static final long LOCKED_DEBT_MID_MIN_PROJ_CENTS = 1_200L;
    private static final long LOCKED_DEBT_LATE_MIN_PROJ_CENTS = 1_000L;
    private static final long WHIPSAW_OPPOSITE_EXCURSION_CENTS = 500L;
    private static final long WHIPSAW_RECOVERY_MIN_PROJ_CENTS = 650L;
    private static final long WHIPSAW_RECOVERY_MAX_PROJ_CENTS = 1_250L;
    private static final long WHIPSAW_RECOVERY_MAX_SPOT_CENTS = 1_400L;
    private static final long WHIPSAW_RECOVERY_MIN_Z_CENTI = 105L;
    private static final int CUSHION_DECAY_MIN_SHARES = 150;
    private static final long CUSHION_DECAY_SAFE_CENTS = 1_200L;
    private static final long CUSHION_DECAY_RATIO_PCT = 50L;
    private static final int BID_DRAWDOWN_MIN_SHARES = 90;
    private static final int BID_DRAWDOWN_STOP_ADD_MC = 100;

    private final UtterParameciumStrategyConfig config;
    private final ChainlinkTwapBuffer chainlinkTwapBuffer = new ChainlinkTwapBuffer();
    private final SpotVolatilityBuffer spotVolatilityBuffer = new SpotVolatilityBuffer();
    private final RecentQuoteBuffer recentQuoteBuffer = new RecentQuoteBuffer();
    private final CalcSpotBuffer calcSpotBuffer = new CalcSpotBuffer();

    private StrategyContext context;

    // Active 15-minute candle session state
    private long activeCandleStartMs = 0L;
    private long activeCandleEndMs = 0L;
    private long targetPriceCents = 0L;
    private long nextCandleStartMs = 0L;
    private long nextCandleTargetPriceCents = 0L;
    private boolean missingTargetPriceWarned = false;

    // Real-time PolyBolt price.crypto feed tracking in USD cents
    private long lastChainlinkPriceCents = 0L;
    private long lastChainlinkTickMs = 0L;
    private long lastChainlinkPriceObsMs = 0L;

    // Supplementary multi-exchange onCalculatedSpotTwap tracking & slow basis EMA (scaled by 16)
    private long lastCalcSpotCents = 0L;
    private long lastCalcTickMs = 0L;
    private long lastCalcEvalMs = 0L;
    private long calcBasisEmaScaled = 0L;
    private int calcBasisSamples = 0;

    // Order book & per-market execution state
    private OrderBookQuote latestValidQuote = null;
    private int netSharesUp = 0;
    private int netSharesDown = 0;
    private int tradesInMarket = 0;
    private long peakFilledDistCents = 0L;
    private int peakFilledBidMc = 0;
    private long settlementWindowMinCents = 0L;
    private long settlementWindowMaxCents = 0L;
    private long lastSignalMs = 0L;
    private boolean hasEmittedSignal = false;
    private boolean balanceRefreshedForMarket = false;
    private boolean orderInFlight = false;
    private TradeDirection pendingDirection = TradeDirection.NO_TRADE;
    private int pendingRequestedShares = 0;
    private long pendingEffectiveDistCents = 0L;
    private int pendingBidMc = 0;
    private String pendingClientOrderId = null;
    private long orderSequence = 0L;
    private int lastModelProbUpMc = 500;

    /**
     * Constructs the strategy and unwraps any CDI client proxy on {@link UtterParameciumStrategyConfig}
     * for direct monomorphic field reads on the hot path.
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
                        config.maxModelProbMc(),
                        config.minEdgeMc(),
                        config.maxSlippageMc(),
                        config.minOrderShares(),
                        config.maxOrderShares(),
                        config.edgeScalingFactor(),
                        config.maxInventorySharesPerSide(),
                        config.earlyInventoryCapBps(),
                        config.fullInventoryTauMs(),
                        config.allowDirectionFlip(),
                        config.adverseDrift10sBlockCents(),
                        config.adverseAskDrop8sBlockMc(),
                        config.calcLeadWeightBps(),
                        config.calcAdverseLeadBlockCents(),
                        config.calcAdverseDrift4sBlockCents(),
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
     * Evaluates the standard normal CDF {@code Phi(z)} in millicents ({@code 1..999 mc})
     * using deterministic integer piecewise-linear interpolation.
     *
     * @param zCenti Z-score in hundredths of a standard deviation (e.g. {@code 150 = +1.50 sigma})
     * @return probability in millicents between {@code 1} and {@code 999}
     */
    public static int normalCdfMc(long zCenti) {
        long absZ = zCenti < 0L ? -zCenti : zCenti;
        int posMc;
        if (absZ >= 400L) {
            posMc = 999;
        } else {
            int idx = (int) (absZ / 10L);
            int rem = (int) (absZ % 10L);
            int y0 = NORMAL_CDF_HALF_TABLE_MC[idx];
            int y1 = NORMAL_CDF_HALF_TABLE_MC[idx + 1];
            posMc = y0 + ((y1 - y0) * rem + 5) / 10;
        }
        return zCenti >= 0L ? posMc : (MAX_MC - posMc);
    }

    /**
     * Computes the continuous piecewise-linear minimum Z-score (in centi-sigma) required to buy an option
     * at {@code askMc}, calibrated to the multi-horizon wave volatility estimator across {@code $0.67..$0.92}.
     *
     * @param askMc candidate ask or limit price in millicents
     * @return required minimum Z-score in centi-sigma ({@code 0} below {@code 670 mc})
     */
    public static long requiredMinZCenti(int askMc) {
        if (askMc < CORE_CHEAP_ASK_CEILING_MC) {
            return 0L;
        }
        if (askMc <= TRANSITION_ASK_CEILING_MC) {
            return 68L + ((long) (askMc - CORE_CHEAP_ASK_CEILING_MC) * 32L) / 50L;
        }
        if (askMc <= MID_ASK_CEILING_MC) {
            return 100L + ((long) (askMc - TRANSITION_ASK_CEILING_MC) * 36L) / 40L;
        }
        if (askMc <= EXPENSIVE_ASK_CEILING_MC) {
            return 136L + ((long) (askMc - MID_ASK_CEILING_MC) * 48L) / 40L;
        }
        if (askMc <= NON_LOCKED_MAX_ENTRY_PRICE_MC) {
            return 184L + ((long) (askMc - EXPENSIVE_ASK_CEILING_MC) * 41L) / 40L;
        }
        if (askMc <= 920) {
            return 235L + ((long) (askMc - NON_LOCKED_MAX_ENTRY_PRICE_MC) * 20L) / 80L;
        }
        return 265L;
    }

    /**
     * Computes the minimum required BTC price cushion over target in USD cents as a function of {@code tauMs}.
     * Ramps linearly from {@code 200} cents ($2.00) at {@code tau >= 45s} down to {@code 150} cents ($1.50)
     * at {@code tau <= 15s} to reject sub-tick micro-noise while preserving quiet low-volatility winners.
     *
     * @param tauMs remaining time to candle expiration in milliseconds
     * @return minimum required effective distance in USD cents
     */
    public static long minEffectiveDistCents(long tauMs) {
        if (tauMs >= 45_000L) {
            return 200L;
        }
        if (tauMs <= 15_000L) {
            return 150L;
        }
        return 150L + ((tauMs - 15_000L) * 50L) / 30_000L;
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
        return Math.max(TWAP_WINDOW_MS + 10_000L, config.maxTauMs() + PRE_ACTIVATION_BALANCE_SYNC_LEAD_MS) + 5_000L;
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
        Log.infof("Initialized %s (tau=[%d..%d ms], cooldown=%d ms, entryPrice=[%d..%d mc], maxProb=%d mc, maxSpread=%d mc, minEdge=%d mc, shares=[%d..%d], maxInventory=%d, earlyCap=%d bps, calcLeadBps=%d, flip=%s)",
                getName(),
                config.minTauMs(),
                config.maxTauMs(),
                config.cooldownMs(),
                config.minEntryPriceMc(),
                config.maxEntryPriceMc(),
                config.maxModelProbMc(),
                config.maxSpreadMc(),
                config.minEdgeMc(),
                config.minOrderShares(),
                config.maxOrderShares(),
                config.maxInventorySharesPerSide(),
                config.earlyInventoryCapBps(),
                config.calcLeadWeightBps(),
                config.allowDirectionFlip());
    }

    @Override
    public void reset() {
        chainlinkTwapBuffer.clear();
        spotVolatilityBuffer.clear();
        recentQuoteBuffer.clear();
        calcSpotBuffer.clear();
        activeCandleStartMs = 0L;
        activeCandleEndMs = 0L;
        targetPriceCents = 0L;
        nextCandleStartMs = 0L;
        nextCandleTargetPriceCents = 0L;
        missingTargetPriceWarned = false;
        lastChainlinkPriceCents = 0L;
        lastChainlinkTickMs = 0L;
        lastChainlinkPriceObsMs = 0L;
        lastCalcSpotCents = 0L;
        lastCalcTickMs = 0L;
        lastCalcEvalMs = 0L;
        calcBasisEmaScaled = 0L;
        calcBasisSamples = 0;
        resetMarketExecutionState();
    }

    private void resetMarketExecutionState() {
        recentQuoteBuffer.clear();
        latestValidQuote = null;
        netSharesUp = 0;
        netSharesDown = 0;
        tradesInMarket = 0;
        peakFilledDistCents = 0L;
        peakFilledBidMc = 0;
        settlementWindowMinCents = 0L;
        settlementWindowMaxCents = 0L;
        lastSignalMs = 0L;
        lastCalcEvalMs = 0L;
        hasEmittedSignal = false;
        balanceRefreshedForMarket = false;
        orderInFlight = false;
        pendingDirection = TradeDirection.NO_TRADE;
        pendingRequestedShares = 0;
        pendingEffectiveDistCents = 0L;
        pendingBidMc = 0;
        pendingClientOrderId = null;
        lastModelProbUpMc = 500;
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
                spotVolatilityBuffer.clear();
                calcSpotBuffer.clear();
                lastChainlinkPriceCents = 0L;
                lastChainlinkTickMs = 0L;
                lastChainlinkPriceObsMs = 0L;
                lastCalcSpotCents = 0L;
                lastCalcTickMs = 0L;
                calcBasisEmaScaled = 0L;
                calcBasisSamples = 0;
            } else {
                spotVolatilityBuffer.resetCandleRange();
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
        if (price <= 0.0 || timestampMs <= 0L) {
            return;
        }
        if (lastChainlinkTickMs > 0L && timestampMs <= lastChainlinkTickMs) {
            return;
        }
        ensureActiveCandle(timestampMs);
        maybeRefreshBalanceBeforeActivation(timestampMs);
        long priceCents = Math.round(price * 100.0);
        recordChainlinkSample(timestampMs, priceCents, priceTimestampMs);

        OrderBookQuote quote = this.latestValidQuote;
        if (quote != null && (quote.timestampMs() <= 0L || timestampMs + 2_000L >= quote.timestampMs())) {
            evaluateEntry(timestampMs, quote);
        }
    }

    private void recordChainlinkSample(long timestampMs, long priceCents, long priceTimestampMs) {
        long minRetainMs = Math.max(0L, timestampMs - BUFFER_RETENTION_MS);
        chainlinkTwapBuffer.pushAndEvict(timestampMs, priceCents, minRetainMs);

        long obsMs = (priceTimestampMs > 0L && priceTimestampMs <= timestampMs)
                ? priceTimestampMs
                : timestampMs;
        long settlementWindowStartMs = activeCandleEndMs - TWAP_WINDOW_MS;
        boolean inPreSettlementCandle = activeCandleStartMs > 0L
                && obsMs >= activeCandleStartMs
                && obsMs <= settlementWindowStartMs;
        spotVolatilityBuffer.onTick(obsMs, priceCents, inPreSettlementCandle);

        if (activeCandleEndMs > 0L && timestampMs >= settlementWindowStartMs && timestampMs <= activeCandleEndMs) {
            if (settlementWindowMinCents <= 0L || priceCents < settlementWindowMinCents) {
                settlementWindowMinCents = priceCents;
            }
            if (priceCents > settlementWindowMaxCents) {
                settlementWindowMaxCents = priceCents;
            }
        }

        lastChainlinkPriceCents = priceCents;
        lastChainlinkTickMs = timestampMs;
        lastChainlinkPriceObsMs = obsMs;

        long matchedCalcCents = calcSpotBuffer.priceAtOrBefore(obsMs, MAX_CALC_FRESHNESS_MS);
        if (matchedCalcCents > 0L) {
            long sampleBasisCents = matchedCalcCents - priceCents;
            if (calcBasisSamples == 0) {
                calcBasisEmaScaled = sampleBasisCents * CALC_BASIS_EMA_SCALE;
                calcBasisSamples = 1;
            } else if (calcBasisSamples < MIN_CALC_BASIS_SAMPLES) {
                calcBasisEmaScaled = (calcBasisEmaScaled * calcBasisSamples + sampleBasisCents * CALC_BASIS_EMA_SCALE)
                        / (calcBasisSamples + 1L);
                calcBasisSamples++;
            } else {
                long currentBasisCents = roundedBasisCents(calcBasisEmaScaled);
                long clampedSampleCents = Math.clamp(
                        sampleBasisCents,
                        currentBasisCents - MAX_CALC_BASIS_INNOVATION_CENTS,
                        currentBasisCents + MAX_CALC_BASIS_INNOVATION_CENTS
                );
                calcBasisEmaScaled += (clampedSampleCents - currentBasisCents);
            }
        }
    }

    private static long roundedBasisCents(long scaledBasis) {
        long half = CALC_BASIS_EMA_SCALE / 2L;
        return scaledBasis >= 0L
                ? (scaledBasis + half) / CALC_BASIS_EMA_SCALE
                : (scaledBasis - half) / CALC_BASIS_EMA_SCALE;
    }

    @Override
    public void onCalculatedSpotTwap(
            long timestampMs,
            String symbol,
            double spotPrice,
            double twap60sPrice) {
        if (spotPrice <= 0.0 || timestampMs <= 0L) {
            return;
        }
        if (lastCalcTickMs > 0L && timestampMs < lastCalcTickMs) {
            return;
        }
        if (lastCalcTickMs > 0L && timestampMs - lastCalcTickMs > 120_000L) {
            calcSpotBuffer.clear();
            calcBasisEmaScaled = 0L;
            calcBasisSamples = 0;
        }
        long spotCents = Math.round(spotPrice * 100.0);
        lastCalcSpotCents = spotCents;
        lastCalcTickMs = timestampMs;
        calcSpotBuffer.record(timestampMs, spotCents, Math.max(0L, timestampMs - CALC_SPOT_RETENTION_MS));

        OrderBookQuote quote = this.latestValidQuote;
        if (quote != null
                && !orderInFlight
                && activeCandleEndMs > 0L
                && targetPriceCents > 0L
                && lastChainlinkPriceCents > 0L
                && calcBasisSamples >= MIN_CALC_BASIS_SAMPLES
                && (timestampMs - lastCalcEvalMs) >= CALC_EVAL_MIN_INTERVAL_MS) {
            long tauMs = activeCandleEndMs - timestampMs;
            if (tauMs >= config.minTauMs() && tauMs <= config.maxTauMs()) {
                long alignedCalcCents = spotCents - roundedBasisCents(calcBasisEmaScaled);
                long rawChainlinkDeltaCents = lastChainlinkPriceCents - targetPriceCents;
                long calcLeadCents = alignedCalcCents - lastChainlinkPriceCents;
                if ((rawChainlinkDeltaCents >= 150L && calcLeadCents > CALC_NOISE_DEADBAND_CENTS)
                        || (rawChainlinkDeltaCents <= -150L && -calcLeadCents > CALC_NOISE_DEADBAND_CENTS)) {
                    lastCalcEvalMs = timestampMs;
                    if (quote.timestampMs() <= 0L || timestampMs + 2_000L >= quote.timestampMs()) {
                        evaluateEntry(timestampMs, quote);
                    }
                }
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
        if (twapPrice <= 0.0 || timestampMs <= 0L) {
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
            recordQuoteSample(nowMs, quote);
            evaluateEntry(nowMs, quote);
        }
    }

    private void recordQuoteSample(long nowMs, OrderBookQuote quote) {
        if (activeCandleEndMs <= 0L || nowMs <= 0L) {
            return;
        }
        long tauMs = activeCandleEndMs - nowMs;
        if (tauMs < 0L || tauMs > Math.max(TWAP_WINDOW_MS, config.maxTauMs()) + 10_000L) {
            return;
        }
        int rawBidUpMc = (int) Math.round(quote.bestBidUp() * 1000.0);
        int rawAskUpMc = (int) Math.round(quote.bestAskUp() * 1000.0);
        if (rawBidUpMc <= 0 || rawAskUpMc <= 0 || rawBidUpMc >= rawAskUpMc || rawBidUpMc >= MAX_MC || rawAskUpMc > MAX_MC) {
            return;
        }
        int bidUpMc = fromMcFloorTick(rawBidUpMc);
        int askUpMc = fromMcCeilTick(rawAskUpMc);
        if (bidUpMc <= 0 || bidUpMc >= askUpMc) {
            return;
        }
        int askDownMc;
        if (!Double.isNaN(quote.bestAskDown()) && quote.bestAskDown() > 0.0 && quote.bestAskDown() <= 1.0) {
            int rawAskDownMc = (int) Math.round(quote.bestAskDown() * 1000.0);
            askDownMc = fromMcCeilTick(Math.clamp(rawAskDownMc, 0, MAX_MC));
        } else {
            askDownMc = MAX_MC - bidUpMc;
        }
        int bidDownMc;
        if (!Double.isNaN(quote.bestBidDown()) && quote.bestBidDown() > 0.0 && quote.bestBidDown() < 1.0) {
            int rawBidDownMc = (int) Math.round(quote.bestBidDown() * 1000.0);
            bidDownMc = fromMcFloorTick(Math.clamp(rawBidDownMc, 0, MAX_MC));
        } else {
            bidDownMc = MAX_MC - askUpMc;
        }
        recentQuoteBuffer.record(nowMs, bidUpMc, askUpMc, bidDownMc, askDownMc, nowMs - RECENT_QUOTE_WINDOW_MS);
    }

    @Override
    public void onExecutionReport(ExecutionReport report) {
        if (report == null || report.clientOrderId() == null) {
            return;
        }
        if (pendingClientOrderId == null || !pendingClientOrderId.equals(report.clientOrderId())) {
            return;
        }
        String status = report.status();
        if ("FILLED".equalsIgnoreCase(status) || "PARTIALLY_FILLED".equalsIgnoreCase(status)) {
            int filledShares = report.executedSize() != null
                    ? (int) Math.round(report.executedSize().doubleValue())
                    : pendingRequestedShares;
            if (filledShares > 0) {
                if (pendingDirection == TradeDirection.UP) {
                    netSharesUp += filledShares;
                } else if (pendingDirection == TradeDirection.DOWN) {
                    netSharesDown += filledShares;
                }
                if (pendingEffectiveDistCents > peakFilledDistCents) {
                    peakFilledDistCents = pendingEffectiveDistCents;
                }
                if (pendingBidMc > peakFilledBidMc) {
                    peakFilledBidMc = pendingBidMc;
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
    public void onMarketCandleCompleted(MarketCandle candle) {
        // Realized volatility is estimated directly from 1s/5s PolyBolt price.crypto updates
    }

    @Override
    public void onCandleResolution(TradeDirection actualOutcome) {
        // All accumulated positions are held to binary settlement at candle expiration
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
     * Computes the progressive time-staged cumulative inventory limit allowed at remaining time {@code tauMs}.
     * <p>
     * Scales linearly from {@code earlyInventoryCapBps} of {@code maxInventorySharesPerSide}
     * at {@code tau >= 60s} up to 100% of {@code maxInventorySharesPerSide} at {@code tau <= fullInventoryTauMs}.
     *
     * @param tauMs remaining time to candle expiry in milliseconds
     * @return time-staged maximum cumulative shares allowed at {@code tauMs}
     */
    int computeTimeStagedMaxInventory(long tauMs) {
        int maxInv = config.maxInventorySharesPerSide();
        int earlyBps = config.earlyInventoryCapBps();
        if (earlyBps >= 10_000 || tauMs <= config.fullInventoryTauMs()) {
            return maxInv;
        }
        int earlyCap = Math.max(config.minOrderShares(), (int) (((long) maxInv * earlyBps) / 10_000L));
        if (tauMs >= TWAP_WINDOW_MS) {
            return earlyCap;
        }
        long rampSpanMs = TWAP_WINDOW_MS - config.fullInventoryTauMs();
        if (rampSpanMs <= 0L) {
            return maxInv;
        }
        long progressedMs = TWAP_WINDOW_MS - tauMs;
        long unlockedExtra = ((long) (maxInv - earlyCap) * progressedMs) / rampSpanMs;
        return (int) Math.clamp(earlyCap + unlockedExtra, earlyCap, maxInv);
    }

    /**
     * Computes the dynamic cumulative inventory cap combining TWAP lock-in progression ({@code tauMs})
     * with binary option price asymmetry ({@code askMc}), statistical cushion ({@code zSideCenti}),
     * net edge ({@code edgeMc}), and mathematical TWAP lock-in ({@code trulyLockedIn}).
     *
     * @param tauMs remaining time to candle expiry in milliseconds
     * @param askMc candidate ask price in millicents
     * @param zSideCenti candidate-side Z-score in centi-sigma
     * @param edgeMc net price edge after taker fee in millicents
     * @param trulyLockedIn whether the 60s TWAP integral is mathematically locked in ({@code tau <= 30s})
     * @return dynamic maximum cumulative shares allowed for the active side
     */
    int computeDynamicMaxInventory(long tauMs, int askMc, long zSideCenti, int edgeMc, boolean trulyLockedIn) {
        int baseStagedCap = computeTimeStagedMaxInventory(tauMs);
        if (tauMs <= TWAP_WINDOW_MS
                && tauMs > OPENING_WINDOW_RAMP_END_TAU_MS
                && askMc >= OPENING_RAMP_MIN_ASK_MC
                && zSideCenti < OPENING_RAMP_EXEMPT_Z_CENTI) {
            baseStagedCap = OPENING_WINDOW_BASE_SHARES
                    + (int) (((long) Math.max(0, baseStagedCap - OPENING_WINDOW_BASE_SHARES)
                            * (TWAP_WINDOW_MS - tauMs)) / (TWAP_WINDOW_MS - OPENING_WINDOW_RAMP_END_TAU_MS));
        }
        long riskTierBps;
        if (askMc <= CORE_CHEAP_ASK_CEILING_MC) {
            if (tauMs <= BONUS_CONVICTION_MAX_TAU_MS
                    && zSideCenti >= BONUS_CONVICTION_Z_CENTI
                    && edgeMc >= BONUS_CONVICTION_EDGE_MC) {
                riskTierBps = 13_000L;
            } else {
                riskTierBps = 10_000L;
            }
        } else if (askMc <= TRANSITION_ASK_CEILING_MC) {
            if (trulyLockedIn
                    || (tauMs <= TRANSITION_UNLOCK_MAX_TAU_MS
                        && zSideCenti >= TRANSITION_UNLOCK_MIN_Z_CENTI
                        && edgeMc >= TRANSITION_UNLOCK_MIN_EDGE_MC)) {
                riskTierBps = 11_000L;
            } else {
                riskTierBps = 8_000L;
            }
        } else if (askMc <= MID_ASK_CEILING_MC) {
            if (trulyLockedIn || (tauMs <= MID_UNLOCK_MAX_TAU_MS && zSideCenti >= MID_UNLOCK_MIN_Z_CENTI)) {
                riskTierBps = 9_500L;
            } else {
                riskTierBps = 6_500L;
            }
        } else if (askMc <= NON_LOCKED_MAX_ENTRY_PRICE_MC) {
            if (trulyLockedIn) {
                riskTierBps = 9_000L;
            } else if (askMc < EXPENSIVE_ASK_CEILING_MC) {
                riskTierBps = 4_200L;
            } else {
                riskTierBps = 3_200L;
            }
        } else if (askMc <= LOCKED_MID_HIGH_ASK_CEILING_MC) {
            riskTierBps = 8_000L;
        } else {
            riskTierBps = 6_000L;
        }

        long convictionBps = 10_000L;
        if (!trulyLockedIn
                && tauMs > PRE_LOCKIN_CONVICTION_MAX_TAU_MS
                && zSideCenti < PRE_LOCKIN_CONVICTION_FULL_Z_CENTI) {
            if (zSideCenti <= PRE_LOCKIN_CONVICTION_LOW_Z_CENTI) {
                convictionBps = PRE_LOCKIN_MIN_CONVICTION_BPS;
            } else {
                long zSpan = PRE_LOCKIN_CONVICTION_FULL_Z_CENTI - PRE_LOCKIN_CONVICTION_LOW_Z_CENTI;
                convictionBps = PRE_LOCKIN_MIN_CONVICTION_BPS
                        + ((zSideCenti - PRE_LOCKIN_CONVICTION_LOW_Z_CENTI) * (10_000L - PRE_LOCKIN_MIN_CONVICTION_BPS)) / zSpan;
            }
        }

        long combinedBps = (riskTierBps * convictionBps) / 10_000L;
        int dynamicCap = (int) (((long) baseStagedCap * combinedBps) / 10_000L);
        if (tauMs > TWAP_WINDOW_MS) {
            dynamicCap = Math.min(dynamicCap, PRE_WINDOW_MAX_INVENTORY_SHARES);
        }
        return Math.max(config.minOrderShares(), dynamicCap);
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

        // Extract and conservatively round BBO prices to 10-mc ($0.01) ticks
        int rawBidUpMc = (int) Math.round(quote.bestBidUp() * 1000.0);
        int rawAskUpMc = (int) Math.round(quote.bestAskUp() * 1000.0);
        if (rawBidUpMc <= 0 || rawAskUpMc <= 0 || rawBidUpMc >= rawAskUpMc || rawBidUpMc >= MAX_MC || rawAskUpMc > MAX_MC) {
            latestValidQuote = null;
            return;
        }

        int bidUpMc = fromMcFloorTick(rawBidUpMc);
        int askUpMc = fromMcCeilTick(rawAskUpMc);
        if (bidUpMc <= 0 || bidUpMc >= askUpMc) {
            latestValidQuote = null;
            return;
        }

        int spreadMc = askUpMc - bidUpMc;
        if (spreadMc > config.maxSpreadMc()) {
            return;
        }

        int askDownMc;
        if (!Double.isNaN(quote.bestAskDown()) && quote.bestAskDown() > 0.0 && quote.bestAskDown() <= 1.0) {
            int rawAskDownMc = (int) Math.round(quote.bestAskDown() * 1000.0);
            askDownMc = fromMcCeilTick(Math.clamp(rawAskDownMc, 0, MAX_MC));
        } else {
            askDownMc = MAX_MC - bidUpMc;
        }
        int bidDownMc;
        if (!Double.isNaN(quote.bestBidDown()) && quote.bestBidDown() > 0.0 && quote.bestBidDown() < 1.0) {
            int rawBidDownMc = (int) Math.round(quote.bestBidDown() * 1000.0);
            bidDownMc = fromMcFloorTick(Math.clamp(rawBidDownMc, 0, MAX_MC));
        } else {
            bidDownMc = MAX_MC - askUpMc;
        }

        // Compute locked 60s TWAP integral and lag-compensated remaining uncertainty sigma_rem(tau_eff)
        long spotCents = lastChainlinkPriceCents;
        long settlementWindowStartMs = activeCandleEndMs - TWAP_WINDOW_MS;
        long lockedMeanCents = (tauMs < TWAP_WINDOW_MS)
                ? chainlinkTwapBuffer.meanCentsInWindow(settlementWindowStartMs, nowMs)
                : 0L;
        boolean fullWindowAnchored = tauMs < TWAP_WINDOW_MS
                && chainlinkTwapBuffer.priceAtOrBefore(settlementWindowStartMs, 30_000L) > 0L;

        long baseProjectedTwapCents = projectTwapCents(tauMs, lockedMeanCents, spotCents);
        long effectiveObsMs = lastChainlinkPriceObsMs > 0L ? lastChainlinkPriceObsMs : lastChainlinkTickMs;
        long oracleLagMs = Math.clamp(nowMs - effectiveObsMs, 0L, MAX_ORACLE_LAG_CLAMP_MS);
        long sigma1sCents = spotVolatilityBuffer.sigma1sCents(config);
        int effectiveTauSec = (int) Math.clamp((tauMs + oracleLagMs + 500L) / 1000L, 0L, TWAP_REM_WEIGHT_BPS.length - 1L);
        long weightBps = TWAP_REM_WEIGHT_BPS[effectiveTauSec];
        long sigmaRemCents = Math.max(MIN_SIGMA_REM_CENTS, (sigma1sCents * weightBps) / 10_000L);

        long zCenti = (100L * (baseProjectedTwapCents - targetPriceCents)) / sigmaRemCents;
        this.lastModelProbUpMc = normalCdfMc(zCenti);

        // Bidirectional multi-exchange onCalculatedSpotTwap: protective adverse haircut & conservative breakout confirmation
        boolean hasFreshCalcSpot = calcBasisSamples >= MIN_CALC_BASIS_SAMPLES
                && lastCalcSpotCents > 0L
                && lastCalcTickMs > 0L
                && nowMs >= lastCalcTickMs
                && (nowMs - lastCalcTickMs) <= MAX_CALC_FRESHNESS_MS;

        long effectiveSpotUpCents = spotCents;
        long effectiveSpotDownCents = spotCents;
        long adverseCalcLeadUpCents = 0L;
        long adverseCalcLeadDownCents = 0L;
        long calcUpDrift4sCents = 0L;
        if (hasFreshCalcSpot) {
            long calcPrice4sAgoCents = calcSpotBuffer.priceAtOrBefore(nowMs - 4_000L, MAX_CALC_FRESHNESS_MS);
            if (calcPrice4sAgoCents > 0L) {
                calcUpDrift4sCents = lastCalcSpotCents - calcPrice4sAgoCents;
            }
            long alignedCalcSpotCents = lastCalcSpotCents - roundedBasisCents(calcBasisEmaScaled);
            long calcLeadUpCents = alignedCalcSpotCents - spotCents;
            long minDistCents = minEffectiveDistCents(tauMs);
            long rawUpDistCents = spotCents - targetPriceCents;
            long rawDownDistCents = targetPriceCents - spotCents;
            int leadWeightBps = config.calcLeadWeightBps();
            if (calcLeadUpCents < 0L) {
                adverseCalcLeadUpCents = -calcLeadUpCents;
                long excessLeadCents = Math.max(0L, adverseCalcLeadUpCents - CALC_NOISE_DEADBAND_CENTS);
                effectiveSpotUpCents = spotCents - (excessLeadCents * leadWeightBps) / 10_000L;
                if (rawDownDistCents >= minDistCents
                        && adverseCalcLeadUpCents > CALC_NOISE_DEADBAND_CENTS
                        && -calcUpDrift4sCents >= CALC_FAVORABLE_DRIFT_4S_MIN_CENTS) {
                    long boostDownCents = Math.min(
                            CALC_FAVORABLE_MAX_BOOST_CENTS,
                            ((adverseCalcLeadUpCents - CALC_NOISE_DEADBAND_CENTS) * CALC_FAVORABLE_LEAD_WEIGHT_BPS) / 10_000L
                    );
                    effectiveSpotDownCents = spotCents - boostDownCents;
                }
            } else if (calcLeadUpCents > 0L) {
                adverseCalcLeadDownCents = calcLeadUpCents;
                long excessLeadCents = Math.max(0L, adverseCalcLeadDownCents - CALC_NOISE_DEADBAND_CENTS);
                effectiveSpotDownCents = spotCents + (excessLeadCents * leadWeightBps) / 10_000L;
                if (rawUpDistCents >= minDistCents
                        && adverseCalcLeadDownCents > CALC_NOISE_DEADBAND_CENTS
                        && calcUpDrift4sCents >= CALC_FAVORABLE_DRIFT_4S_MIN_CENTS) {
                    long boostUpCents = Math.min(
                            CALC_FAVORABLE_MAX_BOOST_CENTS,
                            ((adverseCalcLeadDownCents - CALC_NOISE_DEADBAND_CENTS) * CALC_FAVORABLE_LEAD_WEIGHT_BPS) / 10_000L
                    );
                    effectiveSpotUpCents = spotCents + boostUpCents;
                }
            }
        }
        long projectedTwapUpCents = projectTwapCents(tauMs, lockedMeanCents, effectiveSpotUpCents);
        long projectedTwapDownCents = projectTwapCents(tauMs, lockedMeanCents, effectiveSpotDownCents);

        // Compute short-term Chainlink drift and sustained 1s-bucketed CLOB BBO levels
        long price5sAgoCents = chainlinkTwapBuffer.priceAtOrBefore(nowMs - 5_000L, 3_000L);
        long price10sAgoCents = chainlinkTwapBuffer.priceAtOrBefore(nowMs - 10_000L, 4_000L);
        long upDrift5sCents = price5sAgoCents > 0L ? (spotCents - price5sAgoCents) : 0L;
        long upDrift10sCents = price10sAgoCents > 0L ? (spotCents - price10sAgoCents) : 0L;
        long minQuoteWindowMs = nowMs - RECENT_QUOTE_WINDOW_MS;
        int maxBidUp8sMc = recentQuoteBuffer.maxSustainedBid(true, minQuoteWindowMs);
        int maxAskUp8sMc = recentQuoteBuffer.maxSustainedAsk(true, minQuoteWindowMs);
        int maxBidDown8sMc = recentQuoteBuffer.maxSustainedBid(false, minQuoteWindowMs);
        int maxAskDown8sMc = recentQuoteBuffer.maxSustainedAsk(false, minQuoteWindowMs);

        long upDistProjCents = projectedTwapUpCents - targetPriceCents;
        long upDistSpotCents = effectiveSpotUpCents - targetPriceCents;
        long downDistProjCents = targetPriceCents - projectedTwapDownCents;
        long downDistSpotCents = targetPriceCents - effectiveSpotDownCents;
        long lockedDistUpCents = lockedMeanCents > 0L ? (lockedMeanCents - targetPriceCents) : 0L;
        long lockedDistDownCents = lockedMeanCents > 0L ? (targetPriceCents - lockedMeanCents) : 0L;
        long elapsedWindowMs = Math.max(0L, TWAP_WINDOW_MS - tauMs);
        long upLockedContribCents = (elapsedWindowMs * lockedDistUpCents) / TWAP_WINDOW_MS;
        long downLockedContribCents = (elapsedWindowMs * lockedDistDownCents) / TWAP_WINDOW_MS;
        boolean upCrashImmuneLockedIn = fullWindowAnchored
                && tauMs <= CRASH_IMMUNE_LOCKIN_MAX_TAU_MS
                && upLockedContribCents >= CRASH_IMMUNE_MIN_CONTRIB_CENTS;
        boolean downCrashImmuneLockedIn = fullWindowAnchored
                && tauMs <= CRASH_IMMUNE_LOCKIN_MAX_TAU_MS
                && downLockedContribCents >= CRASH_IMMUNE_MIN_CONTRIB_CENTS;
        boolean upTrulyLockedIn = upCrashImmuneLockedIn
                || (fullWindowAnchored
                    && tauMs <= LATE_LOCKIN_MAX_TAU_MS
                    && upLockedContribCents >= LATE_LOCKIN_MIN_CONTRIB_CENTS
                    && upDistProjCents >= LATE_LOCKIN_MIN_PROJ_LOCK_CENTS
                    && upDistSpotCents >= LOCKED_HIGH_PRICE_MIN_SPOT_CENTS);
        boolean downTrulyLockedIn = downCrashImmuneLockedIn
                || (fullWindowAnchored
                    && tauMs <= LATE_LOCKIN_MAX_TAU_MS
                    && downLockedContribCents >= LATE_LOCKIN_MIN_CONTRIB_CENTS
                    && downDistProjCents >= LATE_LOCKIN_MIN_PROJ_LOCK_CENTS
                    && downDistSpotCents >= LOCKED_HIGH_PRICE_MIN_SPOT_CENTS);

        boolean lockedDebtBlocksUp = lockedMeanCents > 0L
                && isBlockedByLockedDebt(tauMs, lockedDistUpCents, upDistProjCents);
        boolean lockedDebtBlocksDown = lockedMeanCents > 0L
                && isBlockedByLockedDebt(tauMs, lockedDistDownCents, downDistProjCents);
        long oppositeExcursionUpCents = (settlementWindowMinCents > 0L && settlementWindowMinCents < targetPriceCents)
                ? (targetPriceCents - settlementWindowMinCents)
                : 0L;
        long oppositeExcursionDownCents = (settlementWindowMaxCents > targetPriceCents)
                ? (settlementWindowMaxCents - targetPriceCents)
                : 0L;

        int edgeUpMc = ((!config.allowDirectionFlip() && netSharesDown > 0) || lockedDebtBlocksUp)
                ? Integer.MIN_VALUE
                : evaluateCandidateSideEdge(
                        bidUpMc,
                        askUpMc,
                        maxBidUp8sMc,
                        maxAskUp8sMc,
                        upCrashImmuneLockedIn,
                        upTrulyLockedIn,
                        hasFreshCalcSpot,
                        upLockedContribCents,
                        lockedDistUpCents,
                        oppositeExcursionUpCents,
                        upDistProjCents,
                        upDistSpotCents,
                        -upDrift5sCents,
                        -upDrift10sCents,
                        adverseCalcLeadUpCents,
                        -calcUpDrift4sCents,
                        tauMs,
                        sigmaRemCents
                );
        int edgeDownMc = ((!config.allowDirectionFlip() && netSharesUp > 0) || lockedDebtBlocksDown)
                ? Integer.MIN_VALUE
                : evaluateCandidateSideEdge(
                        bidDownMc,
                        askDownMc,
                        maxBidDown8sMc,
                        maxAskDown8sMc,
                        downCrashImmuneLockedIn,
                        downTrulyLockedIn,
                        hasFreshCalcSpot,
                        downLockedContribCents,
                        lockedDistDownCents,
                        oppositeExcursionDownCents,
                        downDistProjCents,
                        downDistSpotCents,
                        upDrift5sCents,
                        upDrift10sCents,
                        adverseCalcLeadDownCents,
                        calcUpDrift4sCents,
                        tauMs,
                        sigmaRemCents
                );

        TradeDirection chosenDirection = TradeDirection.NO_TRADE;
        int chosenBidMc = 0;
        int chosenAskMc = 0;
        int chosenEdgeMc = Integer.MIN_VALUE;

        if (edgeUpMc >= config.minEdgeMc() && edgeUpMc >= edgeDownMc) {
            chosenDirection = TradeDirection.UP;
            chosenBidMc = bidUpMc;
            chosenAskMc = askUpMc;
            chosenEdgeMc = edgeUpMc;
        } else if (edgeDownMc >= config.minEdgeMc()) {
            chosenDirection = TradeDirection.DOWN;
            chosenBidMc = bidDownMc;
            chosenAskMc = askDownMc;
            chosenEdgeMc = edgeDownMc;
        }

        if (chosenDirection == TradeDirection.NO_TRADE) {
            if (Log.isTraceEnabled()) {
                Log.tracef("Strategy evaluateEntry NO_TRADE: tauMs=%d, spotCents=%d, projTwapCents=%d, targetCents=%d, sigmaRemCents=%d, askUpMc=%d, askDownMc=%d, edgeUpMc=%d, edgeDownMc=%d",
                        tauMs, spotCents, baseProjectedTwapCents, targetPriceCents, sigmaRemCents, askUpMc, askDownMc, edgeUpMc, edgeDownMc);
            }
            return;
        }

        boolean isUpChosen = chosenDirection == TradeDirection.UP;
        boolean chosenCrashImmuneLockedIn = isUpChosen ? upCrashImmuneLockedIn : downCrashImmuneLockedIn;
        boolean chosenTrulyLockedIn = isUpChosen ? upTrulyLockedIn : downTrulyLockedIn;
        long chosenLockedContribCents = isUpChosen ? upLockedContribCents : downLockedContribCents;
        long chosenLockedDistCents = isUpChosen ? lockedDistUpCents : lockedDistDownCents;
        long chosenOppositeExcursionCents = isUpChosen ? oppositeExcursionUpCents : oppositeExcursionDownCents;
        long chosenDistProjCents = isUpChosen ? upDistProjCents : downDistProjCents;
        long chosenDistSpotCents = isUpChosen ? upDistSpotCents : downDistSpotCents;
        long chosenEffectiveDistCents = computeEffectiveDistCents(chosenDistProjCents, chosenDistSpotCents, tauMs, chosenTrulyLockedIn);
        if (chosenEffectiveDistCents <= 0L) {
            return;
        }

        int currentSideShares = isUpChosen ? netSharesUp : netSharesDown;
        if (!chosenCrashImmuneLockedIn) {
            if (currentSideShares >= CUSHION_DECAY_MIN_SHARES
                    && chosenEffectiveDistCents < CUSHION_DECAY_SAFE_CENTS
                    && chosenEffectiveDistCents * 100L < peakFilledDistCents * CUSHION_DECAY_RATIO_PCT) {
                return;
            }
            if (currentSideShares >= BID_DRAWDOWN_MIN_SHARES
                    && (peakFilledBidMc - chosenBidMc) >= BID_DRAWDOWN_STOP_ADD_MC) {
                return;
            }
        }

        long chosenZSideCenti = (100L * chosenEffectiveDistCents) / sigmaRemCents;
        long timeFactorBps = Math.clamp(((90_000L - tauMs) * 10_000L) / 60_000L, 7_000L, 12_000L);
        long scaled = ((long) config.edgeScalingFactor() * (chosenEdgeMc - config.minEdgeMc()) * timeFactorBps)
                / (10_000L * 100L);
        int effectiveMaxOrderShares = config.maxOrderShares();
        if (tauMs > TWAP_WINDOW_MS
                || (!chosenTrulyLockedIn && chosenAskMc > MID_ASK_CEILING_MC)
                || (!chosenTrulyLockedIn && tauMs > PRE_LOCKIN_CONVICTION_MAX_TAU_MS && chosenZSideCenti < EARLY_CAUTIOUS_BATCH_Z_CENTI)) {
            effectiveMaxOrderShares = Math.min(effectiveMaxOrderShares, EARLY_CAUTIOUS_MAX_SHARES);
        } else if (!chosenTrulyLockedIn && chosenAskMc > TRANSITION_ASK_CEILING_MC) {
            effectiveMaxOrderShares = Math.min(effectiveMaxOrderShares, MID_HIGH_ASK_MAX_SHARES);
        } else if (chosenTrulyLockedIn
                || (tauMs <= BONUS_CONVICTION_MAX_TAU_MS
                    && chosenAskMc <= TRANSITION_ASK_CEILING_MC
                    && chosenZSideCenti >= BOOST_BATCH_MIN_Z_CENTI
                    && chosenEdgeMc >= BOOST_BATCH_MIN_EDGE_MC)) {
            effectiveMaxOrderShares = Math.max(
                    effectiveMaxOrderShares,
                    (effectiveMaxOrderShares * BOOST_BATCH_MAX_SHARES_BPS) / 10_000
            );
        }
        effectiveMaxOrderShares = Math.max(config.minOrderShares(), effectiveMaxOrderShares);
        int qRaw = (int) Math.clamp(config.minOrderShares() + scaled, config.minOrderShares(), effectiveMaxOrderShares);

        int dynamicCapShares = computeDynamicMaxInventory(tauMs, chosenAskMc, chosenZSideCenti, chosenEdgeMc, chosenTrulyLockedIn);
        int remainingCap = Math.max(0, dynamicCapShares - currentSideShares);
        int qFinal = Math.min(qRaw, remainingCap);
        if (qFinal < config.minOrderShares()) {
            return;
        }

        long chosenAdverseDrift5s = isUpChosen ? -upDrift5sCents : upDrift5sCents;
        long chosenAdverseDrift10s = isUpChosen ? -upDrift10sCents : upDrift10sCents;
        long chosenAdverseCalcLead = isUpChosen ? adverseCalcLeadUpCents : adverseCalcLeadDownCents;
        long chosenAdverseCalcDrift4s = isUpChosen ? -calcUpDrift4sCents : calcUpDrift4sCents;
        int chosenMaxRecentBidMc = isUpChosen ? maxBidUp8sMc : maxBidDown8sMc;
        int chosenMaxRecentAskMc = isUpChosen ? maxAskUp8sMc : maxAskDown8sMc;

        int limitPriceMc = computeEdgePreservingLimitPriceMc(
                chosenBidMc,
                chosenAskMc,
                chosenMaxRecentBidMc,
                chosenMaxRecentAskMc,
                chosenCrashImmuneLockedIn,
                chosenTrulyLockedIn,
                hasFreshCalcSpot,
                chosenLockedContribCents,
                chosenLockedDistCents,
                chosenOppositeExcursionCents,
                chosenDistProjCents,
                chosenDistSpotCents,
                chosenAdverseDrift5s,
                chosenAdverseDrift10s,
                chosenAdverseCalcLead,
                chosenAdverseCalcDrift4s,
                tauMs,
                sigmaRemCents
        );

        if (Log.isDebugEnabled()) {
            Log.debugf("Strategy signal %s: tauMs=%d, askMc=%d, limitPriceMc=%d, edgeMc=%d, shares=%d, dynamicCap=%d (netUp=%d, netDown=%d)",
                    chosenDirection, tauMs, chosenAskMc, limitPriceMc, chosenEdgeMc, qFinal, dynamicCapShares, netSharesUp, netSharesDown);
        }

        submitBatchOrder(nowMs, chosenDirection, chosenBidMc, limitPriceMc, qFinal, chosenEffectiveDistCents);
    }

    private static boolean isBlockedByLockedDebt(long tauMs, long lockedDistSideCents, long distProjCents) {
        if (tauMs > LOCKED_DEBT_MID_CUTOFF_TAU_MS) {
            return lockedDistSideCents <= -LOCKED_DEBT_EARLY_DEFICIT_CENTS;
        }
        if (tauMs > LOCKED_DEBT_EARLY_CUTOFF_TAU_MS) {
            if (lockedDistSideCents <= -LOCKED_DEBT_EARLY_DEFICIT_CENTS) {
                long requiredMidProjCents = Math.max(LOCKED_DEBT_MID_MIN_PROJ_CENTS, -lockedDistSideCents * 3L);
                return distProjCents < requiredMidProjCents;
            }
            return false;
        }
        if (lockedDistSideCents < 0L) {
            long requiredProjCents = Math.max(LOCKED_DEBT_LATE_MIN_PROJ_CENTS, -lockedDistSideCents * 2L);
            return distProjCents < requiredProjCents;
        }
        return false;
    }

    private static long projectTwapCents(long tauMs, long lockedMeanCents, long effectiveSpotCents) {
        if (tauMs >= TWAP_WINDOW_MS || lockedMeanCents <= 0L) {
            return effectiveSpotCents;
        }
        long elapsedMs = TWAP_WINDOW_MS - tauMs;
        return (elapsedMs * lockedMeanCents + tauMs * effectiveSpotCents) / TWAP_WINDOW_MS;
    }

    private static long computeEffectiveDistCents(long distProjCents, long distSpotCents, long tauMs, boolean trulyLockedIn) {
        if (distProjCents <= 0L) {
            return -1L;
        }
        if (distSpotCents > 0L) {
            return trulyLockedIn ? distProjCents : Math.min(distProjCents, distSpotCents);
        }
        if (tauMs > CRASH_IMMUNE_LOCKIN_MAX_TAU_MS
                || distProjCents < LATE_LOCKIN_MIN_PROJ_DIST_CENTS
                || (distProjCents + distSpotCents) <= LATE_LOCKIN_MIN_NET_DIST_CENTS) {
            return -1L;
        }
        return distProjCents + distSpotCents;
    }

    private int evaluateCandidateSideEdge(
            int currentBidMc,
            int askMc,
            int maxRecentBidMc,
            int maxRecentAskMc,
            boolean crashImmuneLockedIn,
            boolean trulyLockedIn,
            boolean hasFreshCalcSpot,
            long lockedContribCents,
            long lockedDistSideCents,
            long oppositeWindowExcursionCents,
            long distProjCents,
            long distSpotCents,
            long adverseDrift5sCents,
            long adverseDrift10sCents,
            long adverseCalcLeadCents,
            long adverseCalcDrift4sCents,
            long tauMs,
            long sigmaRemCents) {
        if (askMc < config.minEntryPriceMc() || askMc > config.maxEntryPriceMc()) {
            return Integer.MIN_VALUE;
        }
        if (!trulyLockedIn && askMc > NON_LOCKED_MAX_ENTRY_PRICE_MC) {
            return Integer.MIN_VALUE;
        }
        if (askMc > NON_LOCKED_MAX_ENTRY_PRICE_MC) {
            if (lockedContribCents < LOCKED_HIGH_PRICE_MIN_CONTRIB_CENTS
                    || distSpotCents < LOCKED_HIGH_PRICE_MIN_SPOT_CENTS
                    || adverseCalcLeadCents >= LOCKED_HIGH_PRICE_MAX_ADVERSE_CALC_CENTS) {
                return Integer.MIN_VALUE;
            }
        }
        if (tauMs > TWAP_WINDOW_MS) {
            if (askMc < PRE_WINDOW_MIN_ASK_MC
                    || askMc > PRE_WINDOW_MAX_ASK_MC
                    || adverseDrift5sCents > 0L
                    || adverseCalcLeadCents > CALC_NOISE_DEADBAND_CENTS) {
                return Integer.MIN_VALUE;
            }
        } else if (tauMs > EARLY_OPENING_GUARD_TAU_MS && askMc <= EARLY_OPENING_MAX_CONTRARIAN_ASK_MC) {
            boolean surgingCalcBreakout = hasFreshCalcSpot
                    && adverseCalcLeadCents == 0L
                    && adverseCalcDrift4sCents <= -CALC_FAVORABLE_DRIFT_4S_MIN_CENTS
                    && (maxRecentAskMc <= askMc || (maxRecentAskMc - askMc) < config.adverseAskDrop8sBlockMc());
            boolean firmBookBreakout = (maxRecentBidMc <= 0 || (maxRecentBidMc - currentBidMc) < EARLY_CONTRARIAN_MAX_BID_DROP_MC)
                    && (!hasFreshCalcSpot
                        || (adverseCalcLeadCents <= CALC_NOISE_DEADBAND_CENTS
                            && adverseCalcDrift4sCents <= CALC_NOISE_DEADBAND_CENTS));
            if (!surgingCalcBreakout && !firmBookBreakout) {
                return Integer.MIN_VALUE;
            }
        }
        if (!trulyLockedIn && askMc >= HIGH_PRICE_ASK_THRESHOLD_MC && tauMs > EARLY_HIGH_PRICE_MAX_TAU_MS) {
            return Integer.MIN_VALUE;
        }

        long effectiveDistCents = computeEffectiveDistCents(distProjCents, distSpotCents, tauMs, trulyLockedIn);
        if (effectiveDistCents < minEffectiveDistCents(tauMs)) {
            return Integer.MIN_VALUE;
        }
        long zSideCenti = (100L * effectiveDistCents) / sigmaRemCents;
        if (spotVolatilityBuffer.lastCalmTailDamped
                && askMc <= EARLY_OPENING_MAX_CONTRARIAN_ASK_MC
                && zSideCenti < CALM_TAIL_CONTRARIAN_MIN_Z_CENTI) {
            return Integer.MIN_VALUE;
        }
        if (tauMs > TWAP_WINDOW_MS
                && (effectiveDistCents < PRE_WINDOW_MIN_DIST_CENTS || zSideCenti < PRE_WINDOW_MIN_Z_CENTI)) {
            return Integer.MIN_VALUE;
        }
        if (!trulyLockedIn && askMc >= HIGH_PRICE_ASK_THRESHOLD_MC && tauMs > HIGH_PRICE_MAX_TAU_MS) {
            if (!hasFreshCalcSpot
                    || adverseCalcLeadCents > CALC_NOISE_DEADBAND_CENTS
                    || adverseCalcDrift4sCents > 0L
                    || adverseDrift5sCents > 0L
                    || lockedDistSideCents < 0L
                    || effectiveDistCents < EARLY_HIGH_PRICE_MIN_DIST_CENTS
                    || zSideCenti < EARLY_HIGH_PRICE_MIN_Z_CENTI) {
                return Integer.MIN_VALUE;
            }
        }

        if (!crashImmuneLockedIn) {
            long calcLeadBlockCents = config.calcAdverseLeadBlockCents();
            if (calcLeadBlockCents > 0L && adverseCalcLeadCents >= calcLeadBlockCents) {
                return Integer.MIN_VALUE;
            }
            long calcDrift4sBlockCents = config.calcAdverseDrift4sBlockCents();
            if (calcDrift4sBlockCents > 0L && adverseCalcDrift4sCents >= calcDrift4sBlockCents) {
                return Integer.MIN_VALUE;
            }
            long maxAdverseDriftCents = Math.max(adverseDrift5sCents, adverseDrift10sCents);
            boolean confirmedDeepPullback = (askMc <= MID_ASK_CEILING_MC
                        || (askMc <= NON_LOCKED_MAX_ENTRY_PRICE_MC && adverseDrift5sCents <= 0L))
                    && lockedDistSideCents >= 0L
                    && hasFreshCalcSpot
                    && adverseCalcLeadCents <= CALC_NOISE_DEADBAND_CENTS
                    && adverseCalcDrift4sCents < CALC_FAVORABLE_DRIFT_4S_MIN_CENTS
                    && ((effectiveDistCents >= PULLBACK_DEEP_DIST_CENTS && zSideCenti >= PULLBACK_MIN_Z_CENTI)
                        || (effectiveDistCents >= PULLBACK_MOD_DIST_CENTS && zSideCenti >= PULLBACK_MOD_MIN_Z_CENTI))
                    && effectiveDistCents * 10L >= Math.max(0L, maxAdverseDriftCents) * 15L;
            if (!confirmedDeepPullback) {
                long block10sCents = config.adverseDrift10sBlockCents();
                if (block10sCents > 0L) {
                    long block5sCents = (block10sCents * 5L) / 8L;
                    if (adverseDrift5sCents >= block5sCents || adverseDrift10sCents >= block10sCents) {
                        return Integer.MIN_VALUE;
                    }
                }
                int askDropBlockMc = config.adverseAskDrop8sBlockMc();
                if (askDropBlockMc > 0) {
                    int bidDropBlockMc = (askDropBlockMc * 3) / 4;
                    if (maxRecentAskMc > askMc && (maxRecentAskMc - askMc) >= askDropBlockMc
                            && maxRecentBidMc > currentBidMc && (maxRecentBidMc - currentBidMc) >= bidDropBlockMc) {
                        return Integer.MIN_VALUE;
                    }
                }
            }
        }

        if (!crashImmuneLockedIn && oppositeWindowExcursionCents >= WHIPSAW_OPPOSITE_EXCURSION_CENTS) {
            long requiredSpotCents = Math.min(WHIPSAW_RECOVERY_MAX_SPOT_CENTS, oppositeWindowExcursionCents);
            long requiredProjCents = Math.clamp(
                    oppositeWindowExcursionCents,
                    WHIPSAW_RECOVERY_MIN_PROJ_CENTS,
                    WHIPSAW_RECOVERY_MAX_PROJ_CENTS
            );
            if (distSpotCents < requiredSpotCents
                    || distProjCents < requiredProjCents
                    || zSideCenti < WHIPSAW_RECOVERY_MIN_Z_CENTI) {
                return Integer.MIN_VALUE;
            }
        }
        long reqMinZ = requiredMinZCenti(askMc);
        if (reqMinZ > 0L && zSideCenti < reqMinZ) {
            return Integer.MIN_VALUE;
        }
        int pModelMc = Math.min(config.maxModelProbMc(), normalCdfMc(zSideCenti));
        if (pModelMc < config.minModelProbMc()) {
            return Integer.MIN_VALUE;
        }
        int feeMc = takerFeeMc(askMc, config.takerFeeRateBps());
        return pModelMc - askMc - feeMc;
    }

    /**
     * Computes the highest 10-mc ($0.01) tick limit price {@code >= askMc} that does not exceed
     * {@code askMc + config.maxSlippageMc()} or {@code config.maxEntryPriceMc()}, satisfies all
     * continuous Z-score, high-price lock-in, intra-window whipsaw, and momentum gates, and still
     * preserves at least {@code config.minEdgeMc()} net edge after the parabolic taker fee.
     */
    private int computeEdgePreservingLimitPriceMc(
            int currentBidMc,
            int askMc,
            int maxRecentBidMc,
            int maxRecentAskMc,
            boolean crashImmuneLockedIn,
            boolean trulyLockedIn,
            boolean hasFreshCalcSpot,
            long lockedContribCents,
            long lockedDistSideCents,
            long oppositeWindowExcursionCents,
            long distProjCents,
            long distSpotCents,
            long adverseDrift5sCents,
            long adverseDrift10sCents,
            long adverseCalcLeadCents,
            long adverseCalcDrift4sCents,
            long tauMs,
            long sigmaRemCents) {
        int maxAllowedMc = Math.min(config.maxEntryPriceMc(), askMc + config.maxSlippageMc());
        int limitMc = askMc;
        for (int candidateMc = askMc + TICK_STEP_MC; candidateMc <= maxAllowedMc; candidateMc += TICK_STEP_MC) {
            int candidateEdgeMc = evaluateCandidateSideEdge(
                    currentBidMc,
                    candidateMc,
                    maxRecentBidMc,
                    maxRecentAskMc,
                    crashImmuneLockedIn,
                    trulyLockedIn,
                    hasFreshCalcSpot,
                    lockedContribCents,
                    lockedDistSideCents,
                    oppositeWindowExcursionCents,
                    distProjCents,
                    distSpotCents,
                    adverseDrift5sCents,
                    adverseDrift10sCents,
                    adverseCalcLeadCents,
                    adverseCalcDrift4sCents,
                    tauMs,
                    sigmaRemCents
            );
            if (candidateEdgeMc < config.minEdgeMc()) {
                break;
            }
            limitMc = candidateMc;
        }
        return limitMc;
    }

    private void submitBatchOrder(
            long nowMs,
            TradeDirection direction,
            int bidMc,
            int askMc,
            int shares,
            long effectiveDistCents) {
        String clientOrderId = "up-v1-" + (activeCandleStartMs / 1000L) + "-" + (++orderSequence);
        String token = (direction == TradeDirection.UP) ? config.upToken() : config.downToken();
        BigDecimal price = BigDecimal.valueOf(askMc, 3);
        BigDecimal size = BigDecimal.valueOf(shares);

        this.orderInFlight = true;
        this.lastSignalMs = nowMs;
        this.hasEmittedSignal = true;
        this.pendingDirection = direction;
        this.pendingRequestedShares = shares;
        this.pendingEffectiveDistCents = effectiveDistCents;
        this.pendingBidMc = bidMc;
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

    private static boolean isQuoteTradable(OrderBookQuote quote) {
        double bidUp = quote.bestBidUp();
        double askUp = quote.bestAskUp();
        return !Double.isNaN(bidUp)
                && !Double.isNaN(askUp)
                && bidUp > 0.0
                && askUp > 0.0
                && bidUp < askUp
                && askUp < 1.0;
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
     * Returns the most recently evaluated UP model probability in millicents ({@code 1..999}).
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
     * for exact millisecond step-weighted 60-second settlement TWAP integration and short-term spot drift lookups.
     */
    static final class ChainlinkTwapBuffer {
        private static final int CAPACITY = 128;
        private static final int MASK = CAPACITY - 1;
        private static final long MAX_TWAP_ANCHOR_AGE_MS = 30_000L;

        private final long[] timestampsMs = new long[CAPACITY];
        private final long[] pricesCents = new long[CAPACITY];
        private int head = 0;
        private int len = 0;

        void clear() {
            head = 0;
            len = 0;
        }

        void evictOlderThan(long minTimestampMs) {
            // Retain one anchor sample at or before minTimestampMs when available for continuous step-hold integration
            while (len > 1 && timestampsMs[(head + 1) & MASK] <= minTimestampMs) {
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

        /**
         * Computes the exact millisecond step-weighted (zero-order hold) average price in USD cents
         * across {@code [windowStartMs, windowEndMs]}.
         */
        long meanCentsInWindow(long windowStartMs, long windowEndMs) {
            if (len == 0 || windowEndMs <= windowStartMs) {
                return 0L;
            }
            long activePrice = 0L;
            long cursorMs = windowStartMs;
            long weightedSum = 0L;
            long totalDurationMs = 0L;

            for (int i = 0; i < len; i++) {
                int idx = (head + i) & MASK;
                long ts = timestampsMs[idx];
                long price = pricesCents[idx];
                if (ts <= windowStartMs) {
                    if (windowStartMs - ts <= MAX_TWAP_ANCHOR_AGE_MS) {
                        activePrice = price;
                        cursorMs = windowStartMs;
                    }
                } else if (ts <= windowEndMs) {
                    if (activePrice > 0L && ts > cursorMs) {
                        long dt = ts - cursorMs;
                        weightedSum += activePrice * dt;
                        totalDurationMs += dt;
                    }
                    activePrice = price;
                    cursorMs = ts;
                } else {
                    break;
                }
            }

            if (activePrice > 0L && windowEndMs > cursorMs) {
                long dt = windowEndMs - cursorMs;
                weightedSum += activePrice * dt;
                totalDurationMs += dt;
            }

            if (totalDurationMs > 0L) {
                return (weightedSum + (totalDurationMs / 2L)) / totalDurationMs;
            }
            return activePrice;
        }

        long priceAtOrBefore(long targetTimestampMs, long maxStalenessMs) {
            for (int i = len - 1; i >= 0; i--) {
                int idx = (head + i) & MASK;
                long ts = timestampsMs[idx];
                if (ts <= targetTimestampMs) {
                    return (targetTimestampMs - ts <= maxStalenessMs) ? pricesCents[idx] : 0L;
                }
            }
            return 0L;
        }
    }

    /**
     * Zero-allocation {@code 500 ms}-bucketed circular buffer storing recent multi-exchange
     * {@code onCalculatedSpotTwap} spot observations for slow basis alignment and 4-second velocity checks.
     */
    static final class CalcSpotBuffer {
        private static final int CAPACITY = 32;
        private static final int MASK = CAPACITY - 1;
        private static final long BUCKET_MS = 500L;

        private final long[] timestampsMs = new long[CAPACITY];
        private final long[] pricesCents = new long[CAPACITY];
        private int head = 0;
        private int len = 0;

        void clear() {
            head = 0;
            len = 0;
        }

        void record(long nowMs, long spotCents, long minRetainMs) {
            while (len > 0 && timestampsMs[head] < minRetainMs) {
                head = (head + 1) & MASK;
                len--;
            }
            if (len > 0) {
                int lastIdx = (head + len - 1) & MASK;
                long lastBucketTs = timestampsMs[lastIdx];
                if (nowMs < lastBucketTs) {
                    return;
                }
                if (nowMs - lastBucketTs < BUCKET_MS) {
                    pricesCents[lastIdx] = spotCents;
                    return;
                }
            }
            if (len == CAPACITY) {
                head = (head + 1) & MASK;
                len--;
            }
            int tail = (head + len) & MASK;
            timestampsMs[tail] = nowMs;
            pricesCents[tail] = spotCents;
            len++;
        }

        long priceAtOrBefore(long targetTimestampMs, long maxStalenessMs) {
            for (int i = len - 1; i >= 0; i--) {
                int idx = (head + i) & MASK;
                long ts = timestampsMs[idx];
                if (ts <= targetTimestampMs) {
                    return (targetTimestampMs - ts <= maxStalenessMs) ? pricesCents[idx] : 0L;
                }
            }
            return 0L;
        }
    }

    /**
     * Multi-horizon zero-allocation volatility estimator clocked on oracle observation timestamps
     * ({@code priceTimestampMs}) and combining:
     * <ul>
     *   <li>{@code 1s} burst MAD ({@code 16} samples, ~16s) for immediate jump response,</li>
     *   <li>{@code 1s} short-term MAD ({@code 64} samples, ~64s) for baseline diffusion scale,</li>
     *   <li>{@code 5s} intra-candle regime MAD ({@code 128} samples, ~10.7m) across short lulls, and</li>
     *   <li>intra-candle pre-settlement {@code 14m} Parkinson high-low range floor to capture autocorrelated wave volatility.</li>
     * </ul>
     */
    static final class SpotVolatilityBuffer {
        private static final int BURST_CAPACITY = 16;
        private static final int BURST_MASK = BURST_CAPACITY - 1;
        private static final int SHORT_1S_CAPACITY = 64;
        private static final int SHORT_1S_MASK = SHORT_1S_CAPACITY - 1;
        private static final int REGIME_5S_CAPACITY = 128;
        private static final int REGIME_5S_MASK = REGIME_5S_CAPACITY - 1;
        private static final long MAX_DIFF_CLAMP_CENTS = 500_000L;
        private static final long MIN_PARKINSON_SPAN_MS = 180_000L;
        private static final long MIN_WAVE_PARKINSON_SIGMA_CENTS = 220L;
        private static final long EXTREME_WAVE_PARKINSON_SIGMA_CENTS = 340L;
        private static final long CALM_TAIL_MAX_MAD_1S_CENTS = 112L;
        private static final long CALM_TAIL_MAX_REGIME_RATIO_PCT = 65L;
        private static final double PARKINSON_RANGE_DENOM_COEFF = 1.595769;

        private final long[] burst1sNormDiffs = new long[BURST_CAPACITY];
        private int burstHead = 0;
        private int burstLen = 0;
        private long runningBurstSum = 0L;

        private final long[] short1sNormDiffs = new long[SHORT_1S_CAPACITY];
        private int short1sHead = 0;
        private int short1sLen = 0;
        private long runningShort1sSum = 0L;
        private long lastTickPriceCents = 0L;
        private long lastTickMs = 0L;

        private final long[] regime5sNormDiffs = new long[REGIME_5S_CAPACITY];
        private int regime5sHead = 0;
        private int regime5sLen = 0;
        private long runningRegime5sSum = 0L;
        private long anchor5sPriceCents = 0L;
        private long anchor5sMs = 0L;

        private long preWindowStartMs = 0L;
        private long preWindowEndMs = 0L;
        private long preWindowHighCents = 0L;
        private long preWindowLowCents = 0L;
        private boolean lastCalmTailDamped = false;

        void clear() {
            burstHead = 0;
            burstLen = 0;
            runningBurstSum = 0L;
            short1sHead = 0;
            short1sLen = 0;
            runningShort1sSum = 0L;
            lastTickPriceCents = 0L;
            lastTickMs = 0L;
            regime5sHead = 0;
            regime5sLen = 0;
            runningRegime5sSum = 0L;
            anchor5sPriceCents = 0L;
            anchor5sMs = 0L;
            resetCandleRange();
        }

        void resetCandleRange() {
            preWindowStartMs = 0L;
            preWindowEndMs = 0L;
            preWindowHighCents = 0L;
            preWindowLowCents = 0L;
            lastCalmTailDamped = false;
        }

        void onTick(long obsMs, long priceCents, boolean inPreSettlementCandle) {
            if (inPreSettlementCandle && obsMs > 0L) {
                if (preWindowStartMs <= 0L || obsMs < preWindowStartMs) {
                    preWindowStartMs = obsMs;
                }
                if (obsMs > preWindowEndMs) {
                    preWindowEndMs = obsMs;
                }
                if (priceCents > preWindowHighCents) {
                    preWindowHighCents = priceCents;
                }
                if (preWindowLowCents <= 0L || priceCents < preWindowLowCents) {
                    preWindowLowCents = priceCents;
                }
            }

            if (lastTickPriceCents <= 0L || obsMs < lastTickMs) {
                lastTickPriceCents = priceCents;
                lastTickMs = obsMs;
                anchor5sPriceCents = priceCents;
                anchor5sMs = obsMs;
                return;
            }

            long dtMs = obsMs - lastTickMs;
            if (dtMs >= 750L) {
                if (dtMs <= 3_000L) {
                    long absDiff = Math.min(MAX_DIFF_CLAMP_CENTS, Math.abs(priceCents - lastTickPriceCents));
                    long norm1sDiff = Math.round(absDiff * Math.sqrt(1_000.0 / dtMs));

                    if (burstLen == BURST_CAPACITY) {
                        runningBurstSum -= burst1sNormDiffs[burstHead];
                        burstHead = (burstHead + 1) & BURST_MASK;
                        burstLen--;
                    }
                    int bTail = (burstHead + burstLen) & BURST_MASK;
                    burst1sNormDiffs[bTail] = norm1sDiff;
                    runningBurstSum += norm1sDiff;
                    burstLen++;

                    if (short1sLen == SHORT_1S_CAPACITY) {
                        runningShort1sSum -= short1sNormDiffs[short1sHead];
                        short1sHead = (short1sHead + 1) & SHORT_1S_MASK;
                        short1sLen--;
                    }
                    int sTail = (short1sHead + short1sLen) & SHORT_1S_MASK;
                    short1sNormDiffs[sTail] = norm1sDiff;
                    runningShort1sSum += norm1sDiff;
                    short1sLen++;
                }
                lastTickPriceCents = priceCents;
                lastTickMs = obsMs;
            }

            long dt5sMs = obsMs - anchor5sMs;
            if (dt5sMs >= 4_500L) {
                if (dt5sMs <= 10_000L) {
                    long absDiff5s = Math.min(MAX_DIFF_CLAMP_CENTS, Math.abs(priceCents - anchor5sPriceCents));
                    long norm5sDiff = Math.round(absDiff5s * Math.sqrt(1_000.0 / dt5sMs));

                    if (regime5sLen == REGIME_5S_CAPACITY) {
                        runningRegime5sSum -= regime5sNormDiffs[regime5sHead];
                        regime5sHead = (regime5sHead + 1) & REGIME_5S_MASK;
                        regime5sLen--;
                    }
                    int rTail = (regime5sHead + regime5sLen) & REGIME_5S_MASK;
                    regime5sNormDiffs[rTail] = norm5sDiff;
                    runningRegime5sSum += norm5sDiff;
                    regime5sLen++;
                }
                anchor5sPriceCents = priceCents;
                anchor5sMs = obsMs;
            }
        }

        long sigma1sCents(UtterParameciumStrategyConfig cfg) {
            lastCalmTailDamped = false;
            boolean hasParkinsonRange = preWindowHighCents > preWindowLowCents
                    && (preWindowEndMs - preWindowStartMs) >= MIN_PARKINSON_SPAN_MS;
            if (short1sLen < 5 && regime5sLen < 6 && !hasParkinsonRange) {
                return cfg.defaultSigma1sCents();
            }
            long mad1s = short1sLen >= 5
                    ? (runningShort1sSum * 1_253L) / (short1sLen * 1_000L)
                    : cfg.defaultSigma1sCents();
            long rawSigma = mad1s;
            long madRegime5s = 0L;
            if (regime5sLen >= 6) {
                madRegime5s = (runningRegime5sSum * 1_253L) / (regime5sLen * 1_000L);
                long blendedRegime = (mad1s * 35L + madRegime5s * 65L) / 100L;
                rawSigma = Math.max(rawSigma, blendedRegime);
            }
            long madBurst = 0L;
            if (burstLen >= 5) {
                madBurst = (runningBurstSum * 1_253L) / (burstLen * 1_000L);
                long blendedBurst = (mad1s + madBurst) / 2L;
                rawSigma = Math.max(rawSigma, blendedBurst);
            }
            if (hasParkinsonRange) {
                long rangeCents = Math.min(MAX_DIFF_CLAMP_CENTS, preWindowHighCents - preWindowLowCents);
                double spanSec = (preWindowEndMs - preWindowStartMs) / 1_000.0;
                long parkinsonSigma1s = Math.round(rangeCents / (PARKINSON_RANGE_DENOM_COEFF * Math.sqrt(spanSec)));
                if (parkinsonSigma1s >= MIN_WAVE_PARKINSON_SIGMA_CENTS) {
                    boolean calmTailModerateWave = parkinsonSigma1s < EXTREME_WAVE_PARKINSON_SIGMA_CENTS
                            && mad1s <= CALM_TAIL_MAX_MAD_1S_CENTS
                            && (burstLen < 5 || madBurst <= CALM_TAIL_MAX_MAD_1S_CENTS)
                            && Math.max(mad1s, madRegime5s) * 100L < parkinsonSigma1s * CALM_TAIL_MAX_REGIME_RATIO_PCT;
                    lastCalmTailDamped = calmTailModerateWave;
                    long blendedRangeSigma = calmTailModerateWave
                            ? (mad1s * 65L + parkinsonSigma1s * 35L) / 100L
                            : (mad1s * 55L + parkinsonSigma1s * 45L) / 100L;
                    rawSigma = Math.max(rawSigma, blendedRangeSigma);
                }
            }
            return Math.clamp(rawSigma, cfg.minSigma1sCents(), cfg.maxSigma1sCents());
        }
    }

    /**
     * Sweep-proof 1-second-bucketed zero-allocation circular buffer tracking sustained CLOB BBO levels.
     * <p>
     * Within each {@code 1000 ms} bucket, stores the <em>minimum</em> observed bid and ask so transient
     * top-of-book ask spikes caused by liquidity sweeps never create false crash signals.
     */
    static final class RecentQuoteBuffer {
        private static final int CAPACITY = 16;
        private static final int MASK = CAPACITY - 1;
        private static final long BUCKET_MS = 1_000L;

        private final long[] timestampsMs = new long[CAPACITY];
        private final int[] bidUpMcs = new int[CAPACITY];
        private final int[] askUpMcs = new int[CAPACITY];
        private final int[] bidDownMcs = new int[CAPACITY];
        private final int[] askDownMcs = new int[CAPACITY];
        private int head = 0;
        private int len = 0;

        void clear() {
            head = 0;
            len = 0;
        }

        void record(long nowMs, int bidUpMc, int askUpMc, int bidDownMc, int askDownMc, long minRetainMs) {
            while (len > 0 && timestampsMs[head] < minRetainMs) {
                head = (head + 1) & MASK;
                len--;
            }
            if (len > 0) {
                int lastIdx = (head + len - 1) & MASK;
                long lastTs = timestampsMs[lastIdx];
                if (nowMs < lastTs) {
                    return;
                }
                if (nowMs - lastTs < BUCKET_MS) {
                    bidUpMcs[lastIdx] = Math.min(bidUpMcs[lastIdx], bidUpMc);
                    askUpMcs[lastIdx] = Math.min(askUpMcs[lastIdx], askUpMc);
                    bidDownMcs[lastIdx] = Math.min(bidDownMcs[lastIdx], bidDownMc);
                    askDownMcs[lastIdx] = Math.min(askDownMcs[lastIdx], askDownMc);
                    return;
                }
            }
            if (len == CAPACITY) {
                head = (head + 1) & MASK;
                len--;
            }
            int tail = (head + len) & MASK;
            timestampsMs[tail] = nowMs;
            bidUpMcs[tail] = bidUpMc;
            askUpMcs[tail] = askUpMc;
            bidDownMcs[tail] = bidDownMc;
            askDownMcs[tail] = askDownMc;
            len++;
        }

        int maxSustainedBid(boolean isUp, long minTimestampMs) {
            int maxMc = 0;
            for (int i = 0; i < len; i++) {
                int idx = (head + i) & MASK;
                if (timestampsMs[idx] >= minTimestampMs) {
                    int candidate = isUp ? bidUpMcs[idx] : bidDownMcs[idx];
                    if (candidate > maxMc) {
                        maxMc = candidate;
                    }
                }
            }
            return maxMc;
        }

        int maxSustainedAsk(boolean isUp, long minTimestampMs) {
            int maxMc = 0;
            for (int i = 0; i < len; i++) {
                int idx = (head + i) & MASK;
                if (timestampsMs[idx] >= minTimestampMs) {
                    int candidate = isUp ? askUpMcs[idx] : askDownMcs[idx];
                    if (candidate > maxMc) {
                        maxMc = candidate;
                    }
                }
            }
            return maxMc;
        }
    }
}
