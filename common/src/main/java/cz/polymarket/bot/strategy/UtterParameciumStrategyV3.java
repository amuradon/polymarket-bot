package cz.polymarket.bot.strategy;

import cz.polymarket.bot.domain.MarketCandle;
import cz.polymarket.bot.domain.OrderBookQuote;
import cz.polymarket.bot.domain.Timeframe;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.math.BigDecimal;
import java.util.Set;

/**
 * Historical Iteration 3 snapshot ({@code V3}, commit {@code 38ae0f1}) of {@code UtterParameciumStrategy}.
 * <p>
 * Introduces oracle-observation-clocked volatility estimation with intra-candle 14m Parkinson wave-range floor,
 * smooth Z-score curve across {@code $0.67..$0.84}, pre-lock-in anti-stagnation conviction sizing, tightened
 * supplementary {@link #onCalculatedSpotTwap} early-reversal veto, and intra-settlement-window whipsaw protection.
 */
@ApplicationScoped
public class UtterParameciumStrategyV3 implements TradingStrategy {

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
    private static final long BUFFER_RETENTION_MS = 65_000L;
    private static final long RECENT_QUOTE_WINDOW_MS = 8_000L;
    private static final long MAX_ORACLE_LAG_CLAMP_MS = 4_000L;
    private static final long PRE_ACTIVATION_BALANCE_SYNC_LEAD_MS = 12_000L;
    private static final long LATE_LOCKIN_MAX_TAU_MS = 25_000L;
    private static final long LATE_LOCKIN_MIN_PROJ_DIST_CENTS = 1_200L;
    private static final long LATE_LOCKIN_MIN_NET_DIST_CENTS = 600L;
    private static final int HIGH_PRICE_ASK_THRESHOLD_MC = 800;
    private static final long HIGH_PRICE_MAX_TAU_MS = 38_000L;

    private static final long CALC_SPOT_RETENTION_MS = 15_000L;
    private static final long MAX_CALC_FRESHNESS_MS = 2_500L;
    private static final int MIN_CALC_BASIS_SAMPLES = 3;
    private static final long CALC_BASIS_EMA_SCALE = 16L;
    private static final long MAX_CALC_BASIS_INNOVATION_CENTS = 1_500L;
    private static final long CALC_NOISE_DEADBAND_CENTS = 320L;

    private static final int CORE_CHEAP_ASK_CEILING_MC = 670;
    private static final int TRANSITION_ASK_CEILING_MC = 720;
    private static final int MID_ASK_CEILING_MC = 760;
    private static final int EXPENSIVE_ASK_CEILING_MC = 800;
    private static final long BONUS_CONVICTION_Z_CENTI = 145L;
    private static final int BONUS_CONVICTION_EDGE_MC = 55;
    private static final long BONUS_CONVICTION_MAX_TAU_MS = 35_000L;
    private static final long TRANSITION_UNLOCK_MAX_TAU_MS = 32_000L;
    private static final long TRANSITION_UNLOCK_MIN_Z_CENTI = 135L;
    private static final long PRE_LOCKIN_CONVICTION_MAX_TAU_MS = 38_000L;
    private static final long PRE_LOCKIN_CONVICTION_LOW_Z_CENTI = 55L;
    private static final long PRE_LOCKIN_CONVICTION_FULL_Z_CENTI = 85L;
    private static final long PRE_LOCKIN_MIN_CONVICTION_BPS = 4_000L;
    private static final long LOCKED_DEBT_EARLY_CUTOFF_TAU_MS = 30_000L;
    private static final long LOCKED_DEBT_EARLY_DEFICIT_CENTS = 350L;
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

    private final Config config;
    private final ChainlinkTwapBuffer chainlinkTwapBuffer = new ChainlinkTwapBuffer();
    private final SpotVolatilityBuffer spotVolatilityBuffer = new SpotVolatilityBuffer();
    private final RecentQuoteBuffer recentQuoteBuffer = new RecentQuoteBuffer();
    private final CalcSpotBuffer calcSpotBuffer = new CalcSpotBuffer();

    private StrategyContext context;

    private long activeCandleStartMs = 0L;
    private long activeCandleEndMs = 0L;
    private long targetPriceCents = 0L;
    private long nextCandleStartMs = 0L;
    private long nextCandleTargetPriceCents = 0L;
    private boolean missingTargetPriceWarned = false;

    private long lastChainlinkPriceCents = 0L;
    private long lastChainlinkTickMs = 0L;
    private long lastChainlinkPriceObsMs = 0L;

    private long lastCalcSpotCents = 0L;
    private long lastCalcTickMs = 0L;
    private long calcBasisEmaScaled = 0L;
    private int calcBasisSamples = 0;

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
     * Constructs the V3 Iteration 3 strategy snapshot and unwraps any CDI client proxy on {@link Config}.
     *
     * @param config isolated V3 configuration bean
     */
    @Inject
    public UtterParameciumStrategyV3(Config config) {
        if (config == null) {
            throw new IllegalArgumentException("config cannot be null");
        }
        this.config = config.getClass() == Config.class
                ? config
                : new Config(
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
     * @param zCenti Z-score in hundredths of a standard deviation
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
     * Computes the V3 continuous piecewise-linear minimum Z-score (in centi-sigma) required to buy an option at {@code askMc}.
     *
     * @param askMc candidate ask or limit price in millicents
     * @return required minimum Z-score in centi-sigma ({@code 0} below {@code 670 mc})
     */
    public static long requiredMinZCenti(int askMc) {
        if (askMc < CORE_CHEAP_ASK_CEILING_MC) {
            return 0L;
        }
        if (askMc <= TRANSITION_ASK_CEILING_MC) {
            return 70L + ((long) (askMc - CORE_CHEAP_ASK_CEILING_MC) * 45L) / 50L;
        }
        if (askMc <= MID_ASK_CEILING_MC) {
            return 115L + ((long) (askMc - TRANSITION_ASK_CEILING_MC) * 55L) / 40L;
        }
        if (askMc <= EXPENSIVE_ASK_CEILING_MC) {
            return 170L + ((long) (askMc - MID_ASK_CEILING_MC) * 80L) / 40L;
        }
        if (askMc <= 850) {
            return 250L + ((long) (askMc - EXPENSIVE_ASK_CEILING_MC) * 30L) / 50L;
        }
        return 280L;
    }

    /**
     * Computes the minimum required BTC price cushion over target in USD cents as a function of {@code tauMs}.
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
     * Rounds a sub-cent price in millicents down to the nearest valid 10-mc ($0.01) tick.
     */
    public static int fromMcFloorTick(int millicents) {
        if (millicents < 0 || millicents > MAX_MC) {
            throw new IllegalArgumentException("Option price out of bounds [0, 1000]: " + millicents);
        }
        return (millicents / TICK_STEP_MC) * TICK_STEP_MC;
    }

    /**
     * Rounds a sub-cent price in millicents up to the nearest valid 10-mc ($0.01) tick.
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
     * Computes the parabolic Polymarket taker fee per share in millicents.
     */
    public static int takerFeeMc(int askMc, int feeRateBps) {
        if (askMc <= 0 || askMc >= MAX_MC || feeRateBps <= 0) {
            return 0;
        }
        return (int) (((long) feeRateBps * askMc * (MAX_MC - askMc) + 5_000_000L) / 10_000_000L);
    }

    @Override
    public String getName() {
        return "UtterParameciumStrategyV3";
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
        return new UtterParameciumStrategyV3(config);
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
        if (tauMs < 0L || tauMs > TWAP_WINDOW_MS + 10_000L) {
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
     * Computes the V3 dynamic cumulative inventory cap combining TWAP lock-in progression, price tier risk,
     * and pre-lock-in anti-stagnation conviction scaling.
     */
    int computeDynamicMaxInventory(long tauMs, int askMc, long zSideCenti, int edgeMc) {
        int timeStagedCap = computeTimeStagedMaxInventory(tauMs);
        long riskTierBps;
        if (askMc <= CORE_CHEAP_ASK_CEILING_MC) {
            if (tauMs <= BONUS_CONVICTION_MAX_TAU_MS
                    && zSideCenti >= BONUS_CONVICTION_Z_CENTI
                    && edgeMc >= BONUS_CONVICTION_EDGE_MC) {
                riskTierBps = 12_000L;
            } else {
                riskTierBps = 10_000L;
            }
        } else if (askMc <= TRANSITION_ASK_CEILING_MC) {
            if (tauMs <= TRANSITION_UNLOCK_MAX_TAU_MS && zSideCenti >= TRANSITION_UNLOCK_MIN_Z_CENTI) {
                riskTierBps = 9_500L;
            } else {
                riskTierBps = 7_500L;
            }
        } else if (askMc <= MID_ASK_CEILING_MC) {
            riskTierBps = 6_000L;
        } else if (askMc < EXPENSIVE_ASK_CEILING_MC) {
            riskTierBps = 3_800L;
        } else {
            riskTierBps = 2_800L;
        }

        long convictionBps = 10_000L;
        if (tauMs > PRE_LOCKIN_CONVICTION_MAX_TAU_MS && zSideCenti < PRE_LOCKIN_CONVICTION_FULL_Z_CENTI) {
            if (zSideCenti <= PRE_LOCKIN_CONVICTION_LOW_Z_CENTI) {
                convictionBps = PRE_LOCKIN_MIN_CONVICTION_BPS;
            } else {
                long zSpan = PRE_LOCKIN_CONVICTION_FULL_Z_CENTI - PRE_LOCKIN_CONVICTION_LOW_Z_CENTI;
                convictionBps = PRE_LOCKIN_MIN_CONVICTION_BPS
                        + ((zSideCenti - PRE_LOCKIN_CONVICTION_LOW_Z_CENTI) * (10_000L - PRE_LOCKIN_MIN_CONVICTION_BPS)) / zSpan;
            }
        }

        long combinedBps = (riskTierBps * convictionBps) / 10_000L;
        int dynamicCap = (int) (((long) timeStagedCap * combinedBps) / 10_000L);
        return Math.max(config.minOrderShares(), dynamicCap);
    }

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
        int effectiveTauSec = (int) Math.clamp((tauMs + oracleLagMs + 500L) / 1000L, 0L, 61L);
        long weightBps = TWAP_REM_WEIGHT_BPS[effectiveTauSec];
        long sigmaRemCents = Math.max(MIN_SIGMA_REM_CENTS, (sigma1sCents * weightBps) / 10_000L);

        long zCenti = (100L * (baseProjectedTwapCents - targetPriceCents)) / sigmaRemCents;
        this.lastModelProbUpMc = normalCdfMc(zCenti);

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
            long alignedCalcSpotCents = lastCalcSpotCents - roundedBasisCents(calcBasisEmaScaled);
            long calcLeadUpCents = alignedCalcSpotCents - spotCents;
            int leadWeightBps = config.calcLeadWeightBps();
            if (calcLeadUpCents < 0L) {
                adverseCalcLeadUpCents = -calcLeadUpCents;
                long excessLeadCents = Math.max(0L, adverseCalcLeadUpCents - CALC_NOISE_DEADBAND_CENTS);
                effectiveSpotUpCents = spotCents - (excessLeadCents * leadWeightBps) / 10_000L;
            } else if (calcLeadUpCents > 0L) {
                adverseCalcLeadDownCents = calcLeadUpCents;
                long excessLeadCents = Math.max(0L, adverseCalcLeadDownCents - CALC_NOISE_DEADBAND_CENTS);
                effectiveSpotDownCents = spotCents + (excessLeadCents * leadWeightBps) / 10_000L;
            }
            long calcPrice4sAgoCents = calcSpotBuffer.priceAtOrBefore(nowMs - 4_000L, MAX_CALC_FRESHNESS_MS);
            if (calcPrice4sAgoCents > 0L) {
                calcUpDrift4sCents = lastCalcSpotCents - calcPrice4sAgoCents;
            }
        }
        long projectedTwapUpCents = projectTwapCents(tauMs, lockedMeanCents, effectiveSpotUpCents);
        long projectedTwapDownCents = projectTwapCents(tauMs, lockedMeanCents, effectiveSpotDownCents);

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
        boolean upTrulyLockedIn = fullWindowAnchored
                && tauMs <= LATE_LOCKIN_MAX_TAU_MS
                && (elapsedWindowMs * lockedDistUpCents) / TWAP_WINDOW_MS >= LATE_LOCKIN_MIN_PROJ_DIST_CENTS;
        boolean downTrulyLockedIn = fullWindowAnchored
                && tauMs <= LATE_LOCKIN_MAX_TAU_MS
                && (elapsedWindowMs * lockedDistDownCents) / TWAP_WINDOW_MS >= LATE_LOCKIN_MIN_PROJ_DIST_CENTS;

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
                        upTrulyLockedIn,
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
                        downTrulyLockedIn,
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
        boolean chosenTrulyLockedIn = isUpChosen ? upTrulyLockedIn : downTrulyLockedIn;
        long chosenOppositeExcursionCents = isUpChosen ? oppositeExcursionUpCents : oppositeExcursionDownCents;
        long chosenDistProjCents = isUpChosen ? upDistProjCents : downDistProjCents;
        long chosenDistSpotCents = isUpChosen ? upDistSpotCents : downDistSpotCents;
        long chosenEffectiveDistCents = computeEffectiveDistCents(chosenDistProjCents, chosenDistSpotCents, tauMs);
        if (chosenEffectiveDistCents <= 0L) {
            return;
        }

        int currentSideShares = isUpChosen ? netSharesUp : netSharesDown;
        if (!chosenTrulyLockedIn) {
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

        long timeFactorBps = Math.clamp(((90_000L - tauMs) * 10_000L) / 60_000L, 7_000L, 11_500L);
        long scaled = ((long) config.edgeScalingFactor() * (chosenEdgeMc - config.minEdgeMc()) * timeFactorBps)
                / (10_000L * 100L);
        int qRaw = (int) Math.clamp(config.minOrderShares() + scaled, config.minOrderShares(), config.maxOrderShares());

        long chosenZSideCenti = (100L * chosenEffectiveDistCents) / sigmaRemCents;
        int dynamicCapShares = computeDynamicMaxInventory(tauMs, chosenAskMc, chosenZSideCenti, chosenEdgeMc);
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
                chosenTrulyLockedIn,
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
        if (tauMs > LOCKED_DEBT_EARLY_CUTOFF_TAU_MS) {
            return lockedDistSideCents <= -LOCKED_DEBT_EARLY_DEFICIT_CENTS;
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

    private static long computeEffectiveDistCents(long distProjCents, long distSpotCents, long tauMs) {
        if (distProjCents <= 0L) {
            return -1L;
        }
        if (distSpotCents > 0L) {
            return Math.min(distProjCents, distSpotCents);
        }
        if (tauMs > LATE_LOCKIN_MAX_TAU_MS
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
            boolean trulyLockedIn,
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
        if (askMc >= HIGH_PRICE_ASK_THRESHOLD_MC && tauMs > HIGH_PRICE_MAX_TAU_MS) {
            return Integer.MIN_VALUE;
        }

        if (!trulyLockedIn) {
            long block10sCents = config.adverseDrift10sBlockCents();
            if (block10sCents > 0L) {
                long block5sCents = (block10sCents * 5L) / 8L;
                if (adverseDrift5sCents >= block5sCents || adverseDrift10sCents >= block10sCents) {
                    return Integer.MIN_VALUE;
                }
            }
            long calcLeadBlockCents = config.calcAdverseLeadBlockCents();
            if (calcLeadBlockCents > 0L && adverseCalcLeadCents >= calcLeadBlockCents) {
                return Integer.MIN_VALUE;
            }
            long calcDrift4sBlockCents = config.calcAdverseDrift4sBlockCents();
            if (calcDrift4sBlockCents > 0L && adverseCalcDrift4sCents >= calcDrift4sBlockCents) {
                return Integer.MIN_VALUE;
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

        long effectiveDistCents = computeEffectiveDistCents(distProjCents, distSpotCents, tauMs);
        if (effectiveDistCents < minEffectiveDistCents(tauMs)) {
            return Integer.MIN_VALUE;
        }
        long zSideCenti = (100L * effectiveDistCents) / sigmaRemCents;
        if (!trulyLockedIn && oppositeWindowExcursionCents >= WHIPSAW_OPPOSITE_EXCURSION_CENTS) {
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

    private int computeEdgePreservingLimitPriceMc(
            int currentBidMc,
            int askMc,
            int maxRecentBidMc,
            int maxRecentAskMc,
            boolean trulyLockedIn,
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
                    trulyLockedIn,
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
        String clientOrderId = "up-v3-" + (activeCandleStartMs / 1000L) + "-" + (++orderSequence);
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

    /** Returns the explicit target price in USD cents determined from PolyBolt 60s TWAP at market start. */
    public long getTargetPriceCents() {
        return targetPriceCents;
    }

    /** Returns whether the active market was skipped due to a missing target price. */
    public boolean isMissingTargetPriceWarned() {
        return missingTargetPriceWarned;
    }

    /** Returns the last observed PolyBolt {@code price.crypto} price in USD cents. */
    public long getLastChainlinkPriceCents() {
        return lastChainlinkPriceCents;
    }

    /** Returns the accumulated UP shares in the active 15m market. */
    public int getNetSharesUp() {
        return netSharesUp;
    }

    /** Returns the accumulated DOWN shares in the active 15m market. */
    public int getNetSharesDown() {
        return netSharesDown;
    }

    /** Returns the number of filled batch trades in the active 15m market. */
    public int getTradesInMarket() {
        return tradesInMarket;
    }

    /** Returns the most recently evaluated UP model probability in millicents ({@code 1..999}). */
    public int getLastModelProbUpMc() {
        return lastModelProbUpMc;
    }

    /** Returns the isolated V3 strategy configuration bean. */
    public Config getConfig() {
        return config;
    }

    /**
     * Isolated configuration bean for {@link UtterParameciumStrategyV3}.
     */
    @ApplicationScoped
    public static class Config {

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
         * Constructs the isolated V3 configuration with MicroProfile properties.
         */
        @Inject
        public Config(
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v3.min-tau-ms", defaultValue = "5000") long minTauMs,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v3.max-tau-ms", defaultValue = "60000") long maxTauMs,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v3.cooldown-ms", defaultValue = "350") long cooldownMs,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v3.min-entry-price-mc", defaultValue = "380") int minEntryPriceMc,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v3.max-entry-price-mc", defaultValue = "840") int maxEntryPriceMc,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v3.max-spread-mc", defaultValue = "80") int maxSpreadMc,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v3.min-model-prob-mc", defaultValue = "650") int minModelProbMc,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v3.max-model-prob-mc", defaultValue = "975") int maxModelProbMc,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v3.min-edge-mc", defaultValue = "25") int minEdgeMc,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v3.max-slippage-mc", defaultValue = "50") int maxSlippageMc,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v3.min-order-shares", defaultValue = "5") int minOrderShares,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v3.max-order-shares", defaultValue = "30") int maxOrderShares,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v3.edge-scaling-factor", defaultValue = "65") int edgeScalingFactor,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v3.max-inventory-shares-per-side", defaultValue = "600") int maxInventorySharesPerSide,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v3.early-inventory-cap-bps", defaultValue = "5000") int earlyInventoryCapBps,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v3.full-inventory-tau-ms", defaultValue = "36000") long fullInventoryTauMs,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v3.allow-direction-flip", defaultValue = "false") boolean allowDirectionFlip,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v3.adverse-drift-10s-block-cents", defaultValue = "950") long adverseDrift10sBlockCents,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v3.adverse-ask-drop-8s-block-mc", defaultValue = "100") int adverseAskDrop8sBlockMc,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v3.calc-lead-weight-bps", defaultValue = "7000") int calcLeadWeightBps,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v3.calc-adverse-lead-block-cents", defaultValue = "650") long calcAdverseLeadBlockCents,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v3.calc-adverse-drift-4s-block-cents", defaultValue = "950") long calcAdverseDrift4sBlockCents,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v3.default-sigma-1s-cents", defaultValue = "160") long defaultSigma1sCents,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v3.min-sigma-1s-cents", defaultValue = "105") long minSigma1sCents,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v3.max-sigma-1s-cents", defaultValue = "3500") long maxSigma1sCents,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v3.warmup-secs-before-expiry", defaultValue = "120") long warmupSecsBeforeExpiry,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v3.taker-fee-rate-bps", defaultValue = "700") int takerFeeRateBps,
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
         * Creates a default configuration calibrated to the historical V3 (Iteration 3) parameters.
         *
         * @return default V3 configuration
         */
        public static Config defaultConfig() {
            return new Config(
                    5_000L,
                    60_000L,
                    350L,
                    380,
                    840,
                    80,
                    650,
                    975,
                    25,
                    50,
                    5,
                    30,
                    65,
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

        /** Minimum remaining time to candle expiry in milliseconds. */
        public long minTauMs() {
            return minTauMs;
        }

        /** Maximum remaining time to candle expiry in milliseconds. */
        public long maxTauMs() {
            return maxTauMs;
        }

        /** Minimum interval between consecutive signals in milliseconds. */
        public long cooldownMs() {
            return cooldownMs;
        }

        /** Minimum acceptable ask price in millicents. */
        public int minEntryPriceMc() {
            return minEntryPriceMc;
        }

        /** Maximum acceptable ask price in millicents. */
        public int maxEntryPriceMc() {
            return maxEntryPriceMc;
        }

        /** Maximum allowed BBO spread in millicents. */
        public int maxSpreadMc() {
            return maxSpreadMc;
        }

        /** Minimum model probability in millicents. */
        public int minModelProbMc() {
            return minModelProbMc;
        }

        /** Maximum fat-tail model probability cap in millicents. */
        public int maxModelProbMc() {
            return maxModelProbMc;
        }

        /** Minimum net price edge after taker fee in millicents. */
        public int minEdgeMc() {
            return minEdgeMc;
        }

        /** Maximum allowed limit price slippage above best ask in millicents. */
        public int maxSlippageMc() {
            return maxSlippageMc;
        }

        /** Base minimum order size in integer shares. */
        public int minOrderShares() {
            return minOrderShares;
        }

        /** Maximum single-order size in integer shares. */
        public int maxOrderShares() {
            return maxOrderShares;
        }

        /** Edge scaling coefficient for order sizing. */
        public int edgeScalingFactor() {
            return edgeScalingFactor;
        }

        /** Base maximum cumulative shares per side in a single 15m market. */
        public int maxInventorySharesPerSide() {
            return maxInventorySharesPerSide;
        }

        /** Early-window fraction of maximum inventory allowed at {@code tau >= 60s} in basis points. */
        public int earlyInventoryCapBps() {
            return earlyInventoryCapBps;
        }

        /** Remaining time in milliseconds at or below which 100% of time-staged inventory is unlocked. */
        public long fullInventoryTauMs() {
            return fullInventoryTauMs;
        }

        /** Whether opposite-direction entries are allowed once a position is held in the active market. */
        public boolean allowDirectionFlip() {
            return allowDirectionFlip;
        }

        /** Adverse 10-second Chainlink spot drift threshold in USD cents blocking pre-lock-in entries. */
        public long adverseDrift10sBlockCents() {
            return adverseDrift10sBlockCents;
        }

        /** Adverse 8-second sustained CLOB BBO drop in millicents blocking pre-lock-in entries. */
        public int adverseAskDrop8sBlockMc() {
            return adverseAskDrop8sBlockMc;
        }

        /** Conservative haircut weight applied to adverse basis-aligned {@code onCalculatedSpotTwap} divergence in bps. */
        public int calcLeadWeightBps() {
            return calcLeadWeightBps;
        }

        /** Adverse basis-aligned {@code onCalculatedSpotTwap} divergence in USD cents blocking non-locked-in entries. */
        public long calcAdverseLeadBlockCents() {
            return calcAdverseLeadBlockCents;
        }

        /** Adverse 4-second {@code onCalculatedSpotTwap} drift in USD cents blocking non-locked-in entries. */
        public long calcAdverseDrift4sBlockCents() {
            return calcAdverseDrift4sBlockCents;
        }

        /** Default 1-second BTC volatility in USD cents during warm-up. */
        public long defaultSigma1sCents() {
            return defaultSigma1sCents;
        }

        /** Minimum clamp for 1-second BTC volatility in USD cents. */
        public long minSigma1sCents() {
            return minSigma1sCents;
        }

        /** Maximum clamp for 1-second BTC volatility in USD cents. */
        public long maxSigma1sCents() {
            return maxSigma1sCents;
        }

        /** Warm-up window before expiry in seconds. */
        public long warmupSecsBeforeExpiry() {
            return warmupSecsBeforeExpiry;
        }

        /** Polymarket parabolic taker fee multiplier in basis points. */
        public int takerFeeRateBps() {
            return takerFeeRateBps;
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

        long sigma1sCents(Config cfg) {
            boolean hasParkinsonRange = preWindowHighCents > preWindowLowCents
                    && (preWindowEndMs - preWindowStartMs) >= MIN_PARKINSON_SPAN_MS;
            if (short1sLen < 5 && regime5sLen < 6 && !hasParkinsonRange) {
                return cfg.defaultSigma1sCents();
            }
            long mad1s = short1sLen >= 5
                    ? (runningShort1sSum * 1_253L) / (short1sLen * 1_000L)
                    : cfg.defaultSigma1sCents();
            long rawSigma = mad1s;
            if (regime5sLen >= 6) {
                long madRegime5s = (runningRegime5sSum * 1_253L) / (regime5sLen * 1_000L);
                long blendedRegime = (mad1s * 35L + madRegime5s * 65L) / 100L;
                rawSigma = Math.max(rawSigma, blendedRegime);
            }
            if (burstLen >= 5) {
                long madBurst = (runningBurstSum * 1_253L) / (burstLen * 1_000L);
                long blendedBurst = (mad1s + madBurst) / 2L;
                rawSigma = Math.max(rawSigma, blendedBurst);
            }
            if (hasParkinsonRange) {
                long rangeCents = Math.min(MAX_DIFF_CLAMP_CENTS, preWindowHighCents - preWindowLowCents);
                double spanSec = (preWindowEndMs - preWindowStartMs) / 1_000.0;
                long parkinsonSigma1s = Math.round(rangeCents / (PARKINSON_RANGE_DENOM_COEFF * Math.sqrt(spanSec)));
                if (parkinsonSigma1s >= MIN_WAVE_PARKINSON_SIGMA_CENTS) {
                    long blendedRangeSigma = (mad1s * 55L + parkinsonSigma1s * 45L) / 100L;
                    rawSigma = Math.max(rawSigma, blendedRangeSigma);
                }
            }
            return Math.clamp(rawSigma, cfg.minSigma1sCents(), cfg.maxSigma1sCents());
        }
    }

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
