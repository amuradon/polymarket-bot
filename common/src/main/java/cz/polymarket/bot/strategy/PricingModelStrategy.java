package cz.polymarket.bot.strategy;

import cz.polymarket.bot.domain.OrderBookQuote;
import cz.polymarket.bot.domain.Timeframe;
import cz.polymarket.bot.indicators.SpotTwapListener;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.math.BigDecimal;

/**
 * Low-latency pricing model strategy exploiting oracle-lag and CLOB market-maker repricing delays
 * on Polymarket binary Up/Down markets.
 * <p>
 * Combines four zero-allocation real-time components:
 * <ol>
 *   <li><b>Basis-Anchored Spot Nowcast</b>: Dynamically anchors high-frequency {@code SpotTwapIndicator}
 *       updates (50-100ms venue delay) to the slow venue-basis EMA observed against {@code PolyBolt}
 *       Chainlink {@code crypto.price} (~1,200ms publication lag), cutting 60s TWAP tracking error below $0.40.</li>
 *   <li><b>EWMA + Retracement-Adjusted Parkinson Volatility</b>: Blends two-horizon 1s return EWMA with a
 *       drift-adjusted Parkinson intra-candle range floor so genuine two-way whipsaws immediately raise
 *       remaining variance without penalizing smooth one-way directional trends.</li>
 *   <li><b>Bayesian Merton Jump-Diffusion 60s TWAP Model</b>: Combines continuous two-regime {@code Student-t (df=4)}
 *       diffusion with time-decaying exogenous jump-reset shrinkage prior to final-minute TWAP lock-in,
 *       while measuring fresh 1.25s impulses on unshrunk diffusive probabilities.</li>
 *   <li><b>Active Desynchronization & Exact Fractional Kelly Sizing</b>: Rejects high-whipsaw candles, scales
 *       position shares inversely with binary payoff variance {@code c * (1 - c)}, caps at 2 clips per market,
 *       and holds positions to settlement without averaging down.</li>
 * </ol>
 */
@ApplicationScoped
public class PricingModelStrategy implements TradingStrategy, SpotTwapListener {

    private static final int MAX_MC = 1_000;
    private static final int NEUTRAL_PROB_MC = 500;
    private static final long NEUTRAL_VARIANCE_MC_SQ = 250_000L;
    private static final int KELLY_EDGE_SLOPE_MC = 55;
    private static final int TICK_STEP_MC = 10;
    private static final long TWAP_WINDOW_MS = 60_000L;
    private static final long JUMP_HAZARD_ZERO_TAU_MS = 45_000L;
    private static final long JUMP_HAZARD_SPAN_MS = 255_000L;
    private static final long MAX_TARGET_OFFSET_MS = 10_000L;
    private static final double FOUR_LN_2 = 2.772588722239781;
    private static final double BPS_INV = 1e-4;
    private static final double BPS_SCALE = 10_000.0;
    private static final double CENTI_BPS_SCALE = 1_000_000.0;

    private static final int SPOT_RING_CAPACITY = 4_096;
    private static final int SPOT_RING_MASK = SPOT_RING_CAPACITY - 1;
    private static final int PROB_RING_CAPACITY = 2_048;
    private static final int PROB_RING_MASK = PROB_RING_CAPACITY - 1;
    private static final int MAX_CACHED_SHARES = 2_048;

    private static final BigDecimal[] PRICE_MC_CACHE = new BigDecimal[MAX_MC + 1];
    private static final BigDecimal[] SHARE_SIZE_CACHE = new BigDecimal[MAX_CACHED_SHARES + 1];

    static {
        for (int mc = 0; mc <= MAX_MC; mc++) {
            PRICE_MC_CACHE[mc] = BigDecimal.valueOf(mc, 3);
        }
        for (int s = 0; s <= MAX_CACHED_SHARES; s++) {
            SHARE_SIZE_CACHE[s] = BigDecimal.valueOf(s);
        }
    }

    private final PricingModelStrategyConfig config;
    private final long candleDurationMs;
    private final double alphaFast;
    private final double alphaSlow;
    private final double fastVarWeight;
    private final double slowVarWeight;
    private final double minSigma1sRatio;
    private final double maxSingleReturnSq;
    private final double parkinsonScale;
    private final double bridgeFloorSq;
    private final double maxJumpHazardRatio;
    private final double takerFeeRate;

    // Pre-allocated zero-GC circular buffers
    private final long[] spotRingTs = new long[SPOT_RING_CAPACITY];
    private final double[] spotRingPx = new double[SPOT_RING_CAPACITY];
    private int spotRingHead = 0;
    private int spotRingLen = 0;

    private final long[] probRingTs = new long[PROB_RING_CAPACITY];
    private final int[] probRingUpMc = new int[PROB_RING_CAPACITY];
    private int probRingHead = 0;
    private int probRingLen = 0;

    private StrategyContext context;

    // Active market lifecycle & target strike state
    private long activeCandleStartMs = 0L;
    private long activeCandleEndMs = 0L;
    private double targetTwapOpenUsd = 0.0;
    private long nextCandleStartMs = 0L;
    private double nextCandleTargetUsd = 0.0;
    private boolean missingTargetWarned = false;

    // Continuous underlying, basis, volatility, candle extrema & final-minute TWAP lock-in state
    private double lastCalcSpotUsd = Double.NaN;
    private long lastCalcSpotMs = 0L;
    private double lastPbSpotUsd = Double.NaN;
    private long lastPbSpotMs = 0L;
    private double relBasisEma = 0.0;
    private boolean hasBasisSample = false;

    private long lastVolSecond = -1L;
    private double prevVolPriceUsd = Double.NaN;
    private double varFast = (1.3e-4) * (1.3e-4);
    private double varSlow = (1.3e-4) * (1.3e-4);
    private int volSampleCount = 0;

    private double candleMinSHatUsd = Double.MAX_VALUE;
    private double candleMaxSHatUsd = 0.0;

    private double lockedTwapAreaUsdMs = 0.0;
    private long lockedAnchorStartMs = 0L;
    private long lastIntegratedTsMs = 0L;
    private double lastIntegratedSHatUsd = Double.NaN;
    private int latestRawProbUpMc = NEUTRAL_PROB_MC;
    private int latestModelProbUpMc = NEUTRAL_PROB_MC;

    // Latest valid CLOB quote snapshot
    private long lastQuoteTsMs = 0L;
    private int lastAskUpMc = 0;
    private int lastBidUpMc = 0;
    private int lastAskDownMc = 0;
    private int lastBidDownMc = 0;
    private int lastDepthUpSh = 0;
    private int lastDepthDownSh = 0;
    private int lastEstFillUpMc = 0;
    private int lastEstFillDownMc = 0;

    // Per-market execution & inventory state
    private TradeDirection marketDirection = TradeDirection.NO_TRADE;
    private int ordersFilledInMarket = 0;
    private int firstFilledAskMc = 0;
    private int filledSharesInMarket = 0;
    private long lastOrderSubmitMs = 0L;
    private boolean orderInFlight = false;
    private String pendingClientOrderId = null;
    private TradeDirection pendingDirection = TradeDirection.NO_TRADE;
    private int pendingRequestedShares = 0;
    private int pendingAskMc = 0;
    private boolean balanceRefreshedForMarket = false;
    private long orderSequence = 0L;

    /**
     * Constructs a {@link PricingModelStrategy} with default calibrated configuration.
     */
    public PricingModelStrategy() {
        this(PricingModelStrategyConfig.defaults());
    }

    /**
     * Constructs a {@link PricingModelStrategy} with the supplied configuration bean,
     * unwrapping any CDI proxy into primitive hot-path constants.
     *
     * @param config strategy configuration
     */
    @Inject
    public PricingModelStrategy(PricingModelStrategyConfig config) {
        PricingModelStrategyConfig unwrapped = PricingModelStrategyConfig.unwrap(config);
        this.config = unwrapped;
        this.candleDurationMs = unwrapped.timeframe().getDurationSeconds() * 1_000L;
        this.alphaFast = 1.0 - Math.exp(-1.0 / unwrapped.volFastTauSec());
        this.alphaSlow = 1.0 - Math.exp(-1.0 / unwrapped.volSlowTauSec());
        this.fastVarWeight = unwrapped.volFastWeight();
        this.slowVarWeight = 1.0 - unwrapped.volFastWeight();
        this.minSigma1sRatio = unwrapped.minSigma1sBps() * BPS_INV;
        double maxRet = unwrapped.maxSingleReturnBps() * BPS_INV;
        this.maxSingleReturnSq = maxRet * maxRet;
        this.parkinsonScale = unwrapped.parkinsonScale();
        double bf = unwrapped.bridgeFloorBps() * BPS_INV;
        this.bridgeFloorSq = bf * bf;
        this.maxJumpHazardRatio = unwrapped.maxJumpHazardBps() * BPS_INV;
        this.takerFeeRate = unwrapped.takerFeeRateBps() * BPS_INV;
    }

    @Override
    public String getName() {
        return "PricingModelStrategy";
    }

    @Override
    public Timeframe getTimeframe() {
        return config.timeframe();
    }

    @Override
    public long getRequiredClobLookbackMs() {
        return Math.min(candleDurationMs - 1_000L, config.maxTauMs() + 10_000L);
    }

    @Override
    public TradingStrategy createInstance() {
        return new PricingModelStrategy(config);
    }

    /**
     * Returns the unwrapped immutable configuration governing this strategy.
     *
     * @return {@link PricingModelStrategyConfig}
     */
    public PricingModelStrategyConfig getConfig() {
        return config;
    }

    @Override
    public void init(StrategyContext context) {
        if (context == null) {
            throw new IllegalArgumentException("context cannot be null");
        }
        this.context = context;
        if (Log.isInfoEnabled()) {
            Log.infof("Initialized %s (tau=[%d..%d ms], maxAsk=%d mc, maxWhipsaw=%d bps, jumpHazard=%d bps, minSigma=%.2f bps, maxClips=%d)",
                    getName(),
                    config.minTauMs(),
                    config.maxTauMs(),
                    config.maxOpenPriceMc(),
                    config.maxWhipsawRangeBps(),
                    config.maxJumpHazardBps(),
                    config.minSigma1sBps(),
                    config.maxOrdersPerMarket());
        }
    }

    @Override
    public void reset() {
        spotRingHead = 0;
        spotRingLen = 0;
        probRingHead = 0;
        probRingLen = 0;
        activeCandleStartMs = 0L;
        activeCandleEndMs = 0L;
        targetTwapOpenUsd = 0.0;
        nextCandleStartMs = 0L;
        nextCandleTargetUsd = 0.0;
        missingTargetWarned = false;
        lastCalcSpotUsd = Double.NaN;
        lastCalcSpotMs = 0L;
        lastPbSpotUsd = Double.NaN;
        lastPbSpotMs = 0L;
        relBasisEma = 0.0;
        hasBasisSample = false;
        resetVolatilityState();
        resetMarketState();
    }

    private void resetVolatilityState() {
        lastVolSecond = -1L;
        prevVolPriceUsd = Double.NaN;
        double initSigma = Math.max(minSigma1sRatio, 1.3e-4);
        varFast = initSigma * initSigma;
        varSlow = initSigma * initSigma;
        volSampleCount = 0;
    }

    private void resetMarketState() {
        probRingHead = 0;
        probRingLen = 0;
        candleMinSHatUsd = Double.MAX_VALUE;
        candleMaxSHatUsd = 0.0;
        lockedTwapAreaUsdMs = 0.0;
        lockedAnchorStartMs = 0L;
        lastIntegratedTsMs = 0L;
        lastIntegratedSHatUsd = Double.NaN;
        latestRawProbUpMc = NEUTRAL_PROB_MC;
        latestModelProbUpMc = NEUTRAL_PROB_MC;
        lastQuoteTsMs = 0L;
        lastAskUpMc = 0;
        lastBidUpMc = 0;
        lastAskDownMc = 0;
        lastBidDownMc = 0;
        lastDepthUpSh = 0;
        lastDepthDownSh = 0;
        lastEstFillUpMc = 0;
        lastEstFillDownMc = 0;
        marketDirection = TradeDirection.NO_TRADE;
        ordersFilledInMarket = 0;
        firstFilledAskMc = 0;
        filledSharesInMarket = 0;
        lastOrderSubmitMs = 0L;
        orderInFlight = false;
        pendingClientOrderId = null;
        pendingDirection = TradeDirection.NO_TRADE;
        pendingRequestedShares = 0;
        pendingAskMc = 0;
        balanceRefreshedForMarket = false;
    }

    private void ensureActiveCandle(long timestampMs) {
        if (timestampMs <= 0L) {
            return;
        }
        if (activeCandleStartMs > 0L && timestampMs >= activeCandleStartMs && timestampMs <= activeCandleEndMs) {
            return;
        }
        long alignedStartMs = (timestampMs / candleDurationMs) * candleDurationMs;
        if (activeCandleStartMs > 0L && alignedStartMs < activeCandleStartMs) {
            return;
        }
        if (alignedStartMs != activeCandleStartMs) {
            long lastRefMs = Math.max(lastCalcSpotMs, lastPbSpotMs);
            if (lastRefMs > 0L && (alignedStartMs < lastRefMs || (alignedStartMs - lastRefMs) > 120_000L)) {
                spotRingHead = 0;
                spotRingLen = 0;
                lastCalcSpotUsd = Double.NaN;
                lastCalcSpotMs = 0L;
                lastPbSpotUsd = Double.NaN;
                lastPbSpotMs = 0L;
                relBasisEma = 0.0;
                hasBasisSample = false;
                resetVolatilityState();
            }
            activeCandleStartMs = alignedStartMs;
            activeCandleEndMs = alignedStartMs + candleDurationMs;
            targetTwapOpenUsd = (alignedStartMs == nextCandleStartMs && nextCandleTargetUsd > 0.0)
                    ? nextCandleTargetUsd
                    : 0.0;
            nextCandleStartMs = 0L;
            nextCandleTargetUsd = 0.0;
            missingTargetWarned = false;
            resetMarketState();
        }
    }

    @Override
    public void onSpotTwap(long timestampMs, String symbol, double spotPrice, double twap60sPrice) {
        if (Double.isNaN(spotPrice) || spotPrice <= 0.0 || timestampMs <= 0L) {
            return;
        }
        if (lastCalcSpotMs > 0L && timestampMs < lastCalcSpotMs) {
            return;
        }
        ensureActiveCandle(timestampMs);
        lastCalcSpotUsd = spotPrice;
        lastCalcSpotMs = timestampMs;
        recordSpotSample(timestampMs, spotPrice);

        double sHat = currentAnchoredSpotUsd();
        updateCandleExtrema(timestampMs, sHat);
        updateVolatility1s(timestampMs, sHat);
        integrateFinalMinuteTwap(timestampMs, sHat);
        updateModelProbability(timestampMs, sHat);

        if (lastQuoteTsMs > 0L && (timestampMs - lastQuoteTsMs) <= config.maxQuoteAgeMs()) {
            evaluateSignal(timestampMs, sHat);
        }
    }

    @Override
    public void onPolymarketCryptoPrice(
            long timestampMs,
            String symbol,
            String source,
            double price,
            long priceTimestampMs) {
        if (Double.isNaN(price) || price <= 0.0 || timestampMs <= 0L) {
            return;
        }
        if (lastPbSpotMs > 0L && timestampMs < lastPbSpotMs) {
            return;
        }
        ensureActiveCandle(timestampMs);
        lastPbSpotUsd = price;
        lastPbSpotMs = timestampMs;

        long originMs = (priceTimestampMs > 0L ? priceTimestampMs : timestampMs - 1_200L) + config.spotDelayOffsetMs();
        double matchedCalc = lookupCalcSpotAtOrBefore(originMs);
        if (!Double.isNaN(matchedCalc) && matchedCalc > 0.0) {
            double rawRelBasis = (price - matchedCalc) / matchedCalc;
            if (Math.abs(rawRelBasis) < 0.02) {
                if (!hasBasisSample) {
                    relBasisEma = rawRelBasis;
                    hasBasisSample = true;
                } else {
                    relBasisEma += config.basisEmaAlpha() * (rawRelBasis - relBasisEma);
                }
            }
        }

        double sHat = currentAnchoredSpotUsd();
        updateCandleExtrema(timestampMs, sHat);
        if (lastCalcSpotMs <= 0L || (timestampMs - lastCalcSpotMs) > 2_000L) {
            updateVolatility1s(timestampMs, sHat);
        }
        integrateFinalMinuteTwap(timestampMs, sHat);
        updateModelProbability(timestampMs, sHat);
    }

    @Override
    public void onPolymarketCryptoTwap60s(
            long timestampMs,
            String symbol,
            String source,
            double twapPrice,
            int windowSeconds,
            long twapTimestampMs) {
        if (Double.isNaN(twapPrice) || twapPrice <= 0.0 || timestampMs <= 0L) {
            return;
        }
        long effectiveTsMs = twapTimestampMs > 0L ? twapTimestampMs : timestampMs;
        if (activeCandleStartMs > 0L && effectiveTsMs == activeCandleEndMs) {
            nextCandleStartMs = activeCandleEndMs;
            nextCandleTargetUsd = twapPrice;
            return;
        }

        ensureActiveCandle(timestampMs);
        long offsetFromStartMs = effectiveTsMs - activeCandleStartMs;
        if (offsetFromStartMs == 0L
                || (targetTwapOpenUsd <= 0.0 && Math.abs(offsetFromStartMs) <= MAX_TARGET_OFFSET_MS)) {
            targetTwapOpenUsd = twapPrice;
            updateCandleExtrema(timestampMs, twapPrice);
            double sHat = currentAnchoredSpotUsd();
            if (!Double.isNaN(sHat) && sHat > 0.0) {
                updateModelProbability(timestampMs, sHat);
            }
        }
    }

    @Override
    public void onOrderBookQuote(OrderBookQuote quote) {
        if (quote == null) {
            return;
        }
        long nowMs = quote.timestampMs() > 0L ? quote.timestampMs() : Math.max(lastCalcSpotMs, lastPbSpotMs);
        if (nowMs <= 0L) {
            return;
        }
        ensureActiveCandle(nowMs);

        int bidUpMc = extractFloorTickMc(quote.bestBidUp());
        int askUpMc = extractCeilTickMc(quote.bestAskUp());
        int askDownMc = (!Double.isNaN(quote.bestAskDown()) && quote.bestAskDown() > 0.0 && quote.bestAskDown() <= 1.0)
                ? extractCeilTickMc(quote.bestAskDown())
                : ((bidUpMc > 0 && bidUpMc < MAX_MC) ? (MAX_MC - bidUpMc) : 0);
        int bidDownMc = (!Double.isNaN(quote.bestBidDown()) && quote.bestBidDown() > 0.0 && quote.bestBidDown() < 1.0)
                ? extractFloorTickMc(quote.bestBidDown())
                : ((askUpMc > 0 && askUpMc < MAX_MC) ? (MAX_MC - askUpMc) : 0);

        if ((bidUpMc <= 0 || askUpMc <= bidUpMc || askUpMc >= MAX_MC)
                && (bidDownMc <= 0 || askDownMc <= bidDownMc || askDownMc >= MAX_MC)) {
            return;
        }

        lastQuoteTsMs = nowMs;
        lastAskUpMc = askUpMc;
        lastBidUpMc = bidUpMc;
        lastAskDownMc = askDownMc;
        lastBidDownMc = bidDownMc;
        lastDepthUpSh = sanitizeDepthShares(quote.depth1cUp());
        lastDepthDownSh = sanitizeDepthShares(quote.depth1cDown());
        lastEstFillUpMc = sanitizeMillicents(quote.estimatedFillPriceUp(), askUpMc);
        lastEstFillDownMc = sanitizeMillicents(quote.estimatedFillPriceDown(), askDownMc);

        double sHat = currentAnchoredSpotUsd();
        if (!Double.isNaN(sHat) && sHat > 0.0) {
            updateModelProbability(nowMs, sHat);
            evaluateSignal(nowMs, sHat);
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
            int filled = report.executedSize() != null
                    ? (int) Math.round(report.executedSize().doubleValue())
                    : pendingRequestedShares;
            if (filled > 0) {
                int fillPriceMc = report.executedPrice() != null
                        ? Math.clamp((int) Math.round(report.executedPrice().doubleValue() * 1_000.0), 1, MAX_MC - 1)
                        : pendingAskMc;
                if (ordersFilledInMarket == 0) {
                    firstFilledAskMc = fillPriceMc;
                    marketDirection = pendingDirection;
                }
                ordersFilledInMarket++;
                filledSharesInMarket += filled;
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

    /**
     * Evaluates the exact closed-form {@code Student-t (df=4)} cumulative distribution function
     * in millicents {@code [1..999]} with zero heap allocations.
     *
     * @param z standardized distance in standard deviation units
     * @return model probability in millicents {@code [1..999]}
     */
    public static int studentT4ProbMc(double z) {
        if (Double.isNaN(z)) {
            return NEUTRAL_PROB_MC;
        }
        double z2 = z * z;
        double denom = Math.sqrt(4.0 + z2);
        double u = z / denom;
        double p = 0.5 + 0.5 * u * (1.0 + 2.0 / (4.0 + z2));
        int mc = (int) Math.round(p * 1_000.0);
        return Math.clamp(mc, 1, MAX_MC - 1);
    }

    /**
     * Applies Bayesian Merton jump-diffusion shrinkage to a diffusive {@code Student-t} probability
     * in millicents as a continuous function of remaining time {@code tauMs}.
     *
     * @param rawT4ProbMc diffusive probability in millicents {@code [1..999]}
     * @param tauMs remaining milliseconds to candle expiry
     * @param maxJumpHazardRatio maximum jump-reset probability at {@code tau >= 300s}
     * @return posterior probability in millicents {@code [1..999]}
     */
    public static int bayesianJumpMixtureProbMc(int rawT4ProbMc, long tauMs, double maxJumpHazardRatio) {
        if (maxJumpHazardRatio <= 0.0 || tauMs <= JUMP_HAZARD_ZERO_TAU_MS) {
            return Math.clamp(rawT4ProbMc, 1, MAX_MC - 1);
        }
        double spanFrac = Math.min(1.0, (double) (tauMs - JUMP_HAZARD_ZERO_TAU_MS) / (double) JUMP_HAZARD_SPAN_MS);
        double eta = maxJumpHazardRatio * spanFrac;
        int mixed = (int) Math.round((1.0 - eta) * rawT4ProbMc + eta * NEUTRAL_PROB_MC);
        return Math.clamp(mixed, 1, MAX_MC - 1);
    }

    /**
     * Computes Polymarket's parabolic taker fee in millicents for a given execution price in millicents.
     *
     * @param priceMc contract execution price in millicents {@code [1..999]}
     * @param feeRate decimal fee rate (e.g. {@code 0.07})
     * @return fee per share in millicents (rounded up conservatively)
     */
    public static int takerFeeMc(int priceMc, double feeRate) {
        if (priceMc <= 0 || priceMc >= MAX_MC || feeRate <= 0.0) {
            return 0;
        }
        double p = priceMc * 1e-3;
        return (int) Math.ceil(feeRate * p * (1.0 - p) * 1_000.0);
    }

    private double currentAnchoredSpotUsd() {
        if (!Double.isNaN(lastCalcSpotUsd) && lastCalcSpotUsd > 0.0) {
            return hasBasisSample ? lastCalcSpotUsd * (1.0 + relBasisEma) : lastCalcSpotUsd;
        }
        return lastPbSpotUsd;
    }

    private void updateCandleExtrema(long tsMs, double sHatUsd) {
        if (Double.isNaN(sHatUsd) || sHatUsd <= 0.0 || tsMs < activeCandleStartMs || tsMs > activeCandleEndMs) {
            return;
        }
        if (sHatUsd < candleMinSHatUsd) {
            candleMinSHatUsd = sHatUsd;
        }
        if (sHatUsd > candleMaxSHatUsd) {
            candleMaxSHatUsd = sHatUsd;
        }
    }

    private void recordSpotSample(long tsMs, double spotUsd) {
        if (spotRingLen > 0) {
            int lastIdx = (spotRingHead + spotRingLen - 1) & SPOT_RING_MASK;
            if (spotRingTs[lastIdx] == tsMs) {
                spotRingPx[lastIdx] = spotUsd;
                return;
            }
        }
        if (spotRingLen == SPOT_RING_CAPACITY) {
            spotRingHead = (spotRingHead + 1) & SPOT_RING_MASK;
            spotRingLen--;
        }
        int tail = (spotRingHead + spotRingLen) & SPOT_RING_MASK;
        spotRingTs[tail] = tsMs;
        spotRingPx[tail] = spotUsd;
        spotRingLen++;
    }

    private double lookupCalcSpotAtOrBefore(long targetTsMs) {
        for (int back = spotRingLen - 1; back >= 0; back--) {
            int idx = (spotRingHead + back) & SPOT_RING_MASK;
            long ts = spotRingTs[idx];
            if (ts <= targetTsMs) {
                return (targetTsMs - ts) <= 5_000L ? spotRingPx[idx] : Double.NaN;
            }
        }
        return Double.NaN;
    }

    private void updateVolatility1s(long tsMs, double sHatUsd) {
        if (Double.isNaN(sHatUsd) || sHatUsd <= 0.0) {
            return;
        }
        long sec = tsMs / 1_000L;
        if (sec <= lastVolSecond) {
            return;
        }
        if (lastVolSecond >= 0L && !Double.isNaN(prevVolPriceUsd) && prevVolPriceUsd > 0.0 && (sec - lastVolSecond) <= 10L) {
            double dtSec = (double) (sec - lastVolSecond);
            double relRet = (sHatUsd - prevVolPriceUsd) / prevVolPriceUsd;
            double rSqPerSec = Math.min((relRet * relRet) / dtSec, maxSingleReturnSq);
            varFast += alphaFast * (rSqPerSec - varFast);
            varSlow += alphaSlow * (rSqPerSec - varSlow);
            volSampleCount++;
        }
        prevVolPriceUsd = sHatUsd;
        lastVolSecond = sec;
    }

    private double effectiveSigma1sRatio(long nowMs, double sHatUsd) {
        double ewmaSigma = Math.max(minSigma1sRatio, Math.sqrt(fastVarWeight * varFast + slowVarWeight * varSlow));
        if (parkinsonScale > 0.0 && candleMaxSHatUsd > candleMinSHatUsd && candleMinSHatUsd < Double.MAX_VALUE && sHatUsd > 0.0) {
            double spanUsd = candleMaxSHatUsd - candleMinSHatUsd;
            double netDispUsd = targetTwapOpenUsd > 0.0 ? Math.abs(sHatUsd - targetTwapOpenUsd) : 0.0;
            double effSpanUsd = Math.max(0.35 * spanUsd, spanUsd - 0.65 * netDispUsd);
            double elapsedSec = Math.max(30.0, (nowMs - activeCandleStartMs) * 1e-3);
            double parkinsonSigma = parkinsonScale * (effSpanUsd / sHatUsd) / Math.sqrt(FOUR_LN_2 * elapsedSec);
            if (parkinsonSigma > ewmaSigma) {
                return parkinsonSigma;
            }
        }
        return ewmaSigma;
    }

    private void integrateFinalMinuteTwap(long tsMs, double sHatUsd) {
        if (activeCandleEndMs <= 0L || Double.isNaN(sHatUsd) || sHatUsd <= 0.0) {
            return;
        }
        long windowStartMs = activeCandleEndMs - TWAP_WINDOW_MS;
        long clampedTs = Math.min(tsMs, activeCandleEndMs);

        if (lastIntegratedTsMs > 0L && clampedTs > lastIntegratedTsMs && !Double.isNaN(lastIntegratedSHatUsd)) {
            if (clampedTs > windowStartMs) {
                long segStart = Math.max(lastIntegratedTsMs, windowStartMs);
                if (lockedAnchorStartMs <= 0L) {
                    lockedAnchorStartMs = segStart;
                }
                if (clampedTs > segStart) {
                    lockedTwapAreaUsdMs += lastIntegratedSHatUsd * (clampedTs - segStart);
                }
            }
        } else if (clampedTs >= windowStartMs && lockedAnchorStartMs <= 0L) {
            lockedAnchorStartMs = clampedTs;
        }

        if (clampedTs >= lastIntegratedTsMs) {
            lastIntegratedTsMs = clampedTs;
            lastIntegratedSHatUsd = sHatUsd;
        }
    }

    private void updateModelProbability(long nowMs, double sHatUsd) {
        if (activeCandleEndMs <= 0L || targetTwapOpenUsd <= 0.0 || Double.isNaN(sHatUsd) || sHatUsd <= 0.0) {
            return;
        }
        long remMs = Math.max(0L, activeCandleEndMs - nowMs);
        double tauSec = remMs * 1e-3;
        long windowStartMs = activeCandleEndMs - TWAP_WINDOW_MS;

        double muTwapUsd;
        if (nowMs <= windowStartMs) {
            muTwapUsd = sHatUsd;
        } else {
            long anchorMs = lockedAnchorStartMs > 0L ? lockedAnchorStartMs : nowMs;
            double areaToNow = lockedTwapAreaUsdMs;
            if (lastIntegratedTsMs >= windowStartMs && nowMs > lastIntegratedTsMs && !Double.isNaN(lastIntegratedSHatUsd)) {
                areaToNow += lastIntegratedSHatUsd * (nowMs - lastIntegratedTsMs);
            } else if (lastIntegratedTsMs > 0L && lastIntegratedTsMs < windowStartMs && !Double.isNaN(lastIntegratedSHatUsd)) {
                anchorMs = windowStartMs;
                areaToNow += lastIntegratedSHatUsd * (nowMs - windowStartMs);
            }
            long totalSpanMs = (nowMs - anchorMs) + remMs;
            muTwapUsd = totalSpanMs > 0L ? (areaToNow + sHatUsd * remMs) / (double) totalSpanMs : sHatUsd;
        }

        double sigma1sRatio = effectiveSigma1sRatio(nowMs, sHatUsd);
        double relVarRem = (tauSec >= 60.0
                ? (sigma1sRatio * sigma1sRatio) * (tauSec - 40.0)
                : (sigma1sRatio * sigma1sRatio) * ((tauSec * tauSec * tauSec) / 10_800.0)) + bridgeFloorSq;
        double sdRemUsd = sHatUsd * Math.sqrt(Math.max(1e-12, relVarRem));
        double z = (muTwapUsd - targetTwapOpenUsd) / sdRemUsd;

        int rawT4Mc = studentT4ProbMc(z);
        latestRawProbUpMc = rawT4Mc;
        latestModelProbUpMc = bayesianJumpMixtureProbMc(rawT4Mc, remMs, maxJumpHazardRatio);
        recordProbSample(nowMs, rawT4Mc);
    }

    private void recordProbSample(long tsMs, int probUpMc) {
        if (probRingLen > 0) {
            int lastIdx = (probRingHead + probRingLen - 1) & PROB_RING_MASK;
            if (probRingTs[lastIdx] == tsMs) {
                probRingUpMc[lastIdx] = probUpMc;
                return;
            }
        }
        if (probRingLen == PROB_RING_CAPACITY) {
            probRingHead = (probRingHead + 1) & PROB_RING_MASK;
            probRingLen--;
        }
        int tail = (probRingHead + probRingLen) & PROB_RING_MASK;
        probRingTs[tail] = tsMs;
        probRingUpMc[tail] = probUpMc;
        probRingLen++;
    }

    private int lookupProbUpMcAtOrBefore(long targetTsMs) {
        for (int back = probRingLen - 1; back >= 0; back--) {
            int idx = (probRingHead + back) & PROB_RING_MASK;
            if (probRingTs[idx] <= targetTsMs) {
                return probRingUpMc[idx];
            }
        }
        return probRingLen > 0 ? probRingUpMc[probRingHead] : latestRawProbUpMc;
    }

    private void evaluateSignal(long nowMs, double sHatUsd) {
        if (context == null || activeCandleEndMs <= 0L || orderInFlight) {
            return;
        }
        if (ordersFilledInMarket >= config.maxOrdersPerMarket()) {
            return;
        }
        long tauMs = activeCandleEndMs - nowMs;
        if (tauMs < config.minTauMs() || tauMs > config.maxTauMs()) {
            return;
        }
        if (!balanceRefreshedForMarket) {
            balanceRefreshedForMarket = true;
            context.refreshAccountBalance();
        }
        if (targetTwapOpenUsd <= 0.0) {
            if (!missingTargetWarned) {
                missingTargetWarned = true;
                Log.warnf("Skipping market [%d, %d] in %s: opening 60s TWAP target price is missing",
                        activeCandleStartMs, activeCandleEndMs, getName());
            }
            return;
        }
        if (volSampleCount < config.minWarmupSamples()
                || Double.isNaN(lastPbSpotUsd) || lastPbSpotUsd <= 0.0
                || lastCalcSpotMs <= 0L || (nowMs - lastCalcSpotMs) > 2_000L) {
            return;
        }
        if (tauMs > TWAP_WINDOW_MS && candleMaxSHatUsd > candleMinSHatUsd && candleMinSHatUsd < Double.MAX_VALUE) {
            double spanUsd = candleMaxSHatUsd - candleMinSHatUsd;
            double whipsawUsd = Math.max(0.0, spanUsd - Math.abs(sHatUsd - targetTwapOpenUsd));
            double whipsawBps = (whipsawUsd / sHatUsd) * BPS_SCALE;
            if (whipsawBps > config.maxWhipsawRangeBps()) {
                return;
            }
        }
        if (lastOrderSubmitMs > 0L && (nowMs - lastOrderSubmitMs) < config.cooldownMs()) {
            return;
        }
        int remainingInv = config.maxInventoryShares() - filledSharesInMarket;
        if (remainingInv < config.minOrderShares()) {
            return;
        }

        int maxAskMc = config.maxOpenPriceMc();
        int pUpMc = latestModelProbUpMc;
        int pDownMc = MAX_MC - pUpMc;

        int prevRawUpMc = lookupProbUpMcAtOrBefore(nowMs - config.modelJumpWindowMs());
        int jumpUpMc = latestRawProbUpMc - prevRawUpMc;
        int jumpDownMc = prevRawUpMc - latestRawProbUpMc;

        long rawLeadCentiBps = Math.round(((sHatUsd - lastPbSpotUsd) / lastPbSpotUsd) * CENTI_BPS_SCALE);

        int upScoreMc = scoreCandidate(
                TradeDirection.UP, lastAskUpMc, lastBidUpMc, lastDepthUpSh, lastEstFillUpMc,
                maxAskMc, pUpMc, jumpUpMc, rawLeadCentiBps);
        int downScoreMc = scoreCandidate(
                TradeDirection.DOWN, lastAskDownMc, lastBidDownMc, lastDepthDownSh, lastEstFillDownMc,
                maxAskMc, pDownMc, jumpDownMc, -rawLeadCentiBps);

        TradeDirection chosenDir = TradeDirection.NO_TRADE;
        int chosenEdgeMc = 0;
        int chosenAskMc = 0;
        int chosenDepthSh = 0;

        if (upScoreMc >= 0 && upScoreMc >= downScoreMc) {
            chosenDir = TradeDirection.UP;
            chosenEdgeMc = upScoreMc;
            chosenAskMc = lastAskUpMc;
            chosenDepthSh = lastDepthUpSh;
        } else if (downScoreMc >= 0) {
            chosenDir = TradeDirection.DOWN;
            chosenEdgeMc = downScoreMc;
            chosenAskMc = lastAskDownMc;
            chosenDepthSh = lastDepthDownSh;
        }

        if (chosenDir == TradeDirection.NO_TRADE) {
            return;
        }

        int shares = computeOrderShares(chosenEdgeMc, chosenAskMc, chosenDepthSh, remainingInv);
        if (shares < config.minOrderShares()) {
            return;
        }

        int rawLimitMc = Math.min(maxAskMc, chosenAskMc + config.maxSlippageMc());
        int snappedLimitMc = Math.clamp((rawLimitMc / TICK_STEP_MC) * TICK_STEP_MC, TICK_STEP_MC, MAX_MC - TICK_STEP_MC);
        if (snappedLimitMc < chosenAskMc) {
            return;
        }

        submitOrder(nowMs, chosenDir, chosenAskMc, snappedLimitMc, shares);
    }

    private int scoreCandidate(
            TradeDirection dir,
            int askMc,
            int bidMc,
            int depthSh,
            int estFillMc,
            int maxAllowedAskMc,
            int modelProbMc,
            int modelJumpMc,
            long favorableLeadCentiBps) {
        if (marketDirection != TradeDirection.NO_TRADE && marketDirection != dir) {
            return -1;
        }
        if (ordersFilledInMarket > 0 && askMc < firstFilledAskMc - TICK_STEP_MC) {
            return -1;
        }
        if (bidMc <= 0 || askMc <= bidMc || askMc < config.minOpenPriceMc() || askMc > maxAllowedAskMc) {
            return -1;
        }
        if ((askMc - bidMc) > config.maxSpreadMc() || depthSh < config.minDepthShares()) {
            return -1;
        }
        if (favorableLeadCentiBps < config.minLeadGapCentiBps() || modelJumpMc < config.minModelJumpMc()) {
            return -1;
        }

        int feeAskMc = takerFeeMc(askMc, takerFeeRate);
        int netEdgeMc = modelProbMc - askMc - feeAskMc;
        if (netEdgeMc < config.minNetEdgeMc()) {
            return -1;
        }

        int effFillMc = Math.max(askMc, estFillMc);
        int feeFillMc = takerFeeMc(effFillMc, takerFeeRate);
        int fillNetEdgeMc = modelProbMc - effFillMc - feeFillMc;
        if (fillNetEdgeMc < config.minFillNetEdgeMc()) {
            return -1;
        }
        return netEdgeMc;
    }

    private int computeOrderShares(int netEdgeMc, int askMc, int depth1cShares, int remainingInventory) {
        int extraEdgeMc = Math.max(0, netEdgeMc - config.minNetEdgeMc());
        int baseShares = config.minOrderShares()
                + ((extraEdgeMc * (config.maxOrderShares() - config.minOrderShares())) / KELLY_EDGE_SLOPE_MC);
        int costMc = Math.clamp(askMc + takerFeeMc(askMc, takerFeeRate), 10, MAX_MC - 10);
        int varianceDenom = costMc * (MAX_MC - costMc);
        int kellyShares = (int) (((long) baseShares * NEUTRAL_VARIANCE_MC_SQ) / Math.max(1, varianceDenom));
        int clampedTarget = Math.clamp(kellyShares, config.minOrderShares(), config.maxOrderShares());
        int depthCap = Math.max(config.minOrderShares(), depth1cShares / 2);
        return Math.min(clampedTarget, Math.min(depthCap, remainingInventory));
    }

    private void submitOrder(long nowMs, TradeDirection direction, int askMc, int limitPriceMc, int shares) {
        String clientOrderId = "pm-mdl-" + activeCandleStartMs + "-" + (++orderSequence);
        String token = direction == TradeDirection.UP ? config.upToken() : config.downToken();
        BigDecimal price = PRICE_MC_CACHE[limitPriceMc];
        BigDecimal size = shares <= MAX_CACHED_SHARES ? SHARE_SIZE_CACHE[shares] : BigDecimal.valueOf(shares);

        this.orderInFlight = true;
        this.pendingClientOrderId = clientOrderId;
        this.pendingDirection = direction;
        this.pendingRequestedShares = shares;
        this.pendingAskMc = askMc;
        this.lastOrderSubmitMs = nowMs;

        if (Log.isDebugEnabled()) {
            Log.debugf("%s BUY %s: ts=%d, askMc=%d, limitMc=%d, shares=%d, modelUpMc=%d",
                    getName(), direction, nowMs, askMc, limitPriceMc, shares, latestModelProbUpMc);
        }

        OrderCommand cmd = new OrderCommand(
                clientOrderId,
                config.defaultMarketId(),
                token,
                config.timeframe(),
                "BUY",
                price,
                size
        );
        context.getExecutionRouter().submitOrder(cmd);
    }

    private static int sanitizeDepthShares(double depth1c) {
        if (Double.isNaN(depth1c) || depth1c <= 0.0) {
            return 0;
        }
        return depth1c >= 100_000.0 ? 100_000 : (int) Math.round(depth1c);
    }

    private static int sanitizeMillicents(double price, int fallbackMc) {
        if (Double.isNaN(price) || price <= 0.0 || price >= 1.0) {
            return fallbackMc;
        }
        return Math.clamp((int) Math.round(price * 1_000.0), 1, MAX_MC - 1);
    }

    private static int extractFloorTickMc(double price) {
        if (Double.isNaN(price) || price <= 0.0 || price >= 1.0) {
            return 0;
        }
        int rawMc = (int) Math.round(price * 1_000.0);
        return (rawMc <= 0 || rawMc >= MAX_MC) ? 0 : (rawMc / TICK_STEP_MC) * TICK_STEP_MC;
    }

    private static int extractCeilTickMc(double price) {
        if (Double.isNaN(price) || price <= 0.0 || price > 1.0) {
            return 0;
        }
        int rawMc = (int) Math.round(price * 1_000.0);
        if (rawMc <= 0 || rawMc >= MAX_MC) {
            return 0;
        }
        return ((rawMc + TICK_STEP_MC - 1) / TICK_STEP_MC) * TICK_STEP_MC;
    }
}
