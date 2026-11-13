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
 * Historical baseline snapshot ({@code V0}, commit {@code 6b45229}) of {@code UtterParameciumStrategy}
 * prior to Phase 1 loss-mitigation iterations.
 * <p>
 * Operates in the final settlement window ({@code tau in [5s, 61s]}) of 15-minute binary prediction
 * markets using Polymarket PolyBolt streams ({@code price.crypto} and {@code price.crypto.twap})
 * and Polymarket CLOB order book quotes.
 */
@ApplicationScoped
public class UtterParameciumStrategyV0 implements TradingStrategy {

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
    private static final long PRE_ACTIVATION_BALANCE_SYNC_LEAD_MS = 10_000L;
    private static final long INITIAL_BURST_MIN_TAU_MS = 55_000L;
    private static final long LATE_LOCKIN_MAX_TAU_MS = 25_000L;
    private static final long LATE_LOCKIN_MIN_PROJ_DIST_CENTS = 1_200L;
    private static final long LATE_LOCKIN_MIN_NET_DIST_CENTS = 600L;
    private static final long MIN_EFFECTIVE_DIST_CENTS = 200L;
    private static final int HIGH_PRICE_ASK_THRESHOLD_MC = 800;
    private static final long HIGH_PRICE_MIN_Z_CENTI = 250L;
    private static final int EXTREME_PRICE_ASK_THRESHOLD_MC = 900;
    private static final long EXTREME_PRICE_MIN_Z_CENTI = 270L;

    private final Config config;
    private final ChainlinkTwapBuffer chainlinkTwapBuffer = new ChainlinkTwapBuffer();
    private final SpotVolatilityBuffer spotVolatilityBuffer = new SpotVolatilityBuffer();

    private StrategyContext context;

    private long activeCandleStartMs = 0L;
    private long activeCandleEndMs = 0L;
    private long targetPriceCents = 0L;
    private long nextCandleStartMs = 0L;
    private long nextCandleTargetPriceCents = 0L;
    private boolean missingTargetPriceWarned = false;

    private long lastChainlinkPriceCents = 0L;
    private long lastChainlinkTickMs = 0L;

    private OrderBookQuote latestValidQuote = null;
    private int netSharesUp = 0;
    private int netSharesDown = 0;
    private int tradesInMarket = 0;
    private long lastSignalMs = 0L;
    private boolean hasEmittedSignal = false;
    private boolean balanceRefreshedForMarket = false;
    private boolean orderInFlight = false;
    private TradeDirection pendingDirection = TradeDirection.NO_TRADE;
    private int pendingRequestedShares = 0;
    private String pendingClientOrderId = null;
    private long orderSequence = 0L;
    private int lastModelProbUpMc = 500;

    /**
     * Constructs the V0 baseline strategy snapshot and unwraps any CDI client proxy on {@link Config}.
     *
     * @param config isolated V0 configuration bean
     */
    @Inject
    public UtterParameciumStrategyV0(Config config) {
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
                        config.minEdgeMc(),
                        config.maxSlippageMc(),
                        config.minOrderShares(),
                        config.maxOrderShares(),
                        config.initialBurstShares(),
                        config.edgeScalingFactor(),
                        config.maxInventorySharesPerSide(),
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
     * Rounds a sub-cent price in millicents down to the nearest valid 10-mc ($0.01) tick.
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
     * Rounds a sub-cent price in millicents up to the nearest valid 10-mc ($0.01) tick.
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
     * Computes the parabolic Polymarket taker fee per share in millicents.
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
        return "UtterParameciumStrategyV0";
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
        return new UtterParameciumStrategyV0(config);
    }

    @Override
    public void init(StrategyContext context) {
        if (context == null) {
            throw new IllegalArgumentException("context cannot be null");
        }
        this.context = context;
        Log.infof("Initialized %s (tau=[%d..%d ms], cooldown=%d ms, entryPrice=[%d..%d mc], maxSpread=%d mc, minEdge=%d mc, shares=[%d..%d], maxInventory=%d)",
                getName(),
                config.minTauMs(),
                config.maxTauMs(),
                config.cooldownMs(),
                config.minEntryPriceMc(),
                config.maxEntryPriceMc(),
                config.maxSpreadMc(),
                config.minEdgeMc(),
                config.minOrderShares(),
                config.maxOrderShares(),
                config.maxInventorySharesPerSide());
    }

    @Override
    public void reset() {
        chainlinkTwapBuffer.clear();
        spotVolatilityBuffer.clear();
        activeCandleStartMs = 0L;
        activeCandleEndMs = 0L;
        targetPriceCents = 0L;
        nextCandleStartMs = 0L;
        nextCandleTargetPriceCents = 0L;
        missingTargetPriceWarned = false;
        lastChainlinkPriceCents = 0L;
        lastChainlinkTickMs = 0L;
        resetMarketExecutionState();
    }

    private void resetMarketExecutionState() {
        latestValidQuote = null;
        netSharesUp = 0;
        netSharesDown = 0;
        tradesInMarket = 0;
        lastSignalMs = 0L;
        hasEmittedSignal = false;
        balanceRefreshedForMarket = false;
        orderInFlight = false;
        pendingDirection = TradeDirection.NO_TRADE;
        pendingRequestedShares = 0;
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
                lastChainlinkPriceCents = 0L;
                lastChainlinkTickMs = 0L;
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
        recordChainlinkSample(timestampMs, priceCents);

        OrderBookQuote quote = this.latestValidQuote;
        if (quote != null && (quote.timestampMs() <= 0L || timestampMs + 2_000L >= quote.timestampMs())) {
            evaluateEntry(timestampMs, quote);
        }
    }

    private void recordChainlinkSample(long timestampMs, long priceCents) {
        long minRetainMs = Math.max(0L, timestampMs - BUFFER_RETENTION_MS);
        chainlinkTwapBuffer.pushAndEvict(timestampMs, priceCents, minRetainMs);
        spotVolatilityBuffer.onTick(timestampMs, priceCents);

        lastChainlinkPriceCents = priceCents;
        lastChainlinkTickMs = timestampMs;
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
                if (pendingDirection == TradeDirection.UP) {
                    netSharesUp += filledShares;
                } else if (pendingDirection == TradeDirection.DOWN) {
                    netSharesDown += filledShares;
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
        // V0 uses real-time 1s PolyBolt price.crypto differences rather than historical 15m candles
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

        long spotCents = lastChainlinkPriceCents;
        long settlementWindowStartMs = activeCandleEndMs - TWAP_WINDOW_MS;
        long lockedMeanCents = (tauMs <= TWAP_WINDOW_MS)
                ? chainlinkTwapBuffer.meanCentsInWindow(settlementWindowStartMs, nowMs)
                : 0L;

        long projectedTwapCents;
        if (tauMs >= TWAP_WINDOW_MS || lockedMeanCents <= 0L) {
            projectedTwapCents = spotCents;
        } else {
            long elapsedMs = TWAP_WINDOW_MS - tauMs;
            projectedTwapCents = (elapsedMs * lockedMeanCents + tauMs * spotCents) / TWAP_WINDOW_MS;
        }

        long sigma1sCents = spotVolatilityBuffer.sigma1sCents(config);
        int tauSec = (int) Math.clamp((tauMs + 500L) / 1000L, 0L, 61L);
        long weightBps = TWAP_REM_WEIGHT_BPS[tauSec];
        long sigmaRemCents = Math.max(MIN_SIGMA_REM_CENTS, (sigma1sCents * weightBps) / 10_000L);

        long zCenti = (100L * (projectedTwapCents - targetPriceCents)) / sigmaRemCents;
        int pUpMc = normalCdfMc(zCenti);
        this.lastModelProbUpMc = pUpMc;

        int edgeUpMc = evaluateCandidateSideEdge(
                askUpMc,
                projectedTwapCents - targetPriceCents,
                spotCents - targetPriceCents,
                tauMs,
                sigmaRemCents
        );
        int edgeDownMc = evaluateCandidateSideEdge(
                askDownMc,
                targetPriceCents - projectedTwapCents,
                targetPriceCents - spotCents,
                tauMs,
                sigmaRemCents
        );

        TradeDirection chosenDirection = TradeDirection.NO_TRADE;
        int chosenAskMc = 0;
        int chosenEdgeMc = Integer.MIN_VALUE;

        if (edgeUpMc >= config.minEdgeMc() && edgeUpMc >= edgeDownMc) {
            chosenDirection = TradeDirection.UP;
            chosenAskMc = askUpMc;
            chosenEdgeMc = edgeUpMc;
        } else if (edgeDownMc >= config.minEdgeMc()) {
            chosenDirection = TradeDirection.DOWN;
            chosenAskMc = askDownMc;
            chosenEdgeMc = edgeDownMc;
        }

        if (chosenDirection == TradeDirection.NO_TRADE) {
            if (Log.isTraceEnabled()) {
                Log.tracef("Strategy evaluateEntry NO_TRADE: tauMs=%d, spotCents=%d, projTwapCents=%d, targetCents=%d, sigmaRemCents=%d, askUpMc=%d, askDownMc=%d, edgeUpMc=%d, edgeDownMc=%d",
                        tauMs, spotCents, projectedTwapCents, targetPriceCents, sigmaRemCents, askUpMc, askDownMc, edgeUpMc, edgeDownMc);
            }
            return;
        }

        int qRaw;
        if (tradesInMarket == 0 && tauMs >= INITIAL_BURST_MIN_TAU_MS) {
            qRaw = config.initialBurstShares();
        } else {
            long scaled = ((long) config.edgeScalingFactor() * (chosenEdgeMc - config.minEdgeMc()) * (tauMs + 30_000L))
                    / (90_000L * 100L);
            qRaw = (int) Math.clamp(config.minOrderShares() + scaled, config.minOrderShares(), config.maxOrderShares());
        }

        int currentSideShares = (chosenDirection == TradeDirection.UP) ? netSharesUp : netSharesDown;
        int remainingCap = Math.max(0, config.maxInventorySharesPerSide() - currentSideShares);
        int qFinal = Math.min(qRaw, remainingCap);
        if (qFinal < config.minOrderShares()) {
            return;
        }

        long chosenDistProjCents = (chosenDirection == TradeDirection.UP)
                ? (projectedTwapCents - targetPriceCents)
                : (targetPriceCents - projectedTwapCents);
        long chosenDistSpotCents = (chosenDirection == TradeDirection.UP)
                ? (spotCents - targetPriceCents)
                : (targetPriceCents - spotCents);
        int limitPriceMc = computeEdgePreservingLimitPriceMc(
                chosenAskMc,
                chosenDistProjCents,
                chosenDistSpotCents,
                tauMs,
                sigmaRemCents
        );

        if (Log.isDebugEnabled()) {
            Log.debugf("Strategy signal %s: tauMs=%d, askMc=%d, limitPriceMc=%d, edgeMc=%d, shares=%d (netUp=%d, netDown=%d)",
                    chosenDirection, tauMs, chosenAskMc, limitPriceMc, chosenEdgeMc, qFinal, netSharesUp, netSharesDown);
        }

        submitBatchOrder(nowMs, chosenDirection, limitPriceMc, qFinal);
    }

    private int evaluateCandidateSideEdge(
            int askMc,
            long distProjCents,
            long distSpotCents,
            long tauMs,
            long sigmaRemCents) {
        if (askMc < config.minEntryPriceMc() || askMc > config.maxEntryPriceMc()) {
            return Integer.MIN_VALUE;
        }
        if (distProjCents <= 0L) {
            return Integer.MIN_VALUE;
        }
        long effectiveDistCents;
        if (distSpotCents > 0L) {
            effectiveDistCents = Math.min(distProjCents, distSpotCents);
        } else {
            if (tauMs > LATE_LOCKIN_MAX_TAU_MS
                    || distProjCents < LATE_LOCKIN_MIN_PROJ_DIST_CENTS
                    || (distProjCents + distSpotCents) <= LATE_LOCKIN_MIN_NET_DIST_CENTS) {
                return Integer.MIN_VALUE;
            }
            effectiveDistCents = distProjCents + distSpotCents;
        }
        if (effectiveDistCents < MIN_EFFECTIVE_DIST_CENTS) {
            return Integer.MIN_VALUE;
        }
        long zSideCenti = (100L * effectiveDistCents) / sigmaRemCents;
        if (askMc >= EXTREME_PRICE_ASK_THRESHOLD_MC && zSideCenti < EXTREME_PRICE_MIN_Z_CENTI) {
            return Integer.MIN_VALUE;
        }
        if (askMc >= HIGH_PRICE_ASK_THRESHOLD_MC && zSideCenti < HIGH_PRICE_MIN_Z_CENTI) {
            return Integer.MIN_VALUE;
        }
        int pModelMc = normalCdfMc(zSideCenti);
        if (pModelMc < config.minModelProbMc()) {
            return Integer.MIN_VALUE;
        }
        int feeMc = takerFeeMc(askMc, config.takerFeeRateBps());
        return pModelMc - askMc - feeMc;
    }

    private int computeEdgePreservingLimitPriceMc(
            int askMc,
            long distProjCents,
            long distSpotCents,
            long tauMs,
            long sigmaRemCents) {
        int maxAllowedMc = Math.min(config.maxEntryPriceMc(), askMc + config.maxSlippageMc());
        int limitMc = askMc;
        for (int candidateMc = askMc + TICK_STEP_MC; candidateMc <= maxAllowedMc; candidateMc += TICK_STEP_MC) {
            int candidateEdgeMc = evaluateCandidateSideEdge(
                    candidateMc,
                    distProjCents,
                    distSpotCents,
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

    private void submitBatchOrder(long nowMs, TradeDirection direction, int askMc, int shares) {
        String clientOrderId = "up-v0-" + (activeCandleStartMs / 1000L) + "-" + (++orderSequence);
        String token = (direction == TradeDirection.UP) ? config.upToken() : config.downToken();
        BigDecimal price = BigDecimal.valueOf(askMc, 3);
        BigDecimal size = BigDecimal.valueOf(shares);

        this.orderInFlight = true;
        this.lastSignalMs = nowMs;
        this.hasEmittedSignal = true;
        this.pendingDirection = direction;
        this.pendingRequestedShares = shares;
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
     * Returns the explicit target price in USD cents determined from PolyBolt 60s TWAP at market start.
     */
    public long getTargetPriceCents() {
        return targetPriceCents;
    }

    /**
     * Returns whether the active market was skipped due to a missing target price.
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
     * Returns the isolated V0 strategy configuration bean.
     */
    public Config getConfig() {
        return config;
    }

    /**
     * Isolated configuration bean for {@link UtterParameciumStrategyV0}.
     */
    @ApplicationScoped
    public static class Config {

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
         * Constructs the isolated V0 configuration with MicroProfile properties.
         */
        @Inject
        public Config(
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v0.min-tau-ms", defaultValue = "5000") long minTauMs,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v0.max-tau-ms", defaultValue = "61000") long maxTauMs,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v0.cooldown-ms", defaultValue = "250") long cooldownMs,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v0.min-entry-price-mc", defaultValue = "380") int minEntryPriceMc,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v0.max-entry-price-mc", defaultValue = "970") int maxEntryPriceMc,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v0.max-spread-mc", defaultValue = "80") int maxSpreadMc,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v0.min-model-prob-mc", defaultValue = "650") int minModelProbMc,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v0.min-edge-mc", defaultValue = "25") int minEdgeMc,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v0.max-slippage-mc", defaultValue = "50") int maxSlippageMc,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v0.min-order-shares", defaultValue = "5") int minOrderShares,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v0.max-order-shares", defaultValue = "30") int maxOrderShares,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v0.initial-burst-shares", defaultValue = "15") int initialBurstShares,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v0.edge-scaling-factor", defaultValue = "65") int edgeScalingFactor,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v0.max-inventory-shares-per-side", defaultValue = "600") int maxInventorySharesPerSide,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v0.default-sigma-1s-cents", defaultValue = "150") long defaultSigma1sCents,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v0.min-sigma-1s-cents", defaultValue = "80") long minSigma1sCents,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v0.max-sigma-1s-cents", defaultValue = "3500") long maxSigma1sCents,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v0.warmup-secs-before-expiry", defaultValue = "120") long warmupSecsBeforeExpiry,
                @ConfigProperty(name = "polymarket.strategy.utter-paramecium-v0.taker-fee-rate-bps", defaultValue = "700") int takerFeeRateBps,
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
         * Creates a default configuration calibrated to the historical V0 baseline parameters.
         *
         * @return default V0 configuration
         */
        public static Config defaultConfig() {
            return new Config(
                    5_000L,
                    61_000L,
                    250L,
                    380,
                    970,
                    80,
                    650,
                    25,
                    50,
                    5,
                    30,
                    15,
                    65,
                    600,
                    150L,
                    80L,
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

        /** Minimum net price edge after taker fee in millicents. */
        public int minEdgeMc() {
            return minEdgeMc;
        }

        /** Maximum allowed limit price slippage above best ask in millicents. */
        public int maxSlippageMc() {
            return maxSlippageMc;
        }

        /** Minimum order size in integer shares. */
        public int minOrderShares() {
            return minOrderShares;
        }

        /** Maximum single-order size in integer shares. */
        public int maxOrderShares() {
            return maxOrderShares;
        }

        /** Initial burst order size in integer shares when {@code tau >= 55s}. */
        public int initialBurstShares() {
            return initialBurstShares;
        }

        /** Edge scaling coefficient for order sizing. */
        public int edgeScalingFactor() {
            return edgeScalingFactor;
        }

        /** Maximum cumulative shares per side in a single 15m market. */
        public int maxInventorySharesPerSide() {
            return maxInventorySharesPerSide;
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
            if (len == 0 || windowEndMs < windowStartMs) {
                return 0L;
            }
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
            return count > 0 ? (sum / count) : 0L;
        }
    }

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

        long sigma1sCents(Config cfg) {
            if (len < 5) {
                return cfg.defaultSigma1sCents();
            }
            long rawSigma = (runningSumAbsCents * MAD_TO_SIGMA_NUM) / (len * MAD_TO_SIGMA_DEN);
            return Math.clamp(rawSigma, cfg.minSigma1sCents(), cfg.maxSigma1sCents());
        }
    }
}
