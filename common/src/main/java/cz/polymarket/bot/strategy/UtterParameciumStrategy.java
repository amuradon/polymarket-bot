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
 * prediction markets using strictly Polymarket PolyBolt streams ({@code price.crypto} and
 * {@code price.crypto.twap}) and Polymarket CLOB order book quotes.
 * <p>
 * Key quantitative and risk-control components:
 * <ul>
 *   <li>Explicit target price ({@code targetPriceCents}) locked from PolyBolt 60s TWAP at candle start.</li>
 *   <li>Multi-horizon robust MAD BTC volatility estimator ({@code 1s} burst MAD, {@code 1s} short MAD,
 *       and {@code 5s} intra-candle regime MAD) immune to quiet-lull underestimation and single-jump squaring.</li>
 *   <li>Exact oracle observation lag compensation ({@code tau_eff = tau + oracleLag}) and fat-tail
 *       model probability ceiling ({@code maxModelProbMc}).</li>
 *   <li>Progressive TWAP lock-in inventory staging, single-direction per-market commitment, and
 *       sweep-proof pre-lock-in guards against adverse spot momentum and sustained CLOB repricing.</li>
 * </ul>
 * <p>
 * All hot-path calculations use deterministic integer/fixed-point arithmetic ({@code long} USD cents
 * for BTC and {@code int} millicents {@code 0..1000} for option prices) with pre-allocated primitive
 * circular buffers and zero heap allocations per tick.
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

    private final UtterParameciumStrategyConfig config;
    private final ChainlinkTwapBuffer chainlinkTwapBuffer = new ChainlinkTwapBuffer();
    private final SpotVolatilityBuffer spotVolatilityBuffer = new SpotVolatilityBuffer();
    private final RecentQuoteBuffer recentQuoteBuffer = new RecentQuoteBuffer();

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

    // Order book & per-market execution state
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
                        config.initialBurstShares(),
                        config.edgeScalingFactor(),
                        config.maxInventorySharesPerSide(),
                        config.earlyInventoryCapBps(),
                        config.fullInventoryTauMs(),
                        config.allowDirectionFlip(),
                        config.adverseDrift10sBlockCents(),
                        config.adverseAskDrop8sBlockMc(),
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
        Log.infof("Initialized %s (tau=[%d..%d ms], cooldown=%d ms, entryPrice=[%d..%d mc], maxProb=%d mc, maxSpread=%d mc, minEdge=%d mc, shares=[%d..%d], maxInventory=%d, earlyCap=%d bps, flip=%s)",
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
                config.allowDirectionFlip());
    }

    @Override
    public void reset() {
        chainlinkTwapBuffer.clear();
        spotVolatilityBuffer.clear();
        recentQuoteBuffer.clear();
        activeCandleStartMs = 0L;
        activeCandleEndMs = 0L;
        targetPriceCents = 0L;
        nextCandleStartMs = 0L;
        nextCandleTargetPriceCents = 0L;
        missingTargetPriceWarned = false;
        lastChainlinkPriceCents = 0L;
        lastChainlinkTickMs = 0L;
        lastChainlinkPriceObsMs = 0L;
        resetMarketExecutionState();
    }

    private void resetMarketExecutionState() {
        recentQuoteBuffer.clear();
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
                lastChainlinkPriceObsMs = 0L;
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
        spotVolatilityBuffer.onTick(timestampMs, priceCents);

        lastChainlinkPriceCents = priceCents;
        lastChainlinkTickMs = timestampMs;
        lastChainlinkPriceObsMs = (priceTimestampMs > 0L && priceTimestampMs <= timestampMs)
                ? priceTimestampMs
                : timestampMs;
    }

    @Override
    public void onCalculatedSpotTwap(
            long timestampMs,
            String symbol,
            double spotPrice,
            double twap60sPrice) {
        // Reserved for later iteration evaluating internal multi-exchange Spot/TWAP early lead
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
     * Computes the progressive cumulative inventory limit allowed at remaining time {@code tauMs}.
     * <p>
     * Scales linearly from {@code earlyInventoryCapBps} of {@code maxInventorySharesPerSide}
     * at {@code tau >= 60s} (when 0% of the settlement TWAP is locked in) up to 100% of
     * {@code maxInventorySharesPerSide} at {@code tau <= fullInventoryTauMs}.
     *
     * @param tauMs remaining time to candle expiry in milliseconds
     * @return maximum cumulative shares allowed for the active side at {@code tauMs}
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

        // Compute projected final 60s TWAP and lag-compensated remaining uncertainty sigma_rem(tau_eff)
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

        long effectiveObsMs = lastChainlinkPriceObsMs > 0L ? lastChainlinkPriceObsMs : lastChainlinkTickMs;
        long oracleLagMs = Math.clamp(nowMs - effectiveObsMs, 0L, MAX_ORACLE_LAG_CLAMP_MS);
        long sigma1sCents = spotVolatilityBuffer.sigma1sCents(config);
        int effectiveTauSec = (int) Math.clamp((tauMs + oracleLagMs + 500L) / 1000L, 0L, 61L);
        long weightBps = TWAP_REM_WEIGHT_BPS[effectiveTauSec];
        long sigmaRemCents = Math.max(MIN_SIGMA_REM_CENTS, (sigma1sCents * weightBps) / 10_000L);

        long zCenti = (100L * (projectedTwapCents - targetPriceCents)) / sigmaRemCents;
        int pUpMc = normalCdfMc(zCenti);
        this.lastModelProbUpMc = pUpMc;

        // Compute short-term spot drift and sustained 1s-bucketed CLOB BBO levels for anti-falling-knife protection
        long price5sAgoCents = chainlinkTwapBuffer.priceAtOrBefore(nowMs - 5_000L, 3_000L);
        long price10sAgoCents = chainlinkTwapBuffer.priceAtOrBefore(nowMs - 10_000L, 4_000L);
        long upDrift5sCents = price5sAgoCents > 0L ? (spotCents - price5sAgoCents) : 0L;
        long upDrift10sCents = price10sAgoCents > 0L ? (spotCents - price10sAgoCents) : 0L;
        long minQuoteWindowMs = nowMs - RECENT_QUOTE_WINDOW_MS;
        int maxBidUp8sMc = recentQuoteBuffer.maxSustainedBid(true, minQuoteWindowMs);
        int maxAskUp8sMc = recentQuoteBuffer.maxSustainedAsk(true, minQuoteWindowMs);
        int maxBidDown8sMc = recentQuoteBuffer.maxSustainedBid(false, minQuoteWindowMs);
        int maxAskDown8sMc = recentQuoteBuffer.maxSustainedAsk(false, minQuoteWindowMs);

        int edgeUpMc = (!config.allowDirectionFlip() && netSharesDown > 0)
                ? Integer.MIN_VALUE
                : evaluateCandidateSideEdge(
                        bidUpMc,
                        askUpMc,
                        maxBidUp8sMc,
                        maxAskUp8sMc,
                        projectedTwapCents - targetPriceCents,
                        spotCents - targetPriceCents,
                        -upDrift5sCents,
                        -upDrift10sCents,
                        tauMs,
                        sigmaRemCents
                );
        int edgeDownMc = (!config.allowDirectionFlip() && netSharesUp > 0)
                ? Integer.MIN_VALUE
                : evaluateCandidateSideEdge(
                        bidDownMc,
                        askDownMc,
                        maxBidDown8sMc,
                        maxAskDown8sMc,
                        targetPriceCents - projectedTwapCents,
                        targetPriceCents - spotCents,
                        upDrift5sCents,
                        upDrift10sCents,
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
        int stagedCapShares = computeTimeStagedMaxInventory(tauMs);
        int remainingCap = Math.max(0, stagedCapShares - currentSideShares);
        int qFinal = Math.min(qRaw, remainingCap);
        if (qFinal < config.minOrderShares()) {
            return;
        }

        boolean isUpChosen = chosenDirection == TradeDirection.UP;
        long chosenDistProjCents = isUpChosen
                ? (projectedTwapCents - targetPriceCents)
                : (targetPriceCents - projectedTwapCents);
        long chosenDistSpotCents = isUpChosen
                ? (spotCents - targetPriceCents)
                : (targetPriceCents - spotCents);
        long chosenAdverseDrift5s = isUpChosen ? -upDrift5sCents : upDrift5sCents;
        long chosenAdverseDrift10s = isUpChosen ? -upDrift10sCents : upDrift10sCents;
        int chosenMaxRecentBidMc = isUpChosen ? maxBidUp8sMc : maxBidDown8sMc;
        int chosenMaxRecentAskMc = isUpChosen ? maxAskUp8sMc : maxAskDown8sMc;

        int limitPriceMc = computeEdgePreservingLimitPriceMc(
                chosenBidMc,
                chosenAskMc,
                chosenMaxRecentBidMc,
                chosenMaxRecentAskMc,
                chosenDistProjCents,
                chosenDistSpotCents,
                chosenAdverseDrift5s,
                chosenAdverseDrift10s,
                tauMs,
                sigmaRemCents
        );

        if (Log.isDebugEnabled()) {
            Log.debugf("Strategy signal %s: tauMs=%d, askMc=%d, limitPriceMc=%d, edgeMc=%d, shares=%d, stagedCap=%d (netUp=%d, netDown=%d)",
                    chosenDirection, tauMs, chosenAskMc, limitPriceMc, chosenEdgeMc, qFinal, stagedCapShares, netSharesUp, netSharesDown);
        }

        submitBatchOrder(nowMs, chosenDirection, limitPriceMc, qFinal);
    }

    private int evaluateCandidateSideEdge(
            int currentBidMc,
            int askMc,
            int maxRecentBidMc,
            int maxRecentAskMc,
            long distProjCents,
            long distSpotCents,
            long adverseDrift5sCents,
            long adverseDrift10sCents,
            long tauMs,
            long sigmaRemCents) {
        if (askMc < config.minEntryPriceMc() || askMc > config.maxEntryPriceMc()) {
            return Integer.MIN_VALUE;
        }
        if (distProjCents <= 0L) {
            return Integer.MIN_VALUE;
        }

        if (tauMs > LATE_LOCKIN_MAX_TAU_MS) {
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
     * z-score and risk gates, and still preserves at least {@code config.minEdgeMc()} net edge
     * after the parabolic taker fee.
     */
    private int computeEdgePreservingLimitPriceMc(
            int currentBidMc,
            int askMc,
            int maxRecentBidMc,
            int maxRecentAskMc,
            long distProjCents,
            long distSpotCents,
            long adverseDrift5sCents,
            long adverseDrift10sCents,
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
                    distProjCents,
                    distSpotCents,
                    adverseDrift5sCents,
                    adverseDrift10sCents,
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
        String clientOrderId = "up-v1-" + (activeCandleStartMs / 1000L) + "-" + (++orderSequence);
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
     * for the 60-second settlement window and short-term spot drift lookups without heap allocations.
     */
    static final class ChainlinkTwapBuffer {
        private static final int CAPACITY = 128;
        private static final int MASK = CAPACITY - 1;

        private final long[] timestampsMs = new long[CAPACITY];
        private final long[] pricesCents = new long[CAPACITY];
        private int head = 0;
        private int len = 0;
        private long runningSumCents = 0L;

        void clear() {
            head = 0;
            len = 0;
            runningSumCents = 0L;
        }

        void evictOlderThan(long minTimestampMs) {
            while (len > 0 && timestampsMs[head] < minTimestampMs) {
                runningSumCents -= pricesCents[head];
                head = (head + 1) & MASK;
                len--;
            }
        }

        void pushAndEvict(long timestampMs, long priceCents, long minTimestampMs) {
            evictOlderThan(minTimestampMs);
            if (len == CAPACITY) {
                runningSumCents -= pricesCents[head];
                head = (head + 1) & MASK;
                len--;
            }
            int tail = (head + len) & MASK;
            timestampsMs[tail] = timestampMs;
            pricesCents[tail] = priceCents;
            runningSumCents += priceCents;
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
     * Multi-horizon zero-allocation robust MAD volatility buffer combining:
     * <ul>
     *   <li>{@code 1s} burst MAD ({@code 16} samples, ~16s) for immediate jump response,</li>
     *   <li>{@code 1s} short-term MAD ({@code 64} samples, ~64s) for baseline diffusion scale, and</li>
     *   <li>{@code 5s} intra-candle regime MAD ({@code 128} samples, ~10.7m) to preserve candle volatility across quiet lulls without squaring single directional jumps.</li>
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
        }

        void onTick(long timestampMs, long priceCents) {
            if (lastTickPriceCents <= 0L) {
                lastTickPriceCents = priceCents;
                lastTickMs = timestampMs;
                anchor5sPriceCents = priceCents;
                anchor5sMs = timestampMs;
                return;
            }

            long dtMs = timestampMs - lastTickMs;
            if (dtMs >= 800L) {
                if (dtMs <= 2_500L) {
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
                lastTickMs = timestampMs;
            }

            long dt5sMs = timestampMs - anchor5sMs;
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
                anchor5sMs = timestampMs;
            }
        }

        long sigma1sCents(UtterParameciumStrategyConfig cfg) {
            if (short1sLen < 5 && regime5sLen < 6) {
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
