package cz.polymarket.bot.strategy;

import cz.polymarket.bot.domain.MarketCandle;
import cz.polymarket.bot.domain.OrderBookQuote;
import cz.polymarket.bot.domain.Timeframe;
import cz.polymarket.bot.domain.TwapUpdate;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.math.BigDecimal;
import java.util.Set;

/**
 * Reconstructed {@code Utter-Paramecium} (Model V1) final-minute Chainlink 60s TWAP lock-in strategy.
 * <p>
 * Operates exclusively in the final 60-second settlement window ({@code tau in [10s, 61s]}) of
 * 15-minute binary prediction markets. Projects the terminal 60-second Chainlink settlement TWAP
 * by combining the already-locked portion of the 60s integral with the current spot price (optionally
 * adjusted for Binance Futures lead-lag), models remaining TWAP uncertainty as
 * {@code sigma_rem(tau) ~ tau^(3/2)}, and executes scaled Taker BUY batches every {@code >= 200ms}
 * whenever {@code P_model(D) - Ask(D) - Fee(Ask(D)) >= min_edge_mc} and {@code Ask(D) <= $0.970}.
 * <p>
 * All hot-path calculations use deterministic integer/fixed-point arithmetic ({@code long} USD cents
 * for BTC and {@code int} millicents {@code 0..1000} for option prices) with pre-allocated ring buffers
 * and zero heap allocations per tick.
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
    private static final long MAX_BINANCE_LEAD_CENTS = 2_500L;
    private static final long MIN_SIGMA_REM_CENTS = 25L;
    private static final long TWAP_WINDOW_MS = 60_000L;
    private static final long BUFFER_RETENTION_MS = 65_000L;
    private static final long INITIAL_BURST_MIN_TAU_MS = 55_000L;

    private final UtterParameciumStrategyConfig config;
    private final ChainlinkTwapBuffer chainlinkTwapBuffer = new ChainlinkTwapBuffer();
    private final SpotVolatilityBuffer spotVolatilityBuffer = new SpotVolatilityBuffer();

    private StrategyContext context;

    // Active 15-minute candle session state
    private long activeCandleStartMs = 0L;
    private long activeCandleEndMs = 0L;
    private long explicitStrikeCents = 0L;
    private long inferredStrikeCents = 0L;

    // Real-time feed tracking in USD cents
    private long lastChainlinkPriceCents = 0L;
    private long lastChainlinkTickMs = 0L;
    private long lastBinancePriceCents = 0L;
    private long binancePriceAtLastClTickCents = 0L;

    // Order book & per-market execution state
    private OrderBookQuote latestValidQuote = null;
    private int netSharesUp = 0;
    private int netSharesDown = 0;
    private int tradesInMarket = 0;
    private long lastSignalMs = 0L;
    private boolean hasEmittedSignal = false;
    private boolean orderInFlight = false;
    private TradeDirection pendingDirection = TradeDirection.NO_TRADE;
    private int pendingRequestedShares = 0;
    private String pendingClientOrderId = null;
    private long orderSequence = 0L;
    private int lastModelProbUpMc = 500;

    /**
     * Constructs the {@link UtterParameciumStrategy} with the given configuration.
     *
     * @param config Model V1 strategy configuration bean
     */
    @Inject
    public UtterParameciumStrategy(UtterParameciumStrategyConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("config cannot be null");
        }
        this.config = config;
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

    /**
     * Computes the exact floor integer square root {@code floor(sqrt(x))} for non-negative {@code long}.
     *
     * @param x non-negative value
     * @return integer square root
     */
    static long isqrt(long x) {
        if (x <= 0L) {
            return 0L;
        }
        long r = (long) Math.sqrt((double) x);
        while ((r + 1L) > 0L && (r + 1L) <= x / (r + 1L)) {
            r++;
        }
        while (r > x / r) {
            r--;
        }
        return r;
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
        if (config.binanceLeadWeightBps() > 0L) {
            return Set.of(ExternalStreamType.BINANCE_FUTURES_AGG_TRADES);
        }
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
    }

    @Override
    public void reset() {
        chainlinkTwapBuffer.clear();
        spotVolatilityBuffer.clear();
        activeCandleStartMs = 0L;
        activeCandleEndMs = 0L;
        explicitStrikeCents = 0L;
        inferredStrikeCents = 0L;
        lastChainlinkPriceCents = 0L;
        lastChainlinkTickMs = 0L;
        lastBinancePriceCents = 0L;
        binancePriceAtLastClTickCents = 0L;
        resetMarketExecutionState();
    }

    private void resetMarketExecutionState() {
        latestValidQuote = null;
        netSharesUp = 0;
        netSharesDown = 0;
        tradesInMarket = 0;
        lastSignalMs = 0L;
        hasEmittedSignal = false;
        orderInFlight = false;
        pendingDirection = TradeDirection.NO_TRADE;
        pendingRequestedShares = 0;
        pendingClientOrderId = null;
        lastModelProbUpMc = 500;
    }

    @Override
    public void onTwapUpdate(TwapUpdate update) {
        if (update == null) {
            return;
        }
        if (update.timeframe() != null && update.timeframe() != Timeframe.FIFTEEN_MINUTES) {
            return;
        }

        long startMs = update.candleStart() * 1000L;
        long endMs = update.candleEnd() * 1000L;

        if (startMs != activeCandleStartMs) {
            if (lastChainlinkTickMs > 0L && (startMs < lastChainlinkTickMs || startMs - lastChainlinkTickMs > 120_000L)) {
                chainlinkTwapBuffer.clear();
                spotVolatilityBuffer.clear();
                lastChainlinkPriceCents = 0L;
                lastChainlinkTickMs = 0L;
                lastBinancePriceCents = 0L;
                binancePriceAtLastClTickCents = 0L;
            }
            activeCandleStartMs = startMs;
            activeCandleEndMs = endMs;
            explicitStrikeCents = (update.openPrice() != null && update.openPrice().doubleValue() > 0.0)
                    ? Math.round(update.openPrice().doubleValue() * 100.0)
                    : 0L;
            inferredStrikeCents = (explicitStrikeCents <= 0L)
                    ? chainlinkTwapBuffer.meanCentsInWindow(startMs - TWAP_WINDOW_MS, startMs)
                    : 0L;
            resetMarketExecutionState();
        } else if (explicitStrikeCents <= 0L && update.openPrice() != null && update.openPrice().doubleValue() > 0.0) {
            explicitStrikeCents = Math.round(update.openPrice().doubleValue() * 100.0);
        }

        if (update.point() != null && update.point().medianPrice() != null) {
            long pointTsMs = update.point().time() * 1000L;
            double medianPrice = update.point().medianPrice().doubleValue();
            if (medianPrice > 0.0 && (lastChainlinkTickMs == 0L || pointTsMs > lastChainlinkTickMs + 1_500L)) {
                recordChainlinkSample(pointTsMs, Math.round(medianPrice * 100.0));
            }
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
        long priceCents = Math.round(price * 100.0);
        recordChainlinkSample(timestampMs, priceCents);

        if (latestValidQuote != null) {
            evaluateEntry(timestampMs, latestValidQuote);
        }
    }

    private void recordChainlinkSample(long timestampMs, long priceCents) {
        long minRetainMs = Math.max(0L, timestampMs - BUFFER_RETENTION_MS);
        chainlinkTwapBuffer.pushAndEvict(timestampMs, priceCents, minRetainMs);
        spotVolatilityBuffer.onTick(timestampMs, priceCents);

        lastChainlinkPriceCents = priceCents;
        lastChainlinkTickMs = timestampMs;
        if (lastBinancePriceCents > 0L) {
            binancePriceAtLastClTickCents = lastBinancePriceCents;
        }

        if (activeCandleStartMs > 0L && explicitStrikeCents <= 0L && timestampMs <= activeCandleStartMs) {
            long mean = chainlinkTwapBuffer.meanCentsInWindow(activeCandleStartMs - TWAP_WINDOW_MS, activeCandleStartMs);
            if (mean > 0L) {
                inferredStrikeCents = mean;
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
        if (getEffectiveStrikeCents() <= 0L
                && (activeCandleStartMs <= 0L || timestampMs <= activeCandleStartMs + TWAP_WINDOW_MS)) {
            inferredStrikeCents = Math.round(twapPrice * 100.0);
        }
    }

    @Override
    public void onBinanceFuturesTrade(long timestampMs, double price, double quantity, boolean isBuyerMaker) {
        if (price <= 0.0 || timestampMs <= 0L) {
            return;
        }
        lastBinancePriceCents = Math.round(price * 100.0);
        if (binancePriceAtLastClTickCents <= 0L) {
            binancePriceAtLastClTickCents = lastBinancePriceCents;
        }
        if (config.binanceLeadWeightBps() > 0L && latestValidQuote != null) {
            evaluateEntry(timestampMs, latestValidQuote);
        }
    }

    @Override
    public void onBinanceSpotTrade(long timestampMs, double price, double quantity, boolean isBuyerMaker) {
        if (price <= 0.0 || timestampMs <= 0L) {
            return;
        }
        lastBinancePriceCents = Math.round(price * 100.0);
        if (binancePriceAtLastClTickCents <= 0L) {
            binancePriceAtLastClTickCents = lastBinancePriceCents;
        }
        if (config.binanceLeadWeightBps() > 0L && latestValidQuote != null) {
            evaluateEntry(timestampMs, latestValidQuote);
        }
    }

    @Override
    public void onOrderBookQuote(OrderBookQuote quote) {
        if (quote == null) {
            latestValidQuote = null;
            return;
        }
        if (!isQuoteTradable(quote)) {
            latestValidQuote = null;
            return;
        }
        latestValidQuote = quote;
        long nowMs = quote.timestampMs() > 0L ? quote.timestampMs() : lastChainlinkTickMs;
        if (nowMs > 0L) {
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
        // Model V1 uses real-time 1s Chainlink price differences rather than historical 15m candles
    }

    @Override
    public void onCandleResolution(TradeDirection actualOutcome) {
        // Model V1 holds all accumulated UP and DOWN positions to candle expiration
        orderInFlight = false;
        pendingClientOrderId = null;
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

        if (hasEmittedSignal && (nowMs - lastSignalMs) < config.cooldownMs()) {
            return;
        }

        long strikeCents = getEffectiveStrikeCents();
        if (strikeCents <= 0L || lastChainlinkPriceCents <= 0L) {
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

        // Compute projected final 60s TWAP and remaining uncertainty sigma_rem(tau)
        long sHatCents = computeEffectiveSpotCents();
        long settlementWindowStartMs = activeCandleEndMs - TWAP_WINDOW_MS;
        long lockedMeanCents = (tauMs <= TWAP_WINDOW_MS)
                ? chainlinkTwapBuffer.meanCentsInWindow(settlementWindowStartMs, nowMs)
                : 0L;

        long projectedTwapCents;
        if (tauMs >= TWAP_WINDOW_MS || lockedMeanCents <= 0L) {
            projectedTwapCents = sHatCents;
        } else {
            long elapsedMs = TWAP_WINDOW_MS - tauMs;
            projectedTwapCents = (elapsedMs * lockedMeanCents + tauMs * sHatCents) / TWAP_WINDOW_MS;
        }

        long sigma1sCents = spotVolatilityBuffer.sigma1sCents(config);
        int tauSec = (int) Math.clamp((tauMs + 500L) / 1000L, 0L, 61L);
        long weightBps = TWAP_REM_WEIGHT_BPS[tauSec];
        long sigmaRemCents = Math.max(MIN_SIGMA_REM_CENTS, (sigma1sCents * weightBps) / 10_000L);

        long zCenti = (100L * (projectedTwapCents - strikeCents)) / sigmaRemCents;
        int pUpMc = normalCdfMc(zCenti);
        int pDownMc = MAX_MC - pUpMc;
        this.lastModelProbUpMc = pUpMc;

        int edgeUpMc = evaluateCandidateSideEdge(askUpMc, pUpMc);
        int edgeDownMc = evaluateCandidateSideEdge(askDownMc, pDownMc);

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

        submitBatchOrder(nowMs, chosenDirection, chosenAskMc, qFinal);
    }

    private int evaluateCandidateSideEdge(int askMc, int pModelMc) {
        if (askMc < config.minEntryPriceMc() || askMc > config.maxEntryPriceMc()) {
            return Integer.MIN_VALUE;
        }
        int feeMc = takerFeeMc(askMc, config.takerFeeRateBps());
        int netEdgeMc = pModelMc - askMc - feeMc;
        boolean favoriteOk = pModelMc >= config.minModelProbMc();
        boolean underdogOk = askMc <= 400 && pModelMc >= 480 && netEdgeMc >= 80;
        if (!favoriteOk && !underdogOk) {
            return Integer.MIN_VALUE;
        }
        return netEdgeMc;
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
     * Computes the effective current BTC spot price in USD cents, incorporating the configured
     * Binance lead-lag adjustment over the last Chainlink tick.
     *
     * @return effective BTC spot price in USD cents
     */
    public long computeEffectiveSpotCents() {
        long clCents = lastChainlinkPriceCents;
        if (clCents <= 0L) {
            return 0L;
        }
        long leadWeightBps = config.binanceLeadWeightBps();
        if (leadWeightBps <= 0L || lastBinancePriceCents <= 0L || binancePriceAtLastClTickCents <= 0L) {
            return clCents;
        }
        long deltaLeadCents = Math.clamp(
                lastBinancePriceCents - binancePriceAtLastClTickCents,
                -MAX_BINANCE_LEAD_CENTS,
                MAX_BINANCE_LEAD_CENTS
        );
        return clCents + (leadWeightBps * deltaLeadCents) / 10_000L;
    }

    /**
     * Returns the effective strike price ({@code priceToBeat}) in USD cents, using either the
     * explicit strike price or the strike price inferred from the 60s Chainlink buffer at candle start.
     *
     * @return effective strike price in USD cents, or {@code 0} if not yet available
     */
    public long getEffectiveStrikeCents() {
        return explicitStrikeCents > 0L ? explicitStrikeCents : inferredStrikeCents;
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
     * for the 60-second settlement window without heap allocations.
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

        long meanCents() {
            return len > 0 ? (runningSumCents / len) : 0L;
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

        int size() {
            return len;
        }
    }

    /**
     * Pre-allocated circular buffer tracking 1-second squared BTC price differences
     * {@code (Delta S_cents)^2} to estimate {@code sigma_1s_cents} without heap allocations.
     */
    static final class SpotVolatilityBuffer {
        private static final int CAPACITY = 64;
        private static final int MASK = CAPACITY - 1;

        private final long[] diffSqBuffer = new long[CAPACITY];
        private int head = 0;
        private int len = 0;
        private long runningSumSq = 0L;
        private long lastSampleTimestampMs = 0L;
        private long lastSamplePriceCents = 0L;

        void clear() {
            head = 0;
            len = 0;
            runningSumSq = 0L;
            lastSampleTimestampMs = 0L;
            lastSamplePriceCents = 0L;
        }

        void onTick(long timestampMs, long priceCents) {
            if (lastSampleTimestampMs > 0L) {
                long dtMs = timestampMs - lastSampleTimestampMs;
                if (dtMs >= 500L && dtMs <= 2_500L) {
                    long diff = Math.abs(priceCents - lastSamplePriceCents);
                    long diffSq = diff * diff;
                    if (len == CAPACITY) {
                        runningSumSq -= diffSqBuffer[head];
                        head = (head + 1) & MASK;
                        len--;
                    }
                    int tail = (head + len) & MASK;
                    diffSqBuffer[tail] = diffSq;
                    runningSumSq += diffSq;
                    len++;
                }
            }
            if (lastSampleTimestampMs == 0L || timestampMs > lastSampleTimestampMs) {
                lastSampleTimestampMs = timestampMs;
                lastSamplePriceCents = priceCents;
            }
        }

        long sigma1sCents(UtterParameciumStrategyConfig cfg) {
            if (len < 5) {
                return cfg.defaultSigma1sCents();
            }
            long meanSq = runningSumSq / len;
            long rms = isqrt(meanSq);
            return Math.clamp(rms, cfg.minSigma1sCents(), cfg.maxSigma1sCents());
        }
    }
}
