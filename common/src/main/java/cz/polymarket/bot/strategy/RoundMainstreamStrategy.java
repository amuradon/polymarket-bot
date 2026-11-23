package cz.polymarket.bot.strategy;

import cz.polymarket.bot.indicators.SpotTwapListener;

import cz.polymarket.bot.domain.OrderBookQuote;
import cz.polymarket.bot.domain.Timeframe;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.math.BigDecimal;

/**
 * Reconstructed {@code Round-Mainstream} ({@code 0xeda9247a2b3c99a9e0bf46cdac6e1974365cf589})
 * whale-sweep copy strategy for 15-minute binary crypto prediction markets.
 * <p>
 * Forensic analysis of 3,408 executions across 254 markets demonstrates that {@code Round-Mainstream}
 * copies a basket of sharp Polymarket whale wallets operating in low-volatility compression regimes:
 * <ol>
 *   <li><b>Volatility-compression mispricing sweeps</b>: When realized 1-second volatility and 5-minute
 *       range are compressed, market makers quoting wider baseline implied volatility leave binary options
 *       underpriced; whales sweep the CLOB and post large mirrored bid walls ({@code depth1cOpp}).</li>
 *   <li><b>Syndicate wall/sweep dominance</b>: In ultra-quiet compression near the opening TWAP target,
 *       multi-thousand-share whale sweeps and ask-wall iceberg depletions drive directional positioning.</li>
 *   <li><b>Synthetic opposite-token {@code BUY} offsets</b>: Whale-confirmed profit-locking ({@code OFFSET_TP})
 *       and single-cycle reversal hedges ({@code OFFSET_SL}) are executed via opposite-token {@code BUY}
 *       orders settled at expiry.</li>
 * </ol>
 * Orders are submitted immediately upon signal confirmation without artificial blockchain observation delay.
 */
@ApplicationScoped
public class RoundMainstreamStrategy implements TradingStrategy, SpotTwapListener {

    private static final int MAX_MC = 1_000;
    private static final int TICK_STEP_MC = 10;
    private static final long CANDLE_DURATION_MS = 900_000L;
    private static final long TWAP_WINDOW_MS = 60_000L;
    private static final long MAX_TARGET_PRICE_OFFSET_MS = 10_000L;
    private static final long RANGE_5M_WINDOW_MS = 300_000L;
    private static final long SPOT_MOMENTUM_WINDOW_MS = 4_000L;
    private static final long SPOT_LEAD_LAG_MS = 1_500L;
    private static final long SPOT_LEAD_BETA_BPS = 7_500L;
    private static final long SPOT_LEAD_DEADBAND_CBPS = 12L;
    private static final long MIN_SWEEP_SPAN_MS = 80L;
    private static final long MIN_WALL_PERSISTENCE_MS = 450L;
    private static final int MIN_WALL_DEPLETION_STEPS = 2;
    private static final int CLEARED_TICK_FLOOR_SHARES = 180;
    private static final int MIN_ANCHOR_DEPTH_FOR_TICK_FLOOR = 400;
    private static final int MAX_ORDERS_PER_MARKET = 28;
    private static final int EARLY_MEGA_WARMUP_SAMPLES = 15;
    private static final long MIN_SIGMA_1S_FLOOR_CBPS = 8L;
    private static final long MIN_SIGMA_REM_FLOOR_CBPS = 35L;
    private static final long PIPS_PER_USD = 10_000L;
    private static final long BPS_SCALE = 10_000L;
    private static final long CENTI_BPS_SCALE = 1_000_000L;
    private static final int MAX_CACHED_SHARES = 2_048;

    private static final int[] SQRT_SEC_X100 = new int[901];
    private static final BigDecimal[] PRICE_MC_CACHE = new BigDecimal[MAX_MC + 1];
    private static final BigDecimal[] SHARE_SIZE_CACHE = new BigDecimal[MAX_CACHED_SHARES + 1];

    static {
        for (int k = 0; k <= 900; k++) {
            SQRT_SEC_X100[k] = (int) Math.round(100.0 * Math.sqrt(k));
        }
        for (int mc = 0; mc <= MAX_MC; mc++) {
            PRICE_MC_CACHE[mc] = BigDecimal.valueOf(mc, 3);
        }
        for (int s = 0; s <= MAX_CACHED_SHARES; s++) {
            SHARE_SIZE_CACHE[s] = BigDecimal.valueOf(s);
        }
    }

    private final RoundMainstreamStrategyConfig config;
    private final UnderlyingPriceRingBuffer chainlinkBuffer = new UnderlyingPriceRingBuffer();
    private final UnderlyingPriceRingBuffer calcSpotBuffer = new UnderlyingPriceRingBuffer();
    private final SpotVolatilityBuffer volatilityBuffer = new SpotVolatilityBuffer();
    private final SideSweepBook upSweepBook = new SideSweepBook();
    private final SideSweepBook downSweepBook = new SideSweepBook();

    private StrategyContext context;

    // Active 15-minute candle lifecycle & target price state (in 1e-4 USD pips)
    private long activeCandleStartMs = 0L;
    private long activeCandleEndMs = 0L;
    private long targetPricePips = 0L;
    private long nextCandleStartMs = 0L;
    private long nextCandleTargetPricePips = 0L;
    private boolean missingTargetPriceWarned = false;

    // Real-time underlying state in 1e-4 USD pips
    private long lastChainlinkPips = 0L;
    private long lastChainlinkMs = 0L;
    private long lastLiveTwapPips = 0L;
    private long lastLiveTwapEffectiveMs = 0L;
    private long lastLiveTwapArrivalMs = 0L;
    private long lastCalcSpotPips = 0L;
    private long lastCalcSpotMs = 0L;
    private long candleMinPips = Long.MAX_VALUE;
    private long candleMaxPips = 0L;

    // Deduplicated last quote state
    private int lastQuoteAskUpMc = 0;
    private int lastQuoteBidUpMc = 0;
    private int lastQuoteDepthUpSh = 0;
    private int lastQuoteAskDownMc = 0;
    private int lastQuoteBidDownMc = 0;
    private int lastQuoteDepthDownSh = 0;

    // Per-market execution & inventory state
    private int netSharesUp = 0;
    private int netSharesDown = 0;
    private long filledPriceMcSharesUp = 0L;
    private long filledPriceMcSharesDown = 0L;
    private int tradesInMarket = 0;
    private boolean offsetHedgeTriggered = false;
    private TradeDirection primarySide = TradeDirection.NO_TRADE;
    private long lastUpOrderMs = 0L;
    private long lastDownOrderMs = 0L;
    private long lastAnyOrderMs = 0L;
    private boolean balanceRefreshedForMarket = false;
    private boolean orderInFlight = false;
    private TradeDirection pendingDirection = TradeDirection.NO_TRADE;
    private int pendingRequestedShares = 0;
    private int pendingLimitPriceMc = 0;
    private String pendingClientOrderId = null;
    private boolean pendingMarksOffsetHedge = false;
    private long orderSequence = 0L;

    /**
     * Constructs {@link RoundMainstreamStrategy} with the supplied configuration bean,
     * unwrapping any CDI client proxy for direct field access on the real-time hot path.
     *
     * @param config strategy configuration
     */
    @Inject
    public RoundMainstreamStrategy(RoundMainstreamStrategyConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("config cannot be null");
        }
        this.config = config.getClass() == RoundMainstreamStrategyConfig.class
                ? config
                : new RoundMainstreamStrategyConfig(
                        config.minTauMs(),
                        config.maxTauMs(),
                        config.sameSideCooldownMs(),
                        config.oppositeSideCooldownMs(),
                        config.maxRange5mBps(),
                        config.maxCandleRangeBps(),
                        config.maxSigma1sCentiBps(),
                        config.maxTargetDistanceBps(),
                        config.maxAdverseSpotMoveCentiBps(),
                        config.minCompressionEdgeMc(),
                        config.minWarmupSamples(),
                        config.sweepWindowMs(),
                        config.minEntryAskAdvanceMc(),
                        config.minEntryBidAdvanceMc(),
                        config.maxSpreadMc(),
                        config.minEffectiveSweepShares(),
                        config.minBidWallShares(),
                        config.minWallDepletionShares(),
                        config.minOpenPriceMc(),
                        config.maxOpenPriceMc(),
                        config.minOffsetTpPriceMc(),
                        config.maxOffsetTpPriceMc(),
                        config.maxOffsetTpPairCostMc(),
                        config.minOffsetSlPairCostMc(),
                        config.maxSlippageMc(),
                        config.takerFeeRateBps(),
                        config.minOrderShares(),
                        config.maxOrderShares(),
                        config.sweepShareScaleBps(),
                        config.convexWhaleSweepThresholdShares(),
                        config.maxInventorySharesPerSide(),
                        config.defaultMarketId(),
                        config.upToken(),
                        config.downToken());
    }

    @Override
    public String getName() {
        return "RoundMainstreamStrategy";
    }

    @Override
    public long getRequiredClobLookbackMs() {
        return Math.min(CANDLE_DURATION_MS - 1_000L, config.maxTauMs() + config.sweepWindowMs() + 5_000L);
    }

    @Override
    public Timeframe getTimeframe() {
        return Timeframe.FIFTEEN_MINUTES;
    }

    @Override
    public TradingStrategy createInstance() {
        return new RoundMainstreamStrategy(config);
    }

    @Override
    public void init(StrategyContext context) {
        if (context == null) {
            throw new IllegalArgumentException("context cannot be null");
        }
        this.context = context;
        Log.infof("Initialized %s (tau=[%d..%d ms], sameCooldown=%d ms, maxRange5m=%d bps, maxSigma1s=%d cbps, minEdge=%d mc, minSweepSh=%d, minBidWall=%d, shares=[%d..%d], maxInv=%d)",
                getName(),
                config.minTauMs(),
                config.maxTauMs(),
                config.sameSideCooldownMs(),
                config.maxRange5mBps(),
                config.maxSigma1sCentiBps(),
                config.minCompressionEdgeMc(),
                config.minEffectiveSweepShares(),
                config.minBidWallShares(),
                config.minOrderShares(),
                config.maxOrderShares(),
                config.maxInventorySharesPerSide());
    }

    @Override
    public void reset() {
        chainlinkBuffer.clear();
        calcSpotBuffer.clear();
        volatilityBuffer.clear();
        upSweepBook.clear();
        downSweepBook.clear();
        activeCandleStartMs = 0L;
        activeCandleEndMs = 0L;
        targetPricePips = 0L;
        nextCandleStartMs = 0L;
        nextCandleTargetPricePips = 0L;
        missingTargetPriceWarned = false;
        lastChainlinkPips = 0L;
        lastChainlinkMs = 0L;
        lastCalcSpotPips = 0L;
        lastCalcSpotMs = 0L;
        resetMarketState();
    }

    private void resetMarketState() {
        upSweepBook.clear();
        downSweepBook.clear();
        candleMinPips = Long.MAX_VALUE;
        candleMaxPips = 0L;
        lastLiveTwapPips = 0L;
        lastLiveTwapEffectiveMs = 0L;
        lastLiveTwapArrivalMs = 0L;
        lastQuoteAskUpMc = 0;
        lastQuoteBidUpMc = 0;
        lastQuoteDepthUpSh = 0;
        lastQuoteAskDownMc = 0;
        lastQuoteBidDownMc = 0;
        lastQuoteDepthDownSh = 0;
        netSharesUp = 0;
        netSharesDown = 0;
        filledPriceMcSharesUp = 0L;
        filledPriceMcSharesDown = 0L;
        tradesInMarket = 0;
        offsetHedgeTriggered = false;
        primarySide = TradeDirection.NO_TRADE;
        lastUpOrderMs = 0L;
        lastDownOrderMs = 0L;
        lastAnyOrderMs = 0L;
        balanceRefreshedForMarket = false;
        orderInFlight = false;
        pendingDirection = TradeDirection.NO_TRADE;
        pendingRequestedShares = 0;
        pendingLimitPriceMc = 0;
        pendingClientOrderId = null;
        pendingMarksOffsetHedge = false;
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
            if (lastChainlinkMs > 0L && (alignedStartMs < lastChainlinkMs || alignedStartMs - lastChainlinkMs > 120_000L)) {
                chainlinkBuffer.clear();
                calcSpotBuffer.clear();
                volatilityBuffer.clear();
                lastChainlinkPips = 0L;
                lastChainlinkMs = 0L;
                lastCalcSpotPips = 0L;
                lastCalcSpotMs = 0L;
            }
            activeCandleStartMs = alignedStartMs;
            activeCandleEndMs = alignedEndMs;
            targetPricePips = (alignedStartMs == nextCandleStartMs && nextCandleTargetPricePips > 0L)
                    ? nextCandleTargetPricePips
                    : 0L;
            nextCandleStartMs = 0L;
            nextCandleTargetPricePips = 0L;
            missingTargetPriceWarned = false;
            resetMarketState();
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
        if (lastChainlinkMs > 0L && timestampMs <= lastChainlinkMs) {
            return;
        }
        ensureActiveCandle(timestampMs);
        long pips = Math.round(price * PIPS_PER_USD);
        if (pips <= 0L) {
            return;
        }
        chainlinkBuffer.record(timestampMs, pips);
        volatilityBuffer.onTick(timestampMs, pips);
        lastChainlinkPips = pips;
        lastChainlinkMs = timestampMs;
        updateCandleExtrema(pips);
    }

    @Override
    public void onSpotTwap(
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
        long pips = Math.round(spotPrice * PIPS_PER_USD);
        if (pips <= 0L) {
            return;
        }
        calcSpotBuffer.record(timestampMs, pips);
        lastCalcSpotPips = pips;
        lastCalcSpotMs = timestampMs;
        if (lastChainlinkPips <= 0L) {
            updateCandleExtrema(pips);
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
        long pips = Math.round(twapPrice * PIPS_PER_USD);
        if (pips <= 0L) {
            return;
        }

        if (activeCandleStartMs > 0L && effectiveTsMs == activeCandleEndMs) {
            nextCandleStartMs = activeCandleEndMs;
            nextCandleTargetPricePips = pips;
            return;
        }

        ensureActiveCandle(timestampMs);
        long offsetFromStartMs = effectiveTsMs - activeCandleStartMs;
        if (offsetFromStartMs == 0L
                || (targetPricePips <= 0L && offsetFromStartMs > 0L && offsetFromStartMs <= MAX_TARGET_PRICE_OFFSET_MS)) {
            targetPricePips = pips;
        }
        if (windowSeconds == 60
                && effectiveTsMs > activeCandleStartMs
                && effectiveTsMs < activeCandleEndMs
                && effectiveTsMs >= lastLiveTwapEffectiveMs) {
            lastLiveTwapPips = pips;
            lastLiveTwapEffectiveMs = effectiveTsMs;
            lastLiveTwapArrivalMs = timestampMs;
        }
    }

    @Override
    public void onOrderBookQuote(OrderBookQuote quote) {
        if (quote == null) {
            return;
        }
        long nowMs = quote.timestampMs() > 0L ? quote.timestampMs() : lastChainlinkMs;
        if (nowMs <= 0L) {
            return;
        }
        ensureActiveCandle(nowMs);

        int bidUpMc = extractFloorTickMc(quote.bestBidUp());
        int askUpMc = extractCeilTickMc(quote.bestAskUp());
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

        if ((bidUpMc <= 0 || askUpMc <= bidUpMc || askUpMc >= MAX_MC)
                && (bidDownMc <= 0 || askDownMc <= bidDownMc || askDownMc >= MAX_MC)) {
            return;
        }

        int depthUpSh = sanitizeDepthShares(quote.depth1cUp());
        int depthDownSh = sanitizeDepthShares(quote.depth1cDown());

        if (askUpMc == lastQuoteAskUpMc
                && bidUpMc == lastQuoteBidUpMc
                && depthUpSh == lastQuoteDepthUpSh
                && askDownMc == lastQuoteAskDownMc
                && bidDownMc == lastQuoteBidDownMc
                && depthDownSh == lastQuoteDepthDownSh) {
            return;
        }
        lastQuoteAskUpMc = askUpMc;
        lastQuoteBidUpMc = bidUpMc;
        lastQuoteDepthUpSh = depthUpSh;
        lastQuoteAskDownMc = askDownMc;
        lastQuoteBidDownMc = bidDownMc;
        lastQuoteDepthDownSh = depthDownSh;

        // Binary parity: depth1cDown mirrors UP's 1c bid wall and depth1cUp mirrors DOWN's 1c bid wall
        if (bidUpMc > 0 && askUpMc > bidUpMc && askUpMc < MAX_MC) {
            upSweepBook.onQuote(nowMs, askUpMc, bidUpMc, depthUpSh, depthDownSh, config.maxSpreadMc(), config.sweepWindowMs());
        }
        if (bidDownMc > 0 && askDownMc > bidDownMc && askDownMc < MAX_MC) {
            downSweepBook.onQuote(nowMs, askDownMc, bidDownMc, depthDownSh, depthUpSh, config.maxSpreadMc(), config.sweepWindowMs());
        }

        evaluateImmediateSignal(nowMs, askUpMc, bidUpMc, depthUpSh, askDownMc, bidDownMc, depthDownSh);
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
                        ? (int) Math.round(report.executedPrice().doubleValue() * 1_000.0)
                        : pendingLimitPriceMc;
                fillPriceMc = Math.clamp(fillPriceMc, 1, MAX_MC - 1);

                if (pendingDirection == TradeDirection.UP) {
                    netSharesUp += filledShares;
                    filledPriceMcSharesUp += (long) fillPriceMc * filledShares;
                } else if (pendingDirection == TradeDirection.DOWN) {
                    netSharesDown += filledShares;
                    filledPriceMcSharesDown += (long) fillPriceMc * filledShares;
                }

                if (primarySide == TradeDirection.NO_TRADE) {
                    primarySide = pendingDirection;
                }
                if (pendingMarksOffsetHedge) {
                    offsetHedgeTriggered = true;
                }
                tradesInMarket++;
            }
            orderInFlight = false;
            pendingClientOrderId = null;
            pendingMarksOffsetHedge = false;
        } else if ("REJECTED".equalsIgnoreCase(status) || "CANCELLED".equalsIgnoreCase(status)) {
            orderInFlight = false;
            pendingClientOrderId = null;
            pendingMarksOffsetHedge = false;
        }
    }

    @Override
    public void onCandleResolution(TradeDirection actualOutcome) {
        orderInFlight = false;
        pendingClientOrderId = null;
        pendingMarksOffsetHedge = false;
    }

    private void updateCandleExtrema(long pips) {
        if (pips < candleMinPips) {
            candleMinPips = pips;
        }
        if (pips > candleMaxPips) {
            candleMaxPips = pips;
        }
    }

    private void maybeRefreshBalance(long nowMs) {
        if (!balanceRefreshedForMarket && context != null && activeCandleEndMs > 0L) {
            long tauMs = activeCandleEndMs - nowMs;
            if (tauMs >= config.minTauMs() && tauMs <= config.maxTauMs()) {
                balanceRefreshedForMarket = true;
                context.refreshAccountBalance();
            }
        }
    }

    private void evaluateImmediateSignal(
            long nowMs,
            int askUpMc,
            int bidUpMc,
            int depthUpSh,
            int askDownMc,
            int bidDownMc,
            int depthDownSh) {
        if (context == null || activeCandleEndMs <= 0L || orderInFlight || tradesInMarket >= MAX_ORDERS_PER_MARKET) {
            return;
        }

        long tauMs = activeCandleEndMs - nowMs;
        if (tauMs < config.minTauMs() || tauMs > config.maxTauMs()) {
            return;
        }

        if (targetPricePips <= 0L) {
            if (!missingTargetPriceWarned) {
                missingTargetPriceWarned = true;
                Log.warnf("Skipping market [%d, %d] in %s: opening 60s TWAP target price is missing",
                        activeCandleStartMs, activeCandleEndMs, getName());
            }
            return;
        }

        long refSpotPips = lastChainlinkPips > 0L ? lastChainlinkPips : lastCalcSpotPips;
        if (refSpotPips <= 0L) {
            return;
        }

        long projectedSettlementPips = computeProjectedSettlementPips(nowMs, tauMs, refSpotPips);
        long rawSigma1sCbps = volatilityBuffer.sigma1sCentiBps();
        long effSigma1sCbps = Math.max(MIN_SIGMA_1S_FLOOR_CBPS, rawSigma1sCbps > 0L ? rawSigma1sCbps : 16L);
        long sigmaRemCbps = computeSigmaRemCentiBps(tauMs, effSigma1sCbps);
        long distUpCbps = ((projectedSettlementPips - targetPricePips) * CENTI_BPS_SCALE) / targetPricePips;
        long zUpCenti = (100L * distUpCbps) / sigmaRemCbps;
        int pCompUpMc = UtterParameciumStrategy.modelProbCdfMc(zUpCenti, tauMs);
        int pCompDownMc = MAX_MC - pCompUpMc;

        // 1. Evaluate whale-confirmed synthetic OFFSET_TP & single-cycle OFFSET_SL when holding unhedged inventory
        int unhedgedDiff = netSharesUp - netSharesDown;
        if (unhedgedDiff != 0 && (nowMs - lastAnyOrderMs) >= config.oppositeSideCooldownMs()) {
            TradeDirection mainSide = unhedgedDiff > 0 ? TradeDirection.UP : TradeDirection.DOWN;
            TradeDirection oppSide = mainSide == TradeDirection.UP ? TradeDirection.DOWN : TradeDirection.UP;
            int unhedgedShares = Math.abs(unhedgedDiff);

            if (unhedgedShares >= config.minOrderShares()) {
                int mainShares = mainSide == TradeDirection.UP ? netSharesUp : netSharesDown;
                int oppShares = mainSide == TradeDirection.UP ? netSharesDown : netSharesUp;
                long mainCostMcShares = mainSide == TradeDirection.UP ? filledPriceMcSharesUp : filledPriceMcSharesDown;
                int avgMainPriceMc = mainShares > 0 ? (int) (mainCostMcShares / mainShares) : 500;

                int askOppMc = oppSide == TradeDirection.UP ? askUpMc : askDownMc;
                int bidOppMc = oppSide == TradeDirection.UP ? bidUpMc : bidDownMc;
                SideSweepBook oppBook = oppSide == TradeDirection.UP ? upSweepBook : downSweepBook;

                if (bidOppMc > 0 && askOppMc > bidOppMc && askOppMc < MAX_MC) {
                    int pairCostMc = avgMainPriceMc + askOppMc;

                    // 1a. Whale-triggered Synthetic Take-Profit (OFFSET_TP): requires active cheap sweep + bid support
                    if (askOppMc >= config.minOffsetTpPriceMc()
                            && askOppMc <= config.maxOffsetTpPriceMc()
                            && (askOppMc - bidOppMc) <= config.maxSpreadMc() + TICK_STEP_MC
                            && pairCostMc <= config.maxOffsetTpPairCostMc()) {
                        boolean oppWhaleBounce = oppBook.hasBounceOrSweep(
                                nowMs,
                                config.sweepWindowMs(),
                                20,
                                10,
                                config.minEffectiveSweepShares() / 3,
                                config.minBidWallShares() / 2);
                        if (oppWhaleBounce) {
                            int remainingOppCap = config.maxInventorySharesPerSide() - oppShares;
                            int tpTarget = unhedgedShares <= 30 ? unhedgedShares : Math.max(config.minOrderShares(), (unhedgedShares + 1) / 2);
                            int tpShares = Math.min(Math.min(tpTarget, config.maxOrderShares()), remainingOppCap);
                            if (tpShares >= config.minOrderShares()) {
                                int limitMc = Math.min(config.maxOffsetTpPriceMc() + TICK_STEP_MC,
                                        Math.min(MAX_MC - TICK_STEP_MC, askOppMc + config.maxSlippageMc()));
                                submitImmediateOrder(nowMs, oppSide, limitMc, tpShares, false);
                                return;
                            }
                        }
                    }

                    // 1b. Single-cycle Synthetic Stop-Loss / Reversal Hedge (OFFSET_SL)
                    if (!offsetHedgeTriggered
                            && pairCostMc >= config.minOffsetSlPairCostMc()
                            && askOppMc >= 540
                            && askOppMc <= config.maxOpenPriceMc()
                            && (askOppMc - bidOppMc) <= config.maxSpreadMc()) {
                        int pCompOppMc = oppSide == TradeDirection.UP ? pCompUpMc : pCompDownMc;
                        int feeOppMc = UtterParameciumStrategy.takerFeeMc(askOppMc, config.takerFeeRateBps());
                        long spotDelta4sCbps = computeRecentSpotDeltaCentiBps(nowMs, refSpotPips);
                        long favOppSpotCbps = oppSide == TradeDirection.UP ? spotDelta4sCbps : -spotDelta4sCbps;

                        if (favOppSpotCbps >= -config.maxAdverseSpotMoveCentiBps()
                                && (pCompOppMc - askOppMc - feeOppMc) >= 20) {
                            int oppSweepShares = oppBook.evaluateSweepShares(
                                    nowMs,
                                    config.sweepWindowMs(),
                                    config.minEntryAskAdvanceMc(),
                                    config.minEntryBidAdvanceMc(),
                                    config.maxSpreadMc(),
                                    config.minBidWallShares(),
                                    config.minWallDepletionShares());
                            if (oppSweepShares >= config.minEffectiveSweepShares()) {
                                int rawClip = computeConvexClipShares(oppSweepShares);
                                int slCap = Math.min(config.maxInventorySharesPerSide() - oppShares, unhedgedShares);
                                int slShares = Math.min(Math.max(rawClip, unhedgedShares / 2), slCap);
                                if (slShares >= config.minOrderShares()) {
                                    int limitMc = Math.min(config.maxOpenPriceMc(), askOppMc + config.maxSlippageMc());
                                    submitImmediateOrder(nowMs, oppSide, limitMc, slShares, true);
                                    return;
                                }
                            }
                        }
                    }
                }
            }
        }

        if (offsetHedgeTriggered) {
            return;
        }

        // 2. Underlying Low-Volatility Compression Gate (in asset-agnostic basis points)
        int samples = volatilityBuffer.sampleCount();
        if (samples < EARLY_MEGA_WARMUP_SAMPLES) {
            return;
        }
        boolean alreadyActive = tradesInMarket > 0;
        long maxAllowedSigmaCbps = alreadyActive ? config.maxSigma1sCentiBps() + 8L : config.maxSigma1sCentiBps();
        if (rawSigma1sCbps > maxAllowedSigmaCbps) {
            return;
        }
        long candleRangeBps = computeCandleRangeBps(refSpotPips);
        long maxAllowedCandleBps = alreadyActive ? (long) config.maxCandleRangeBps() * 2L : config.maxCandleRangeBps();
        if (candleRangeBps > maxAllowedCandleBps) {
            return;
        }
        long range5mCbps = computeRange5mCentiBps(nowMs, refSpotPips);
        int elapsedWindowSec = (int) Math.clamp((nowMs - activeCandleStartMs) / 1_000L, 15L, 300L);
        long maxBase5mBps = alreadyActive ? (long) config.maxRange5mBps() * 2L : config.maxRange5mBps();
        long allowedRange5mCbps = (maxBase5mBps * 100L * SQRT_SEC_X100[elapsedWindowSec]) / SQRT_SEC_X100[300];
        if (range5mCbps > allowedRange5mCbps) {
            return;
        }
        long range5mBps = range5mCbps / 100L;

        if (!alreadyActive) {
            long targetDistBps = (Math.abs(refSpotPips - targetPricePips) * BPS_SCALE) / targetPricePips;
            if (targetDistBps > config.maxTargetDistanceBps()) {
                return;
            }
        }

        long spotDelta4sCbps = computeRecentSpotDeltaCentiBps(nowMs, refSpotPips);
        boolean fullWarmupReady = samples >= config.minWarmupSamples();

        // 3. Directional Whale Sweep & Compression Mispricing Evaluation (FRESH_OPEN & ADD_SAME_DIR)
        int upSweepSh = evaluateDirectionalCandidate(
                nowMs, TradeDirection.UP, askUpMc, bidUpMc, depthUpSh, depthDownSh,
                pCompUpMc, zUpCenti, spotDelta4sCbps, fullWarmupReady, rawSigma1sCbps, range5mBps, upSweepBook);
        int downSweepSh = evaluateDirectionalCandidate(
                nowMs, TradeDirection.DOWN, askDownMc, bidDownMc, depthDownSh, depthUpSh,
                pCompDownMc, -zUpCenti, -spotDelta4sCbps, fullWarmupReady, rawSigma1sCbps, range5mBps, downSweepBook);

        TradeDirection chosenDir = TradeDirection.NO_TRADE;
        int chosenAskMc = 0;
        int chosenSweepSh = 0;

        if (upSweepSh >= config.minEffectiveSweepShares() && upSweepSh >= downSweepSh) {
            chosenDir = TradeDirection.UP;
            chosenAskMc = askUpMc;
            chosenSweepSh = upSweepSh;
        } else if (downSweepSh >= config.minEffectiveSweepShares()) {
            chosenDir = TradeDirection.DOWN;
            chosenAskMc = askDownMc;
            chosenSweepSh = downSweepSh;
        }

        if (chosenDir == TradeDirection.NO_TRADE) {
            return;
        }

        int sameShares = chosenDir == TradeDirection.UP ? netSharesUp : netSharesDown;
        int remainingCap = config.maxInventorySharesPerSide() - sameShares;
        if (remainingCap < config.minOrderShares()) {
            return;
        }

        int clipShares = Math.min(computeConvexClipShares(chosenSweepSh), remainingCap);
        if (clipShares < config.minOrderShares()) {
            return;
        }

        int limitPriceMc = Math.min(config.maxOpenPriceMc(), chosenAskMc + config.maxSlippageMc());
        if (Log.isDebugEnabled()) {
            Log.debugf("RoundMainstream signal %s: tauMs=%d, askMc=%d, limitMc=%d, sweepSh=%d, clipSh=%d, rng5mBps=%d, sig1sCbps=%d",
                    chosenDir, tauMs, chosenAskMc, limitPriceMc, chosenSweepSh, clipShares, range5mBps, rawSigma1sCbps);
        }
        submitImmediateOrder(nowMs, chosenDir, limitPriceMc, clipShares, false);
    }

    private int evaluateDirectionalCandidate(
            long nowMs,
            TradeDirection direction,
            int askMc,
            int bidMc,
            int askDepthSh,
            int bidWallSh,
            int pCompSideMc,
            long zSideCenti,
            long favorableSpotDelta4sCbps,
            boolean fullWarmupReady,
            long sigma1sCbps,
            long range5mBps,
            SideSweepBook sweepBook) {
        if (bidMc <= 0 || askMc <= bidMc || askMc < config.minOpenPriceMc() || askMc > config.maxOpenPriceMc()) {
            return 0;
        }
        if ((askMc - bidMc) > config.maxSpreadMc()) {
            return 0;
        }
        if (favorableSpotDelta4sCbps < -config.maxAdverseSpotMoveCentiBps()) {
            return 0;
        }
        if (primarySide != TradeDirection.NO_TRADE && primarySide != direction) {
            return 0;
        }

        long lastSideMs = direction == TradeDirection.UP ? lastUpOrderMs : lastDownOrderMs;
        if (lastSideMs > 0L && (nowMs - lastSideMs) < config.sameSideCooldownMs()) {
            return 0;
        }

        int feeMc = UtterParameciumStrategy.takerFeeMc(askMc, config.takerFeeRateBps());
        int netCompEdgeMc = pCompSideMc - askMc - feeMc;

        int sweepSh = sweepBook.evaluateSweepShares(
                nowMs,
                config.sweepWindowMs(),
                config.minEntryAskAdvanceMc(),
                config.minEntryBidAdvanceMc(),
                config.maxSpreadMc(),
                config.minBidWallShares(),
                config.minWallDepletionShares());

        // ADD_SAME_DIR continuation scaling inside an already-confirmed whale market
        if (primarySide == direction && tradesInMarket > 0 && sweepSh < config.minEffectiveSweepShares()) {
            int contSweepSh = sweepBook.evaluateSweepShares(
                    nowMs,
                    config.sweepWindowMs(),
                    TICK_STEP_MC,
                    TICK_STEP_MC,
                    config.maxSpreadMc(),
                    (config.minBidWallShares() * 2) / 3,
                    (config.minWallDepletionShares() * 2) / 3);
            if (contSweepSh >= config.minEffectiveSweepShares() / 2
                    && netCompEdgeMc >= config.minCompressionEdgeMc() - 10) {
                return Math.max(config.minEffectiveSweepShares(), contSweepSh + bidWallSh / 4);
            }
            if (bidWallSh >= config.minBidWallShares()
                    && bidWallSh >= askDepthSh * 2
                    && netCompEdgeMc >= config.minCompressionEdgeMc()) {
                return Math.max(config.minEffectiveSweepShares(), bidWallSh);
            }
            return 0;
        }

        if (sweepSh < config.minEffectiveSweepShares()) {
            return 0;
        }

        // Channel 1: Full-warmup compression fair-value mispricing confirmed by whale sweep + mirrored bid wall
        if (fullWarmupReady && netCompEdgeMc >= config.minCompressionEdgeMc()) {
            return Math.max(sweepSh, bidWallSh >= config.minBidWallShares() * 2 ? (sweepSh + bidWallSh / 4) : sweepSh);
        }

        // Channel 2: Mega-whale syndicate sweep / iceberg wall eat (also active in early candle with positive spot lead)
        boolean calmEnough = sigma1sCbps <= (config.maxSigma1sCentiBps() * 4L) / 5L
                && range5mBps <= config.maxRange5mBps();
        boolean megaSyndicate = sweepSh >= config.convexWhaleSweepThresholdShares()
                && askMc >= 360
                && askMc <= 780
                && (netCompEdgeMc >= 15 || (fullWarmupReady && zSideCenti >= -30L && bidWallSh >= config.minBidWallShares() * 2))
                && (bidWallSh >= askDepthSh || sweepSh >= config.minWallDepletionShares());
        if (calmEnough && megaSyndicate) {
            return sweepSh;
        }

        return 0;
    }

    private long computeProjectedSettlementPips(long nowMs, long tauMs, long refSpotPips) {
        long leadDeltaPips = computeDeBiasedSpotLeadPips(nowMs, refSpotPips);
        long projectedSpotPips = refSpotPips + leadDeltaPips;
        if (tauMs >= TWAP_WINDOW_MS) {
            return projectedSpotPips;
        }
        long settlementStartMs = activeCandleEndMs - TWAP_WINDOW_MS;
        long elapsedMs = TWAP_WINDOW_MS - tauMs;
        if (lastLiveTwapPips > 0L
                && lastLiveTwapEffectiveMs > settlementStartMs
                && (nowMs - lastLiveTwapArrivalMs) <= 4_000L
                && elapsedMs >= 15_000L) {
            return (elapsedMs * lastLiveTwapPips + tauMs * projectedSpotPips) / TWAP_WINDOW_MS;
        }
        long lockedMeanPips = chainlinkBuffer.meanInWindowPips(settlementStartMs);
        long anchorLockedPips = lockedMeanPips > 0L ? lockedMeanPips : refSpotPips;
        return (elapsedMs * anchorLockedPips + tauMs * projectedSpotPips) / TWAP_WINDOW_MS;
    }

    private long computeDeBiasedSpotLeadPips(long nowMs, long refSpotPips) {
        if (lastChainlinkPips <= 0L
                || lastCalcSpotPips <= 0L
                || lastCalcSpotMs < lastChainlinkMs
                || (nowMs - lastCalcSpotMs) > 2_500L
                || (nowMs - lastChainlinkMs) > 5_000L) {
            return 0L;
        }
        long anchorCalcPips = calcSpotBuffer.priceAtOrBeforePips(lastChainlinkMs - SPOT_LEAD_LAG_MS, 5_000L);
        if (anchorCalcPips <= 0L) {
            return 0L;
        }
        long rawDeltaPips = lastCalcSpotPips - anchorCalcPips;
        long absCbps = (Math.abs(rawDeltaPips) * CENTI_BPS_SCALE) / refSpotPips;
        if (absCbps <= SPOT_LEAD_DEADBAND_CBPS) {
            return 0L;
        }
        return (rawDeltaPips * SPOT_LEAD_BETA_BPS) / BPS_SCALE;
    }

    private static long computeSigmaRemCentiBps(long tauMs, long sigma1sCbps) {
        int tauSec = (int) Math.clamp((tauMs + 500L) / 1_000L, 0L, 900L);
        long rawRemCbps;
        if (tauSec <= 61) {
            rawRemCbps = (sigma1sCbps * UtterParameciumStrategy.TWAP_REM_WEIGHT_BPS[tauSec]) / BPS_SCALE;
        } else {
            int effVarSec = Math.max(40, tauSec - 20);
            rawRemCbps = (sigma1sCbps * SQRT_SEC_X100[effVarSec]) / 100L;
        }
        return Math.max(MIN_SIGMA_REM_FLOOR_CBPS, rawRemCbps);
    }

    private int computeConvexClipShares(int effectiveSweepShares) {
        long excessAboveMin = Math.max(0L, (long) effectiveSweepShares - config.minEffectiveSweepShares());
        long shares = config.minOrderShares() + (excessAboveMin * config.sweepShareScaleBps()) / BPS_SCALE;
        int threshold = config.convexWhaleSweepThresholdShares();
        if (effectiveSweepShares > threshold) {
            long whaleExcess = effectiveSweepShares - threshold;
            shares += (whaleExcess * 650L) / BPS_SCALE;
        }
        return (int) Math.clamp(shares, config.minOrderShares(), config.maxOrderShares());
    }

    private void submitImmediateOrder(
            long nowMs,
            TradeDirection direction,
            int limitPriceMc,
            int shares,
            boolean marksOffsetHedge) {
        maybeRefreshBalance(nowMs);
        int clampedMc = Math.clamp(limitPriceMc, 10, MAX_MC - 10);
        String clientOrderId = "rm-v1-" + (activeCandleStartMs / 1_000L) + "-" + (++orderSequence);
        String token = direction == TradeDirection.UP ? config.upToken() : config.downToken();
        BigDecimal price = PRICE_MC_CACHE[clampedMc];
        BigDecimal size = shares <= MAX_CACHED_SHARES ? SHARE_SIZE_CACHE[shares] : BigDecimal.valueOf(shares);

        this.orderInFlight = true;
        this.pendingDirection = direction;
        this.pendingRequestedShares = shares;
        this.pendingLimitPriceMc = clampedMc;
        this.pendingClientOrderId = clientOrderId;
        this.pendingMarksOffsetHedge = marksOffsetHedge;
        this.lastAnyOrderMs = nowMs;
        if (direction == TradeDirection.UP) {
            this.lastUpOrderMs = nowMs;
        } else {
            this.lastDownOrderMs = nowMs;
        }

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

    private long computeCandleRangeBps(long refSpotPips) {
        if (candleMaxPips <= candleMinPips || candleMinPips == Long.MAX_VALUE || refSpotPips <= 0L) {
            return 0L;
        }
        return ((candleMaxPips - candleMinPips) * BPS_SCALE) / refSpotPips;
    }

    private long computeRange5mCentiBps(long nowMs, long refSpotPips) {
        long clRangePips = chainlinkBuffer.rangeInWindowPips(nowMs - RANGE_5M_WINDOW_MS);
        long calcRangePips = calcSpotBuffer.rangeInWindowPips(nowMs - RANGE_5M_WINDOW_MS);
        long rangePips = clRangePips > 0L ? clRangePips : calcRangePips;
        return refSpotPips > 0L ? ((rangePips * CENTI_BPS_SCALE) / refSpotPips) : 0L;
    }

    private long computeRecentSpotDeltaCentiBps(long nowMs, long refSpotPips) {
        if (lastCalcSpotPips > 0L && (nowMs - lastCalcSpotMs) <= 2_500L) {
            long prevCalcPips = calcSpotBuffer.priceAtOrBeforePips(nowMs - SPOT_MOMENTUM_WINDOW_MS, 5_000L);
            if (prevCalcPips > 0L) {
                return ((lastCalcSpotPips - prevCalcPips) * CENTI_BPS_SCALE) / refSpotPips;
            }
        }
        if (lastChainlinkPips > 0L) {
            long prevClPips = chainlinkBuffer.priceAtOrBeforePips(nowMs - SPOT_MOMENTUM_WINDOW_MS, 6_000L);
            if (prevClPips > 0L) {
                return ((lastChainlinkPips - prevClPips) * CENTI_BPS_SCALE) / refSpotPips;
            }
        }
        return 0L;
    }

    private static int sanitizeDepthShares(double depth1c) {
        if (Double.isNaN(depth1c) || depth1c <= 0.0) {
            return 0;
        }
        if (depth1c >= 100_000.0) {
            return 100_000;
        }
        return (int) Math.round(depth1c);
    }

    private static int extractFloorTickMc(double price) {
        if (Double.isNaN(price) || price <= 0.0 || price >= 1.0) {
            return 0;
        }
        int rawMc = (int) Math.round(price * 1_000.0);
        if (rawMc <= 0 || rawMc >= MAX_MC) {
            return 0;
        }
        return (rawMc / TICK_STEP_MC) * TICK_STEP_MC;
    }

    private static int extractCeilTickMc(double price) {
        if (Double.isNaN(price) || price <= 0.0 || price > 1.0) {
            return 0;
        }
        int rawMc = (int) Math.round(price * 1_000.0);
        if (rawMc <= 0 || rawMc > MAX_MC) {
            return 0;
        }
        int rounded = ((rawMc + TICK_STEP_MC - 1) / TICK_STEP_MC) * TICK_STEP_MC;
        return Math.min(MAX_MC, rounded);
    }

    /**
     * Returns the locked opening 60s TWAP target price in USD cents, or {@code 0} if missing.
     *
     * @return target price in USD cents
     */
    public long getTargetPriceCents() {
        return targetPricePips / 100L;
    }

    /**
     * Returns whether the active candle logged a missing target-price warning.
     *
     * @return true if missing target warning was emitted
     */
    public boolean isMissingTargetPriceWarned() {
        return missingTargetPriceWarned;
    }

    /**
     * Returns the cumulative filled UP shares in the active 15m market.
     *
     * @return filled UP shares
     */
    public int getNetSharesUp() {
        return netSharesUp;
    }

    /**
     * Returns the cumulative filled DOWN shares in the active 15m market.
     *
     * @return filled DOWN shares
     */
    public int getNetSharesDown() {
        return netSharesDown;
    }

    /**
     * Returns the number of filled orders in the active 15m market.
     *
     * @return filled order count
     */
    public int getTradesInMarket() {
        return tradesInMarket;
    }

    /**
     * Returns the strategy configuration instance.
     *
     * @return strategy configuration
     */
    public RoundMainstreamStrategyConfig getConfig() {
        return config;
    }

    /**
     * Pre-allocated circular buffer storing 500ms-coalesced underlying price buckets in {@code 1e-4 USD} pips
     * for {@code O(N)} cache-resident 5-minute range, settlement mean, and lagged momentum lookups.
     */
    static final class UnderlyingPriceRingBuffer {
        private static final int CAPACITY = 1_024;
        private static final int MASK = CAPACITY - 1;
        private static final long BUCKET_MS = 500L;

        private final long[] endTsMs = new long[CAPACITY];
        private final long[] minPips = new long[CAPACITY];
        private final long[] maxPips = new long[CAPACITY];
        private final long[] closePips = new long[CAPACITY];
        private int head = 0;
        private int len = 0;

        void clear() {
            head = 0;
            len = 0;
        }

        void record(long timestampMs, long pricePips) {
            if (len > 0) {
                int lastIdx = (head + len - 1) & MASK;
                long lastTs = endTsMs[lastIdx];
                if (timestampMs < lastTs) {
                    return;
                }
                if (timestampMs - lastTs < BUCKET_MS) {
                    endTsMs[lastIdx] = timestampMs;
                    if (pricePips < minPips[lastIdx]) {
                        minPips[lastIdx] = pricePips;
                    }
                    if (pricePips > maxPips[lastIdx]) {
                        maxPips[lastIdx] = pricePips;
                    }
                    closePips[lastIdx] = pricePips;
                    return;
                }
            }
            if (len == CAPACITY) {
                head = (head + 1) & MASK;
                len--;
            }
            int tail = (head + len) & MASK;
            endTsMs[tail] = timestampMs;
            minPips[tail] = pricePips;
            maxPips[tail] = pricePips;
            closePips[tail] = pricePips;
            len++;
        }

        long rangeInWindowPips(long minTimestampMs) {
            if (len == 0) {
                return 0L;
            }
            long lo = Long.MAX_VALUE;
            long hi = 0L;
            for (int i = len - 1; i >= 0; i--) {
                int idx = (head + i) & MASK;
                if (endTsMs[idx] < minTimestampMs) {
                    break;
                }
                if (minPips[idx] < lo) {
                    lo = minPips[idx];
                }
                if (maxPips[idx] > hi) {
                    hi = maxPips[idx];
                }
            }
            return hi > lo && lo != Long.MAX_VALUE ? (hi - lo) : 0L;
        }

        long meanInWindowPips(long minTimestampMs) {
            if (len == 0) {
                return 0L;
            }
            long sum = 0L;
            int cnt = 0;
            for (int i = len - 1; i >= 0; i--) {
                int idx = (head + i) & MASK;
                if (endTsMs[idx] < minTimestampMs) {
                    break;
                }
                sum += closePips[idx];
                cnt++;
            }
            return cnt >= 4 ? (sum / cnt) : 0L;
        }

        long priceAtOrBeforePips(long targetTimestampMs, long maxStalenessMs) {
            for (int i = len - 1; i >= 0; i--) {
                int idx = (head + i) & MASK;
                long ts = endTsMs[idx];
                if (ts <= targetTimestampMs) {
                    return (targetTimestampMs - ts) <= maxStalenessMs ? closePips[idx] : 0L;
                }
            }
            return 0L;
        }
    }

    /**
     * Pre-allocated circular buffer estimating 1-second underlying volatility in centi-bps ({@code 0.01 bps})
     * via Gaussian-consistent Mean Absolute Deviation ({@code MAD * 1.253}).
     */
    static final class SpotVolatilityBuffer {
        private static final int CAPACITY = 64;
        private static final int MASK = CAPACITY - 1;
        private static final long MAD_TO_SIGMA_NUM = 1_253L;
        private static final long MAD_TO_SIGMA_DEN = 1_000L;

        private final long[] absDiffsCentiBps = new long[CAPACITY];
        private int head = 0;
        private int len = 0;
        private long runningSumCentiBps = 0L;
        private long lastSamplePips = 0L;
        private long lastSampleMs = 0L;
        private int totalSamples = 0;

        void clear() {
            head = 0;
            len = 0;
            runningSumCentiBps = 0L;
            lastSamplePips = 0L;
            lastSampleMs = 0L;
            totalSamples = 0;
        }

        void onTick(long timestampMs, long pricePips) {
            if (lastSamplePips <= 0L) {
                lastSamplePips = pricePips;
                lastSampleMs = timestampMs;
                totalSamples = 1;
                return;
            }
            long dtMs = timestampMs - lastSampleMs;
            if (dtMs < 800L) {
                return;
            }
            totalSamples++;
            if (dtMs <= 2_500L) {
                long absDiffCbps = (Math.abs(pricePips - lastSamplePips) * CENTI_BPS_SCALE) / pricePips;
                if (len == CAPACITY) {
                    runningSumCentiBps -= absDiffsCentiBps[head];
                    head = (head + 1) & MASK;
                    len--;
                }
                int tail = (head + len) & MASK;
                absDiffsCentiBps[tail] = absDiffCbps;
                runningSumCentiBps += absDiffCbps;
                len++;
            }
            lastSamplePips = pricePips;
            lastSampleMs = timestampMs;
        }

        int sampleCount() {
            return totalSamples;
        }

        long sigma1sCentiBps() {
            if (len == 0) {
                return 0L;
            }
            return (runningSumCentiBps * MAD_TO_SIGMA_NUM) / (len * MAD_TO_SIGMA_DEN);
        }
    }

    /**
     * Pre-allocated per-outcome CLOB sweep detector requiring genuine whale liquidity footprints:
     * either multi-level price-advancing sweeps backed by mirrored whale bid walls ({@code depth1cOpp})
     * or persistent multi-step ask-wall iceberg depletions.
     */
    static final class SideSweepBook {
        private static final int CAPACITY = 256;
        private static final int MASK = CAPACITY - 1;
        private static final long COALESCE_MS = 40L;

        private final long[] tsMs = new long[CAPACITY];
        private final int[] askMc = new int[CAPACITY];
        private final int[] bidMc = new int[CAPACITY];
        private final int[] askDepthSh = new int[CAPACITY];
        private final int[] bidWallSh = new int[CAPACITY];
        private int head = 0;
        private int len = 0;

        // Persistent same-ask wall depletion tracker
        private int wallAnchorAskMc = 0;
        private int wallAnchorBidMc = 0;
        private int wallPeakDepthSh = 0;
        private int wallLastDepthSh = 0;
        private long wallStartMs = 0L;
        private int wallDownSteps = 0;

        void clear() {
            head = 0;
            len = 0;
            wallAnchorAskMc = 0;
            wallAnchorBidMc = 0;
            wallPeakDepthSh = 0;
            wallLastDepthSh = 0;
            wallStartMs = 0L;
            wallDownSteps = 0;
        }

        void onQuote(
                long nowMs,
                int curAskMc,
                int curBidMc,
                int curAskDepthSh,
                int curBidWallSh,
                int maxSpreadMc,
                long sweepWindowMs) {
            int spread = curAskMc - curBidMc;
            if (curAskMc == wallAnchorAskMc && spread <= maxSpreadMc && curBidMc >= wallAnchorBidMc - TICK_STEP_MC) {
                if ((nowMs - wallStartMs) > sweepWindowMs) {
                    refreshWallWindowPeak(nowMs - sweepWindowMs, curAskMc, curAskDepthSh, nowMs);
                }
                if (curAskDepthSh >= wallPeakDepthSh) {
                    wallPeakDepthSh = curAskDepthSh;
                    wallLastDepthSh = curAskDepthSh;
                    wallStartMs = nowMs;
                    wallDownSteps = 0;
                } else {
                    if (curAskDepthSh + 150 <= wallLastDepthSh) {
                        wallDownSteps++;
                    }
                    wallLastDepthSh = curAskDepthSh;
                }
            } else {
                wallAnchorAskMc = curAskMc;
                wallAnchorBidMc = curBidMc;
                wallPeakDepthSh = curAskDepthSh;
                wallLastDepthSh = curAskDepthSh;
                wallStartMs = nowMs;
                wallDownSteps = 0;
            }

            if (len > 0) {
                int lastIdx = (head + len - 1) & MASK;
                long lastTs = tsMs[lastIdx];
                if (nowMs < lastTs) {
                    return;
                }
                if (nowMs - lastTs < COALESCE_MS && askMc[lastIdx] == curAskMc && bidMc[lastIdx] == curBidMc) {
                    tsMs[lastIdx] = nowMs;
                    askDepthSh[lastIdx] = curAskDepthSh;
                    if (curBidWallSh > bidWallSh[lastIdx]) {
                        bidWallSh[lastIdx] = curBidWallSh;
                    }
                    return;
                }
            }
            if (len == CAPACITY) {
                head = (head + 1) & MASK;
                len--;
            }
            int tail = (head + len) & MASK;
            tsMs[tail] = nowMs;
            askMc[tail] = curAskMc;
            bidMc[tail] = curBidMc;
            askDepthSh[tail] = curAskDepthSh;
            bidWallSh[tail] = curBidWallSh;
            len++;
        }

        private void refreshWallWindowPeak(long minTsMs, int targetAskMc, int curDepthSh, long nowMs) {
            int peak = curDepthSh;
            long earliest = nowMs;
            int steps = 0;
            int prevD = curDepthSh;
            for (int i = len - 1; i >= 0; i--) {
                int idx = (head + i) & MASK;
                if (tsMs[idx] < minTsMs || askMc[idx] != targetAskMc) {
                    break;
                }
                earliest = tsMs[idx];
                if (askDepthSh[idx] > peak) {
                    peak = askDepthSh[idx];
                }
                if (askDepthSh[idx] >= prevD + 150) {
                    steps++;
                }
                prevD = askDepthSh[idx];
            }
            wallPeakDepthSh = peak;
            wallStartMs = earliest;
            wallDownSteps = steps;
        }

        int evaluateSweepShares(
                long nowMs,
                long sweepWindowMs,
                int minAskAdvanceMc,
                int minBidAdvanceMc,
                int maxSpreadMc,
                int minBidWallShares,
                int minWallDepletionShares) {
            if (len == 0) {
                return 0;
            }
            int lastIdx = (head + len - 1) & MASK;
            int curAsk = askMc[lastIdx];
            int curBid = bidMc[lastIdx];
            int curAskDepth = askDepthSh[lastIdx];
            int curBidWall = bidWallSh[lastIdx];
            if ((curAsk - curBid) > maxSpreadMc) {
                return 0;
            }

            // 1. Check persistent ask-wall iceberg depletion at stable tight BBO
            int wallDepletionSh = 0;
            if (curAsk == wallAnchorAskMc
                    && wallDownSteps >= MIN_WALL_DEPLETION_STEPS
                    && (nowMs - wallStartMs) >= MIN_WALL_PERSISTENCE_MS
                    && (nowMs - wallStartMs) <= sweepWindowMs
                    && curBidWall >= minBidWallShares / 2) {
                int netDepleted = wallPeakDepthSh - curAskDepth;
                if (netDepleted >= minWallDepletionShares) {
                    wallDepletionSh = netDepleted;
                }
            }

            // 2. Check multi-level price-advancing sweep backed strictly by a mirrored whale bid wall
            long minTs = nowMs - sweepWindowMs;
            long maxAnchorTs = nowMs - MIN_SWEEP_SPAN_MS;
            int windowMaxAsk = curAsk;
            int maxWindowBidWall = curBidWall;
            int bestAnchorOffset = -1;
            int bestAnchorAsk = curAsk;
            int bestAnchorBid = curBid;

            for (int i = len - 1; i >= 0; i--) {
                int idx = (head + i) & MASK;
                long t = tsMs[idx];
                if (t < minTs) {
                    break;
                }
                int a = askMc[idx];
                int b = bidMc[idx];
                if (bidWallSh[idx] > maxWindowBidWall) {
                    maxWindowBidWall = bidWallSh[idx];
                }
                if (a > windowMaxAsk && (a - b) <= maxSpreadMc) {
                    windowMaxAsk = a;
                }
                if (t <= maxAnchorTs && (a - b) <= maxSpreadMc + TICK_STEP_MC) {
                    int askAdv = curAsk - a;
                    int bidAdv = curBid - b;
                    if (askAdv >= minAskAdvanceMc && bidAdv >= minBidAdvanceMc) {
                        if (bestAnchorOffset < 0 || a < bestAnchorAsk
                                || (a == bestAnchorAsk && askDepthSh[idx] > askDepthSh[(head + bestAnchorOffset) & MASK])) {
                            bestAnchorOffset = i;
                            bestAnchorAsk = a;
                            bestAnchorBid = b;
                        }
                    }
                }
            }

            if (curAsk < windowMaxAsk || bestAnchorOffset < 0 || (curBid - bestAnchorBid) < minBidAdvanceMc) {
                return wallDepletionSh;
            }

            // Guard against post-spoof bounce back to prior resting ask
            if (bestAnchorOffset > 0) {
                int preIdx = (head + bestAnchorOffset - 1) & MASK;
                if (tsMs[preIdx] >= minTs
                        && (askMc[preIdx] - bidMc[preIdx]) <= maxSpreadMc
                        && askMc[preIdx] >= curAsk
                        && bidMc[bestAnchorOffset] == bidMc[preIdx]) {
                    return wallDepletionSh;
                }
            }

            // Strict whale bid-wall confirmation: rejects symmetric small MM quote shifts
            if (maxWindowBidWall < minBidWallShares || curBidWall < minBidWallShares / 2 || curBidWall < curAskDepth) {
                return wallDepletionSh;
            }

            int anchorIdx = (head + bestAnchorOffset) & MASK;
            int anchorDepth = askDepthSh[anchorIdx];
            int sweptShares = 0;
            int nextUncountedAskMc = bestAnchorAsk;

            for (int i = bestAnchorOffset; i < len; i++) {
                int idx = (head + i) & MASK;
                int snapAsk = askMc[idx];
                int snapDepth = askDepthSh[idx];
                if (snapAsk < nextUncountedAskMc || snapAsk >= curAsk) {
                    continue;
                }
                if (snapAsk > nextUncountedAskMc && anchorDepth >= MIN_ANCHOR_DEPTH_FOR_TICK_FLOOR) {
                    int skippedTicks = (snapAsk - nextUncountedAskMc) / TICK_STEP_MC;
                    sweptShares += skippedTicks * CLEARED_TICK_FLOOR_SHARES;
                }
                if (snapAsk + TICK_STEP_MC < curAsk) {
                    sweptShares += snapDepth;
                    nextUncountedAskMc = snapAsk + 2 * TICK_STEP_MC;
                } else {
                    sweptShares += Math.max(snapDepth / 2, Math.max(0, snapDepth - curAskDepth));
                    nextUncountedAskMc = curAsk;
                }
            }

            if (curAsk > nextUncountedAskMc && anchorDepth >= MIN_ANCHOR_DEPTH_FOR_TICK_FLOOR) {
                int skippedTicks = (curAsk - nextUncountedAskMc) / TICK_STEP_MC;
                sweptShares += skippedTicks * CLEARED_TICK_FLOOR_SHARES;
            }

            return Math.max(wallDepletionSh, sweptShares);
        }

        boolean hasBounceOrSweep(
                long nowMs,
                long sweepWindowMs,
                int minAskBounceMc,
                int minBidBounceMc,
                int minSweptShares,
                int minBidWallShares) {
            if (len < 2) {
                return false;
            }
            int lastIdx = (head + len - 1) & MASK;
            int curAsk = askMc[lastIdx];
            int curBid = bidMc[lastIdx];
            int curDepth = askDepthSh[lastIdx];
            int curBidWall = bidWallSh[lastIdx];
            if (curBidWall < minBidWallShares) {
                return false;
            }
            long minTs = nowMs - sweepWindowMs;

            for (int i = len - 2; i >= 0; i--) {
                int idx = (head + i) & MASK;
                if (tsMs[idx] < minTs) {
                    break;
                }
                int askAdv = curAsk - askMc[idx];
                int bidAdv = curBid - bidMc[idx];
                int wiped = askAdv > 0 ? askDepthSh[idx] : Math.max(0, askDepthSh[idx] - curDepth);
                if (askAdv >= minAskBounceMc && bidAdv >= minBidBounceMc && wiped >= minSweptShares) {
                    return true;
                }
            }
            return false;
        }
    }
}
