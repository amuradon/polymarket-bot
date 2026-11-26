package cz.polymarket.bot.strategy;

import cz.polymarket.bot.domain.OrderBookQuote;
import cz.polymarket.bot.domain.Timeframe;
import cz.polymarket.bot.indicators.FastSpotMicroListener;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.math.BigDecimal;

/**
 * First-principles two-layer structural-statistical trading strategy for Polymarket binary
 * Up/Down markets ({@code OraclePulseLockStrategy}).
 * <p>
 * Architecture:
 * <ol>
 *   <li><b>Layer 1 — Exact Contract Physics</b>: Anchors {@code 50 ms} Direct-USD spot updates
 *       ({@link FastSpotMicroListener}) onto Chainlink {@code price.crypto} via origin-aligned
 *       basis cancellation, computes 2-second non-overlapping realized volatility with a
 *       drift-adjusted Parkinson floor, and inside the final 60 seconds applies a <b>Hybrid
 *       Roll-Out / Roll-In TWAP Anchor</b> on live {@code price.crypto.twap} with heavy-tailed
 *       {@code Student-t (df=4)} and exogenous BTC wick hazard shrinkage.</li>
 *   <li><b>Layer 2 — Microstructure Log-Odds & Dual-Alpha Gate</b>: Adjusts structural log-odds
 *       with 1s 3-venue signed OFI and 500ms 3-venue synchronized impulse, enforcing either
 *       an <b>Irreversible Banked TWAP Cushion</b> ({@code tau <= 45s}) or a <b>Glosten-Milgrom
 *       Unrepriced Stale-Quote Gate</b> paired with a Unified Log-Odds Edge Gate and Bayesian
 *       Epistemic-Variance-Adjusted Kelly sizing held strictly One-Side to settlement.</li>
 * </ol>
 */
@ApplicationScoped
public class OraclePulseLockStrategy implements TradingStrategy, FastSpotMicroListener {

    private static final int MAX_MC = 1_000;
    private static final int TICK_STEP_MC = 10;
    private static final int MAX_CACHED_SHARES = 1_024;
    private static final long TWAP_WINDOW_MS = 60_000L;
    private static final long MAX_TARGET_OFFSET_MS = 10_000L;
    private static final long ORDER_IN_FLIGHT_TIMEOUT_MS = 3_000L;
    private static final double FOUR_LN_2 = 2.772588722239781;
    private static final double ALPHA_VOL_FAST_2S = 1.0 - Math.exp(-2.0 / 30.0);
    private static final double ALPHA_VOL_SLOW_2S = 1.0 - Math.exp(-2.0 / 240.0);

    private static final int FM_RING_CAP = 8_192;
    private static final int FM_RING_MASK = FM_RING_CAP - 1;
    private static final int PB_RING_CAP = 512;
    private static final int PB_RING_MASK = PB_RING_CAP - 1;
    private static final int HIST_RING_CAP = 512;
    private static final int HIST_RING_MASK = HIST_RING_CAP - 1;

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

    private final OraclePulseLockStrategyConfig config;
    private final long candleDurationMs;
    private final double minSigma1sRatio;

    // Pre-allocated zero-GC circular buffers
    private final long[] fmRingTs = new long[FM_RING_CAP];
    private final double[] fmRingPx = new double[FM_RING_CAP];
    private int fmRingHead = 0;
    private int fmRingLen = 0;

    private final long[] pbRingSrcMs = new long[PB_RING_CAP];
    private final double[] pbRingCsum = new double[PB_RING_CAP];
    private long pbTotalSeq = 0L;
    private double pbRunningSum = 0.0;

    private final long[] histRingTs = new long[HIST_RING_CAP];
    private final double[] histRingPUp = new double[HIST_RING_CAP];
    private final double[] histRingAskU = new double[HIST_RING_CAP];
    private final double[] histRingAskD = new double[HIST_RING_CAP];
    private int histRingHead = 0;
    private int histRingLen = 0;
    private long lastHistPushMs = -100L;

    private StrategyContext context;

    // Active market lifecycle & target opening 60s TWAP state
    private long activeCandleStartMs = 0L;
    private long activeCandleEndMs = 0L;
    private double targetTwapOpenUsd = 0.0;
    private long bestTargetOffsetAbsMs = Long.MAX_VALUE;
    private long nextCandleStartMs = 0L;
    private double nextCandleTargetUsd = 0.0;

    // Continuous underlying microstructure, Chainlink spot & 60s TWAP state
    private long lastFastSpotMs = 0L;
    private double lastFastSpotUsd = Double.NaN;
    private double lastSignedOfi1s = 0.0;
    private double lastSyncMove500Usd = 0.0;

    private long lastPbReceiptMs = 0L;
    private long lastPbSourceMs = 0L;
    private double lastPbPriceUsd = Double.NaN;

    private long lastTwap60ReceiptMs = 0L;
    private long lastTwap60SourceMs = 0L;
    private double lastTwap60PriceUsd = Double.NaN;

    // Monotone per-candle cursors for O(1) Hybrid Roll-Out / Roll-In TWAP Anchor
    private long roLoSeq = 1L;
    private long roHiSeq = 1L;
    private long riLoSeq = 1L;

    // 2s realized volatility + drift-adjusted Parkinson state
    private long lastVolBucket2s = -1L;
    private double prevVolSpotUsd = Double.NaN;
    private double var1sFast = (0.9e-4) * (0.9e-4);
    private double var1sSlow = (0.9e-4) * (0.9e-4);
    private double candleOpenSpotUsd = Double.NaN;
    private double candleHighSpotUsd = 0.0;
    private double candleLowSpotUsd = Double.MAX_VALUE;
    private double latestSig1sUsd = 1.5;

    // Latest valid CLOB quote state
    private long lastQuoteTsMs = 0L;
    private double lastBestAskUp = 0.0;
    private double lastBestBidUp = 0.0;
    private double lastBestAskDown = 0.0;
    private double lastBestBidDown = 0.0;
    private double lastDepth1cUp = 0.0;
    private double lastDepth1cDown = 0.0;
    private double lastModelProbUp = Double.NaN;

    // Per-market one-side execution & inventory state
    private TradeDirection primaryDirection = TradeDirection.NO_TRADE;
    private int clipsFilledInMarket = 0;
    private double riskUsdCommitted = 0.0;
    private long lastOrderSubmitMs = -10_000L;
    private boolean orderInFlight = false;
    private long orderInFlightSinceMs = 0L;
    private TradeDirection pendingDirection = TradeDirection.NO_TRADE;
    private double pendingEstAsk = 0.0;
    private int pendingReqShares = 0;
    private boolean balanceRefreshedForMarket = false;
    private long orderSequence = 0L;

    /**
     * Constructs an {@link OraclePulseLockStrategy} with the injected configuration bean,
     * unwrapping any CDI proxy into primitive hot-path fields.
     *
     * @param config strategy configuration
     */
    @Inject
    public OraclePulseLockStrategy(OraclePulseLockStrategyConfig config) {
        OraclePulseLockStrategyConfig unwrapped = OraclePulseLockStrategyConfig.unwrap(config);
        this.config = unwrapped;
        this.candleDurationMs = unwrapped.timeframe().getDurationSeconds() * 1_000L;
        this.minSigma1sRatio = unwrapped.minSigma1sBps() * 1e-4;
        reset();
    }

    @Override
    public String getName() {
        return "OraclePulseLockStrategy";
    }

    @Override
    public Timeframe getTimeframe() {
        return config.timeframe();
    }

    @Override
    public long getRequiredClobLookbackMs() {
        return Math.min(candleDurationMs - 1_000L, config.maxTauMs() + 15_000L);
    }

    @Override
    public TradingStrategy createInstance() {
        return new OraclePulseLockStrategy(config);
    }

    /**
     * Returns the immutable configuration governing this strategy.
     *
     * @return {@link OraclePulseLockStrategyConfig}
     */
    public OraclePulseLockStrategyConfig getConfig() {
        return config;
    }

    @Override
    public void init(StrategyContext context) {
        if (context == null) {
            throw new IllegalArgumentException("context cannot be null");
        }
        this.context = context;
        if (Log.isInfoEnabled()) {
            Log.infof("Initialized %s (tau=[%d..%d ms], ask=[%.2f..%.2f/%.2f], logitEdge=[%.2f/%.2f], maxRisk=$%.0f, maxClips=%d)",
                    getName(),
                    config.minTauMs(),
                    config.maxTauMs(),
                    config.minEntryAsk(),
                    config.maxEntryAskPre60(),
                    config.maxEntryAskLockIn(),
                    config.minLogitEdgePre60(),
                    config.minLogitEdgeLate(),
                    config.maxRiskPerMarketUsd(),
                    config.maxClipsPerMarket());
        }
    }

    @Override
    public void reset() {
        fmRingHead = 0;
        fmRingLen = 0;
        pbTotalSeq = 0L;
        pbRunningSum = 0.0;
        pbRingSrcMs[0] = 0L;
        pbRingCsum[0] = 0.0;
        activeCandleStartMs = 0L;
        activeCandleEndMs = 0L;
        targetTwapOpenUsd = 0.0;
        bestTargetOffsetAbsMs = Long.MAX_VALUE;
        nextCandleStartMs = 0L;
        nextCandleTargetUsd = 0.0;
        lastFastSpotMs = 0L;
        lastFastSpotUsd = Double.NaN;
        lastSignedOfi1s = 0.0;
        lastSyncMove500Usd = 0.0;
        lastPbReceiptMs = 0L;
        lastPbSourceMs = 0L;
        lastPbPriceUsd = Double.NaN;
        lastTwap60ReceiptMs = 0L;
        lastTwap60SourceMs = 0L;
        lastTwap60PriceUsd = Double.NaN;
        resetVolatilityState();
        resetMarketState();
    }

    private void resetVolatilityState() {
        lastVolBucket2s = -1L;
        prevVolSpotUsd = Double.NaN;
        double initSig = Math.max(minSigma1sRatio, 0.9e-4);
        var1sFast = initSig * initSig;
        var1sSlow = initSig * initSig;
        latestSig1sUsd = 1.5;
    }

    private void resetMarketState() {
        histRingHead = 0;
        histRingLen = 0;
        lastHistPushMs = -100L;
        candleOpenSpotUsd = Double.NaN;
        candleHighSpotUsd = 0.0;
        candleLowSpotUsd = Double.MAX_VALUE;
        long minValidSeq = Math.max(1L, pbTotalSeq - (PB_RING_CAP - 4));
        roLoSeq = minValidSeq;
        roHiSeq = minValidSeq;
        riLoSeq = minValidSeq;
        lastQuoteTsMs = 0L;
        lastBestAskUp = 0.0;
        lastBestBidUp = 0.0;
        lastBestAskDown = 0.0;
        lastBestBidDown = 0.0;
        lastDepth1cUp = 0.0;
        lastDepth1cDown = 0.0;
        lastModelProbUp = Double.NaN;
        primaryDirection = TradeDirection.NO_TRADE;
        clipsFilledInMarket = 0;
        riskUsdCommitted = 0.0;
        lastOrderSubmitMs = -10_000L;
        orderInFlight = false;
        orderInFlightSinceMs = 0L;
        pendingDirection = TradeDirection.NO_TRADE;
        pendingEstAsk = 0.0;
        pendingReqShares = 0;
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
            long lastRefMs = Math.max(lastFastSpotMs, Math.max(lastPbReceiptMs, lastTwap60ReceiptMs));
            if (lastRefMs > 0L && (alignedStartMs < lastRefMs || (alignedStartMs - lastRefMs) > 120_000L)) {
                fmRingHead = 0;
                fmRingLen = 0;
                pbTotalSeq = 0L;
                pbRunningSum = 0.0;
                pbRingSrcMs[0] = 0L;
                pbRingCsum[0] = 0.0;
                lastFastSpotMs = 0L;
                lastFastSpotUsd = Double.NaN;
                lastPbReceiptMs = 0L;
                lastPbSourceMs = 0L;
                lastPbPriceUsd = Double.NaN;
                lastTwap60ReceiptMs = 0L;
                lastTwap60SourceMs = 0L;
                lastTwap60PriceUsd = Double.NaN;
                resetVolatilityState();
            }
            activeCandleStartMs = alignedStartMs;
            activeCandleEndMs = alignedStartMs + candleDurationMs;
            if (alignedStartMs == nextCandleStartMs && nextCandleTargetUsd > 0.0) {
                targetTwapOpenUsd = nextCandleTargetUsd;
                bestTargetOffsetAbsMs = 0L;
            } else {
                targetTwapOpenUsd = 0.0;
                bestTargetOffsetAbsMs = Long.MAX_VALUE;
            }
            nextCandleStartMs = 0L;
            nextCandleTargetUsd = 0.0;
            resetMarketState();
            if (!Double.isNaN(lastFastSpotUsd) && lastFastSpotUsd > 0.0) {
                candleOpenSpotUsd = lastFastSpotUsd;
                candleHighSpotUsd = lastFastSpotUsd;
                candleLowSpotUsd = lastFastSpotUsd;
            }
        }
    }

    @Override
    public void onFastSpotMicro(
            long timestampMs,
            String symbol,
            double fastDirectUsd,
            double signedOfi1s,
            double syncMove500Usd) {
        if (!Double.isFinite(fastDirectUsd) || fastDirectUsd <= 0.0 || timestampMs <= 0L) {
            return;
        }
        if (lastFastSpotMs > 0L && timestampMs < lastFastSpotMs) {
            return;
        }
        ensureActiveCandle(timestampMs);
        lastFastSpotMs = timestampMs;
        lastFastSpotUsd = fastDirectUsd;
        lastSignedOfi1s = signedOfi1s;
        lastSyncMove500Usd = syncMove500Usd;

        recordFastSpotSample(timestampMs, fastDirectUsd);
        updateVolatility2s(timestampMs, fastDirectUsd);
        stepModelAndEvaluate(timestampMs);
    }

    @Override
    public void onPolymarketCryptoPrice(
            long timestampMs,
            String symbol,
            String source,
            double price,
            long priceTimestampMs) {
        if (!Double.isFinite(price) || price <= 0.0 || timestampMs <= 0L) {
            return;
        }
        if (lastPbReceiptMs > 0L && timestampMs < lastPbReceiptMs) {
            return;
        }
        ensureActiveCandle(timestampMs);
        lastPbReceiptMs = timestampMs;

        long srcMs = priceTimestampMs > 0L ? priceTimestampMs : (timestampMs - 1_000L);
        if (srcMs >= lastPbSourceMs) {
            lastPbPriceUsd = price;
        }
        if (srcMs > lastPbSourceMs) {
            lastPbSourceMs = srcMs;
            pbTotalSeq++;
            pbRunningSum += price;
            int slot = (int) (pbTotalSeq & PB_RING_MASK);
            pbRingSrcMs[slot] = srcMs;
            pbRingCsum[slot] = pbRunningSum;
        }

        if (lastFastSpotMs <= 0L || (timestampMs - lastFastSpotMs) > 2_500L) {
            updateVolatility2s(timestampMs, lastPbPriceUsd);
        }
        stepModelAndEvaluate(timestampMs);
    }

    @Override
    public void onPolymarketCryptoTwap60s(
            long timestampMs,
            String symbol,
            String source,
            double twapPrice,
            int windowSeconds,
            long twapTimestampMs) {
        if (!Double.isFinite(twapPrice) || twapPrice <= 0.0 || timestampMs <= 0L) {
            return;
        }
        long srcMs = twapTimestampMs > 0L ? twapTimestampMs : timestampMs;
        if (activeCandleStartMs > 0L && srcMs == activeCandleEndMs) {
            nextCandleStartMs = activeCandleEndMs;
            nextCandleTargetUsd = twapPrice;
        }

        ensureActiveCandle(timestampMs);
        if (timestampMs >= lastTwap60ReceiptMs) {
            lastTwap60ReceiptMs = timestampMs;
        }
        if (srcMs >= lastTwap60SourceMs) {
            lastTwap60SourceMs = srcMs;
            lastTwap60PriceUsd = twapPrice;
        }

        long absOffset = Math.abs(srcMs - activeCandleStartMs);
        if (absOffset <= MAX_TARGET_OFFSET_MS && absOffset <= bestTargetOffsetAbsMs) {
            targetTwapOpenUsd = twapPrice;
            bestTargetOffsetAbsMs = absOffset;
        }
        stepModelAndEvaluate(timestampMs);
    }

    @Override
    public void onOrderBookQuote(OrderBookQuote quote) {
        if (quote == null) {
            return;
        }
        long nowMs = quote.timestampMs() > 0L
                ? quote.timestampMs()
                : Math.max(lastFastSpotMs, Math.max(lastPbReceiptMs, lastTwap60ReceiptMs));
        if (nowMs <= 0L) {
            return;
        }
        ensureActiveCandle(nowMs);

        lastQuoteTsMs = nowMs;
        lastBestAskUp = quote.bestAskUp();
        lastBestBidUp = quote.bestBidUp();
        lastBestAskDown = quote.bestAskDown();
        lastBestBidDown = quote.bestBidDown();
        lastDepth1cUp = quote.depth1cUp();
        lastDepth1cDown = quote.depth1cDown();

        stepModelAndEvaluate(nowMs);
    }

    @Override
    public double getLastModelProbabilityUp() {
        return lastModelProbUp;
    }

    @Override
    public void onExecutionReport(ExecutionReport report) {
        if (report == null) {
            return;
        }
        String status = report.status();
        if (status == null) {
            return;
        }
        if ("FILLED".equalsIgnoreCase(status) || "PARTIALLY_FILLED".equalsIgnoreCase(status)) {
            orderInFlight = false;
            double execSh = report.executedSize() != null ? report.executedSize().doubleValue() : pendingReqShares;
            double avgPx = report.executedPrice() != null && report.executedPrice().doubleValue() > 0.0
                    ? report.executedPrice().doubleValue()
                    : pendingEstAsk;
            if (execSh > 0.0) {
                if (primaryDirection == TradeDirection.NO_TRADE && pendingDirection != TradeDirection.NO_TRADE) {
                    primaryDirection = pendingDirection;
                }
                clipsFilledInMarket++;
                double feeUsd = execSh * config.takerFeeRate() * avgPx * Math.max(0.0, 1.0 - avgPx);
                riskUsdCommitted += execSh * avgPx + feeUsd;
            }
            pendingDirection = TradeDirection.NO_TRADE;
        } else if ("REJECTED".equalsIgnoreCase(status)
                || "CANCELED".equalsIgnoreCase(status)
                || "CANCELLED".equalsIgnoreCase(status)
                || "EXPIRED".equalsIgnoreCase(status)) {
            orderInFlight = false;
            pendingDirection = TradeDirection.NO_TRADE;
        }
    }

    @Override
    public void onCandleResolution(TradeDirection actualOutcome) {
        orderInFlight = false;
        pendingDirection = TradeDirection.NO_TRADE;
    }

    private void recordFastSpotSample(long timestampMs, double priceUsd) {
        if (fmRingLen > 0) {
            int lastSlot = (fmRingHead + fmRingLen - 1) & FM_RING_MASK;
            if ((timestampMs / 25L) == (fmRingTs[lastSlot] / 25L)) {
                fmRingTs[lastSlot] = timestampMs;
                fmRingPx[lastSlot] = priceUsd;
                return;
            }
        }
        if (fmRingLen == FM_RING_CAP) {
            fmRingHead = (fmRingHead + 1) & FM_RING_MASK;
            fmRingLen--;
        }
        int tail = (fmRingHead + fmRingLen) & FM_RING_MASK;
        fmRingTs[tail] = timestampMs;
        fmRingPx[tail] = priceUsd;
        fmRingLen++;
    }

    private double lookupFastSpotAtOrBefore(long targetMs, double fallbackUsd) {
        if (fmRingLen == 0) {
            return fallbackUsd;
        }
        if (fmRingTs[fmRingHead] > targetMs) {
            return fallbackUsd;
        }
        int lo = 0;
        int hi = fmRingLen - 1;
        int ans = 0;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int slot = (fmRingHead + mid) & FM_RING_MASK;
            if (fmRingTs[slot] <= targetMs) {
                ans = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return fmRingPx[(fmRingHead + ans) & FM_RING_MASK];
    }

    private void updateVolatility2s(long timestampMs, double spotUsd) {
        if (timestampMs >= activeCandleStartMs) {
            if (Double.isNaN(candleOpenSpotUsd)) {
                candleOpenSpotUsd = spotUsd;
            }
            if (spotUsd > candleHighSpotUsd) {
                candleHighSpotUsd = spotUsd;
            }
            if (spotUsd < candleLowSpotUsd) {
                candleLowSpotUsd = spotUsd;
            }
        }
        long b2s = timestampMs / 2_000L;
        if (b2s > lastVolBucket2s) {
            if (!Double.isNaN(prevVolSpotUsd) && prevVolSpotUsd > 0.0 && lastVolBucket2s >= 0L) {
                double dtSec = Math.max(2.0, (b2s - lastVolBucket2s) * 2.0);
                double r = Math.clamp((spotUsd - prevVolSpotUsd) / prevVolSpotUsd, -0.0040, 0.0040);
                double v1s = (r * r) / dtSec;
                var1sFast += ALPHA_VOL_FAST_2S * (v1s - var1sFast);
                var1sSlow += ALPHA_VOL_SLOW_2S * (v1s - var1sSlow);
            }
            prevVolSpotUsd = spotUsd;
            lastVolBucket2s = b2s;
        }
        double vBlend = 0.55 * var1sFast + 0.45 * var1sSlow;
        double vPark = 0.0;
        if (timestampMs >= activeCandleStartMs && candleHighSpotUsd > candleLowSpotUsd && candleLowSpotUsd > 0.0) {
            double elapSec = Math.max(30.0, (timestampMs - activeCandleStartMs) * 0.001);
            double rawHl = Math.log(candleHighSpotUsd / candleLowSpotUsd);
            double netDrift = (!Double.isNaN(candleOpenSpotUsd) && candleOpenSpotUsd > 0.0)
                    ? Math.abs(Math.log(spotUsd / candleOpenSpotUsd))
                    : 0.0;
            double adjHl = Math.max(0.0, rawHl - 0.85 * netDrift);
            vPark = (adjHl * adjHl) / (FOUR_LN_2 * elapSec);
        }
        double sigRatio = Math.max(minSigma1sRatio, Math.sqrt(Math.max(vBlend, 0.55 * vPark)));
        latestSig1sUsd = spotUsd * sigRatio;
    }

    private void stepModelAndEvaluate(long nowMs) {
        if (activeCandleEndMs <= 0L || targetTwapOpenUsd <= 0.0) {
            return;
        }
        long tauMs = activeCandleEndMs - nowMs;
        if (tauMs < 0L || tauMs > config.maxTauMs() + 3_000L) {
            return;
        }
        if (!balanceRefreshedForMarket && context != null && tauMs <= config.maxTauMs() + 10_000L) {
            balanceRefreshedForMarket = true;
            context.refreshAccountBalance();
        }
        if (Double.isNaN(lastFastSpotUsd) || Double.isNaN(lastPbPriceUsd) || Double.isNaN(lastTwap60PriceUsd)) {
            return;
        }
        if (lastQuoteTsMs <= 0L) {
            return;
        }

        double tauSec = tauMs * 0.001;
        boolean isLockIn = tauSec <= 60.0;

        // Layer 1: Origin-aligned basis-cancelled Chainlink spot nowcast
        long pbOriginEffMs = lastPbSourceMs + 50L;
        double spotAtPb = lookupFastSpotAtOrBefore(pbOriginEffMs, lastFastSpotUsd);
        double sHat = lastPbPriceUsd + config.spotLeadBeta() * (lastFastSpotUsd - spotAtPb);
        double sig1sUsd = Math.max(0.50, latestSig1sUsd);

        double projTwap;
        double sigRem;
        double bankedCushionUsd = 0.0;

        if (!isLockIn) {
            projTwap = sHat;
            sigRem = sig1sUsd * Math.sqrt(Math.max(1.0, tauSec - 39.5));
        } else {
            long win60StartMs = activeCandleEndMs - TWAP_WINDOW_MS;
            long roLoMs = lastTwap60SourceMs - TWAP_WINDOW_MS;
            if (win60StartMs > roLoMs && pbTotalSeq > 0L) {
                long minValidSeq = Math.max(1L, pbTotalSeq - (PB_RING_CAP - 4));
                if (roLoSeq < minValidSeq) roLoSeq = minValidSeq;
                if (roHiSeq < minValidSeq) roHiSeq = minValidSeq;
                if (riLoSeq < minValidSeq) riLoSeq = minValidSeq;

                while (roLoSeq <= pbTotalSeq && pbRingSrcMs[(int) (roLoSeq & PB_RING_MASK)] <= roLoMs) {
                    roLoSeq++;
                }
                while (roHiSeq <= pbTotalSeq && pbRingSrcMs[(int) (roHiSeq & PB_RING_MASK)] <= win60StartMs) {
                    roHiSeq++;
                }
                long iro1 = roHiSeq - 1L;
                if (roLoSeq <= iro1 && iro1 <= pbTotalSeq && (pbTotalSeq - roLoSeq) < (PB_RING_CAP - 4)) {
                    int cntOut = (int) (iro1 - roLoSeq + 1L);
                    double sumOut = csumAt(iro1) - csumAt(roLoSeq - 1L);
                    while (riLoSeq <= pbTotalSeq && pbRingSrcMs[(int) (riLoSeq & PB_RING_MASK)] <= lastTwap60SourceMs) {
                        riLoSeq++;
                    }
                    int cntIn = riLoSeq <= pbTotalSeq ? (int) (pbTotalSeq - riLoSeq + 1L) : 0;
                    double sumIn = riLoSeq <= pbTotalSeq ? (csumAt(pbTotalSeq) - csumAt(riLoSeq - 1L)) : 0.0;
                    int remS = Math.max(0, cntOut - cntIn);
                    projTwap = lastTwap60PriceUsd + (sumIn + remS * sHat - sumOut) / 60.0;
                    bankedCushionUsd = (lastTwap60PriceUsd - targetTwapOpenUsd)
                            + (sumIn - sumOut + remS * targetTwapOpenUsd) / 60.0;
                } else {
                    double wLocked = 1.0 - (tauSec / 60.0);
                    projTwap = wLocked * lastTwap60PriceUsd + (tauSec / 60.0) * sHat;
                    bankedCushionUsd = wLocked * (lastTwap60PriceUsd - targetTwapOpenUsd);
                }
            } else {
                projTwap = lastTwap60PriceUsd;
                bankedCushionUsd = lastTwap60PriceUsd - targetTwapOpenUsd;
            }
            double pathSig = sig1sUsd * Math.sqrt(tauSec * (tauSec + 1.0) * (2.0 * tauSec + 1.0) / 6.0) / 60.0;
            double oracleFloor = config.sigmaFloorLockInUsd() * (0.35 + 0.65 * (tauSec / 60.0));
            sigRem = Math.sqrt(pathSig * pathSig + oracleFloor * oracleFloor);
        }

        double zRaw = (projTwap - targetTwapOpenUsd) / Math.max(0.20, sigRem);
        double u4 = zRaw / Math.sqrt(4.0 + zRaw * zRaw);
        double rawCdfUp = 0.5 + 0.25 * u4 * (3.0 - u4 * u4);
        double jumpHazard = 0.0035 + 0.0105 * Math.min(1.0, tauSec / 120.0);
        double pStructUp = Math.clamp(jumpHazard + (1.0 - 2.0 * jumpHazard) * rawCdfUp, 0.004, 0.996);

        // Layer 2: Regularized microstructure log-odds adjustment
        double invSig = 1.0 / sig1sUsd;
        double syncNorm = Math.clamp(lastSyncMove500Usd * invSig, -2.0, 2.0);
        double logitUp = Math.log(pStructUp / (1.0 - pStructUp))
                + config.ofiLogitWeight() * lastSignedOfi1s
                + config.syncLogitWeight() * syncNorm;
        double pFinalUp = Math.clamp(1.0 / (1.0 + Math.exp(-logitUp)), 0.004, 0.996);
        this.lastModelProbUp = pFinalUp;

        pushHistorySample(nowMs, pFinalUp, lastBestAskUp, lastBestAskDown);

        if (orderInFlight) {
            if ((nowMs - orderInFlightSinceMs) <= ORDER_IN_FLIGHT_TIMEOUT_MS) {
                return;
            }
            orderInFlight = false;
            pendingDirection = TradeDirection.NO_TRADE;
        }
        if (context == null || tauMs < config.minTauMs() || tauMs > config.maxTauMs() || histRingLen < 2) {
            return;
        }
        if ((nowMs - lastQuoteTsMs) > config.maxQuoteAgeMs()) {
            return;
        }
        if (clipsFilledInMarket >= config.maxClipsPerMarket()) {
            return;
        }

        long cooldownMs = !isLockIn
                ? config.cooldownPre60Ms()
                : (tauSec > 35.0 ? config.cooldownMidLockMs() : config.cooldownLateLockMs());
        if ((nowMs - lastOrderSubmitMs) < cooldownMs) {
            return;
        }

        int idx450 = histRingHead;
        int idx1500 = histRingHead;
        long target450Ms = nowMs - 450L;
        for (int ri = histRingLen - 1; ri >= 0; ri--) {
            int slot = (histRingHead + ri) & HIST_RING_MASK;
            if (histRingTs[slot] <= target450Ms) {
                idx450 = slot;
                break;
            }
        }

        double spot400 = lookupFastSpotAtOrBefore(nowMs - 400L, lastFastSpotUsd);
        double spot1500 = lookupFastSpotAtOrBefore(nowMs - 1_500L, lastFastSpotUsd);
        double imp400Sigma = (lastFastSpotUsd - spot400) * invSig;
        double imp1500Sigma = (lastFastSpotUsd - spot1500) * invSig;
        double spotLeadOverPbSigma = (sHat - lastPbPriceUsd) * invSig;

        for (int side = 0; side < 2; side++) {
            boolean buyUp = (side == 0);
            TradeDirection dir = buyUp ? TradeDirection.UP : TradeDirection.DOWN;
            if (primaryDirection != TradeDirection.NO_TRADE && primaryDirection != dir) {
                continue;
            }

            double dirSign = buyUp ? 1.0 : -1.0;
            double pSide = buyUp ? pFinalUp : (1.0 - pFinalUp);
            if (pSide < config.minModelProb()) {
                continue;
            }

            double bankedSigma = (dirSign * bankedCushionUsd) / Math.max(0.20, sigRem);
            double totalLeadSigma = dirSign * zRaw;
            boolean isCushionLocked = isLockIn
                    && tauSec <= 45.0
                    && bankedSigma >= config.minBankedCushionSigma()
                    && totalLeadSigma >= (config.minBankedCushionSigma() + 0.55)
                    && (dirSign * imp1500Sigma) > -2.5;

            if (!isCushionLocked) {
                if ((dirSign * lastSignedOfi1s) < config.toxicOfiVeto()) {
                    continue;
                }
                if ((dirSign * imp1500Sigma) < -0.9) {
                    continue;
                }
            } else if ((dirSign * lastSignedOfi1s) < -0.45) {
                continue;
            }

            double askP = buyUp ? lastBestAskUp : lastBestAskDown;
            double bidP = buyUp ? lastBestBidUp : lastBestBidDown;
            double depth1c = buyUp ? lastDepth1cUp : lastDepth1cDown;
            if (askP <= 0.0 || bidP <= 0.0 || askP <= bidP
                    || (askP - bidP) > config.maxSpread()
                    || depth1c < config.minDepthShares()) {
                continue;
            }
            if (askP < config.minEntryAsk()) {
                continue;
            }
            double allowedMaxAsk = isCushionLocked
                    ? Math.min(0.91, config.maxEntryAskLockIn() + 0.04)
                    : (isLockIn ? config.maxEntryAskLockIn() : config.maxEntryAskPre60());
            if (askP > allowedMaxAsk) {
                continue;
            }

            if (!isCushionLocked) {
                double pPrev450 = buyUp ? histRingPUp[idx450] : (1.0 - histRingPUp[idx450]);
                double pPrev1500 = buyUp ? histRingPUp[idx1500] : (1.0 - histRingPUp[idx1500]);
                double askPrev450 = buyUp ? histRingAskU[idx450] : histRingAskD[idx450];
                double askPrev1500 = buyUp ? histRingAskU[idx1500] : histRingAskD[idx1500];

                double unrep450 = (pSide - pPrev450) - Math.max(0.0, askP - askPrev450);
                double unrep1500 = (pSide - pPrev1500) - Math.max(0.0, askP - askPrev1500);
                double bestUnrep = Math.max(unrep450, unrep1500);

                double bestSpotImp = Math.max(dirSign * spotLeadOverPbSigma,
                        Math.max(dirSign * imp400Sigma, dirSign * imp1500Sigma));

                double horizonMult = !isLockIn ? 1.05 : 0.85;
                if (bestUnrep < config.minFreshUnrepricedEdge() * horizonMult) {
                    continue;
                }
                if (bestSpotImp < config.minSpotImpulseSigma() * horizonMult) {
                    continue;
                }
            }

            double feePerSh = config.takerFeeRate() * askP * (1.0 - askP);
            double effCost = askP + feePerSh;
            if (effCost >= 0.965 || (pSide - effCost) < 0.025) {
                continue;
            }

            double logitEdge = Math.log(pSide / (1.0 - pSide)) - Math.log(effCost / (1.0 - effCost));
            double reqLogit;
            if (!isLockIn) {
                reqLogit = config.minLogitEdgePre60();
            } else if (tauSec <= 25.0) {
                reqLogit = config.minLogitEdgeLate();
            } else {
                double wPre = (tauSec - 25.0) / 35.0;
                reqLogit = config.minLogitEdgeLate() + wPre * (config.minLogitEdgePre60() - config.minLogitEdgeLate());
            }
            if (isLockIn && (dirSign * (lastTwap60PriceUsd - targetTwapOpenUsd)) < -1.0 * sig1sUsd) {
                reqLogit += 0.25;
            }
            if (logitEdge < reqLogit) {
                continue;
            }

            // Bayesian Epistemic-Variance-Adjusted Kelly sizing
            double totalVar = pSide * (1.0 - pSide) + config.epistemicVariance();
            double kellyFrac = Math.max(0.0, (effCost * (pSide - effCost)) / totalVar);
            double lockProgress = !isLockIn
                    ? 0.28
                    : (tauSec >= 35.0 ? (0.72 + 0.28 * (60.0 - tauSec) / 25.0) : 1.00);
            double capUsd = config.maxRiskPerMarketUsd() * lockProgress;
            double targetRiskUsd = Math.min(capUsd, lockProgress * config.kellyBankrollUsd() * kellyFrac);
            double remRiskUsd = targetRiskUsd - riskUsdCommitted;
            if (remRiskUsd < 8.0) {
                continue;
            }

            double maxSingleClipUsd = !isLockIn ? 75.0 : (tauSec <= 35.0 ? 210.0 : 145.0);
            double clipUsd = Math.min(remRiskUsd, maxSingleClipUsd);
            int rawSh = (int) Math.floor(clipUsd / effCost);
            int depthCapSh = (int) (depth1c * 0.85);
            int reqShares = Math.min(config.maxOrderShares(), Math.min(depthCapSh, rawSh));
            if (reqShares < config.minOrderShares()) {
                continue;
            }

            int askCeilMc = ((int) Math.round(askP * 1_000.0) + TICK_STEP_MC - 1) / TICK_STEP_MC * TICK_STEP_MC;
            int maxEdgeMc = ((int) Math.floor((pSide - feePerSh - 0.010) * 1_000.0) / TICK_STEP_MC) * TICK_STEP_MC;
            int limitMc = Math.clamp(Math.min(askCeilMc + TICK_STEP_MC, maxEdgeMc), TICK_STEP_MC, MAX_MC - TICK_STEP_MC);
            if (limitMc < (int) Math.round(askP * 1_000.0)) {
                continue;
            }

            double limitPrice = limitMc * 1e-3;
            double fillableSh = context.estimateBuyFillableShares(dir, limitPrice);
            if (Double.isFinite(fillableSh)) {
                reqShares = Math.min(reqShares, (int) Math.floor(fillableSh));
                if (reqShares < config.minOrderShares()) {
                    continue;
                }
            }
            double estVwap = context.estimateBuyFillVwap(dir, reqShares, limitPrice);
            if (Double.isNaN(estVwap)) {
                continue;
            }
            double effFillP = estVwap > 0.0 ? estVwap : askP;
            if (effFillP > askP) {
                double sweepFee = config.takerFeeRate() * effFillP * (1.0 - effFillP);
                if ((pSide - effFillP - sweepFee) < 0.015) {
                    continue;
                }
            }

            submitBuyOrder(nowMs, dir, askP, limitMc, reqShares, pSide, logitEdge, isCushionLocked);
            return;
        }
    }

    private double csumAt(long seq) {
        return seq <= 0L ? 0.0 : pbRingCsum[(int) (seq & PB_RING_MASK)];
    }

    private void pushHistorySample(long nowMs, double pFinalUp, double askUp, double askDown) {
        if ((nowMs - lastHistPushMs) < 25L) {
            return;
        }
        if (histRingLen == HIST_RING_CAP) {
            histRingHead = (histRingHead + 1) & HIST_RING_MASK;
            histRingLen--;
        }
        int tail = (histRingHead + histRingLen) & HIST_RING_MASK;
        histRingTs[tail] = nowMs;
        histRingPUp[tail] = pFinalUp;
        histRingAskU[tail] = askUp > 0.0 ? askUp : 1.0;
        histRingAskD[tail] = askDown > 0.0 ? askDown : 1.0;
        histRingLen++;
        lastHistPushMs = nowMs;
        while (histRingLen > 2 && (nowMs - histRingTs[histRingHead]) > 2_200L) {
            histRingHead = (histRingHead + 1) & HIST_RING_MASK;
            histRingLen--;
        }
    }

    private void submitBuyOrder(
            long nowMs,
            TradeDirection direction,
            double askP,
            int limitPriceMc,
            int shares,
            double pSide,
            double logitEdge,
            boolean cushionLocked) {
        String clientOrderId = "opl-" + activeCandleStartMs + "-" + (++orderSequence);
        String token = direction == TradeDirection.UP ? config.upToken() : config.downToken();
        BigDecimal price = PRICE_MC_CACHE[limitPriceMc];
        BigDecimal size = shares <= MAX_CACHED_SHARES ? SHARE_SIZE_CACHE[shares] : BigDecimal.valueOf(shares);

        this.orderInFlight = true;
        this.orderInFlightSinceMs = nowMs;
        this.pendingDirection = direction;
        this.pendingEstAsk = askP;
        this.pendingReqShares = shares;
        this.lastOrderSubmitMs = nowMs;

        if (Log.isDebugEnabled()) {
            Log.debugf("%s BUY %s: ts=%d, ask=%.4f, limitMc=%d, shares=%d, pSide=%.4f, logitEdge=%.3f, cushionLocked=%s",
                    getName(), direction, nowMs, askP, limitPriceMc, shares, pSide, logitEdge, cushionLocked);
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
}
