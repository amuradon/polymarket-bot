package cz.polymarket.bot.backtest.stream;

import cz.polymarket.bot.calculator.RealizedVolatilityCalculator;
import cz.polymarket.bot.calculator.VwapCalculator;
import cz.polymarket.bot.domain.MarketCandle;
import cz.polymarket.bot.strategy.IndicatorType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * High-performance dynamic technical indicator calculation engine.
 * <p>
 * Computes on-the-fly intra-candle indicators strictly from Spot and Futures exchange streams
 * for the indicator types declared by the active strategy. Polymarket streams are never mixed
 * into Spot/Futures indicator calculations.
 */
public class DynamicIndicatorEngine {

    private static final int INDICATOR_COUNT = IndicatorType.values().length;

    private final EnumSet<IndicatorType> requiredIndicators;
    private final boolean hasAnyIndicators;
    private final boolean needTwap;
    private final boolean needVwap;
    private final boolean needVwapZScore;
    private final boolean needCvd;
    private final boolean needBasis;
    private final boolean needVol4h;
    private final boolean needObi;
    private final boolean needMicroPrice;

    private final RealizedVolatilityCalculator volCalculator = new RealizedVolatilityCalculator();
    private final VwapCalculator vwapCalculator = new VwapCalculator();

    private final List<MarketCandle> candleHistory = new ArrayList<>();
    private final double[] currentValues = new double[INDICATOR_COUNT];

    // Precomputed historical 24h VWAP & standard deviation for O(1) Z-score updates on every spot trade
    private boolean hasHistoricalVwap = false;
    private double cachedHistoricalVwap = 0.0;
    private double cachedHistoricalStdDev = 1.0;

    // Active candle intra-candle state
    private double rollingTwapSum = 0.0;
    private long rollingTwapCount = 0L;

    private double rollingVwapUsd = 0.0;
    private double rollingVwapBtc = 0.0;
    private double intraCandleCvdBtc = 0.0;

    private double currentSpotPrice = 0.0;
    private double currentFuturesPrice = 0.0;

    // Active candle Spot/Futures OHLCV accumulators
    private double sOpen = 0.0;
    private double sHigh = 0.0;
    private double sLow = 0.0;
    private double sClose = 0.0;
    private double spotVolBtc = 0.0;
    private double spotVolUsd = 0.0;
    private double spotDeltaBtc = 0.0;

    private double fOpen = 0.0;
    private double fHigh = 0.0;
    private double fLow = 0.0;
    private double fClose = 0.0;
    private double futVolBtc = 0.0;
    private double futVolUsd = 0.0;
    private double futDeltaBtc = 0.0;

    /**
     * Constructs the dynamic indicator engine configured with the indicators required by the active strategy.
     *
     * @param requiredIndicators set of indicator types to compute
     */
    public DynamicIndicatorEngine(Set<IndicatorType> requiredIndicators) {
        if (requiredIndicators == null || requiredIndicators.isEmpty()) {
            this.requiredIndicators = EnumSet.noneOf(IndicatorType.class);
        } else {
            this.requiredIndicators = EnumSet.copyOf(requiredIndicators);
        }
        this.hasAnyIndicators = !this.requiredIndicators.isEmpty();
        this.needTwap = this.requiredIndicators.contains(IndicatorType.TWAP);
        this.needVwap = this.requiredIndicators.contains(IndicatorType.VWAP);
        this.needVwapZScore = this.requiredIndicators.contains(IndicatorType.VWAP_ZSCORE);
        this.needCvd = this.requiredIndicators.contains(IndicatorType.CVD);
        this.needBasis = this.requiredIndicators.contains(IndicatorType.BASIS);
        this.needVol4h = this.requiredIndicators.contains(IndicatorType.VOLATILITY_4H);
        this.needObi = this.requiredIndicators.contains(IndicatorType.ORDER_BOOK_IMBALANCE);
        this.needMicroPrice = this.requiredIndicators.contains(IndicatorType.MICRO_PRICE);
        reset();
    }

    /**
     * Resets the entire engine state including historical candles and computed indicator caches.
     */
    public void reset() {
        candleHistory.clear();
        hasHistoricalVwap = false;
        cachedHistoricalVwap = 0.0;
        cachedHistoricalStdDev = 1.0;
        Arrays.fill(currentValues, Double.NaN);
        resetActiveCandle();
    }

    /**
     * Replaces the historical completed candle list (used when initializing isolated parallel market workers)
     * and precomputes the rolling historical VWAP and standard deviation once per candle.
     *
     * @param priorCandles chronological list of completed candles preceding the active market
     */
    public void setCandleHistory(List<MarketCandle> priorCandles) {
        candleHistory.clear();
        if (priorCandles != null && !priorCandles.isEmpty()) {
            int size = priorCandles.size();
            int from = Math.max(0, size - VwapCalculator.MAX_WINDOW_CANDLES);
            candleHistory.addAll(priorCandles.subList(from, size));
        }
        if (hasAnyIndicators) {
            refreshHistoricalVwapCache();
            recalculateAll();
        }
    }

    /**
     * Resets intra-candle rolling accumulators for a new active candle interval.
     */
    public void resetActiveCandle() {
        this.rollingTwapSum = 0.0;
        this.rollingTwapCount = 0L;
        this.rollingVwapUsd = 0.0;
        this.rollingVwapBtc = 0.0;
        this.intraCandleCvdBtc = 0.0;
        this.currentSpotPrice = 0.0;
        this.currentFuturesPrice = 0.0;

        this.sOpen = 0.0;
        this.sHigh = 0.0;
        this.sLow = 0.0;
        this.sClose = 0.0;
        this.spotVolBtc = 0.0;
        this.spotVolUsd = 0.0;
        this.spotDeltaBtc = 0.0;

        this.fOpen = 0.0;
        this.fHigh = 0.0;
        this.fLow = 0.0;
        this.fClose = 0.0;
        this.futVolBtc = 0.0;
        this.futVolUsd = 0.0;
        this.futDeltaBtc = 0.0;

        if (hasAnyIndicators) {
            Arrays.fill(currentValues, Double.NaN);
            recalculateAll();
        }
    }

    /**
     * Appends a completed market candle to historical memory and recalculates multi-candle indicators.
     *
     * @param candle completed domain market candle
     */
    public void onMarketCandleCompleted(MarketCandle candle) {
        if (candle != null) {
            candleHistory.add(candle);
            if (candleHistory.size() > VwapCalculator.MAX_WINDOW_CANDLES) {
                candleHistory.removeFirst();
            }
            if (hasAnyIndicators) {
                refreshHistoricalVwapCache();
                recalculateAll();
            }
        }
    }

    /**
     * Builds a completed {@link MarketCandle} for the current 15-minute interval from accumulated
     * Spot and Futures exchange stream data, or returns {@code null} if no spot exchange data was observed.
     *
     * @param tStart candle start epoch seconds
     * @param tEnd candle end epoch seconds
     * @return constructed MarketCandle, or {@code null} when no spot trades occurred
     */
    public MarketCandle buildCompletedCandle(long tStart, long tEnd) {
        if (sOpen <= 0.0 || sHigh <= 0.0 || sLow <= 0.0 || sClose <= 0.0) {
            return null;
        }
        double basisBps = (sClose > 0.0 && fClose > 0.0)
                ? ((fClose - sClose) / sClose) * 10000.0
                : 0.0;
        return new MarketCandle(
                tStart,
                tEnd,
                sOpen, sHigh, sLow, sClose,
                spotVolBtc, spotVolUsd, spotDeltaBtc,
                fOpen > 0.0 ? fOpen : sOpen,
                fHigh > 0.0 ? fHigh : sHigh,
                fLow > 0.0 ? fLow : sLow,
                fClose > 0.0 ? fClose : sClose,
                futVolBtc, futVolUsd, futDeltaBtc,
                basisBps
        );
    }

    /**
     * Updates rolling price, TWAP, VWAP, VWAP Z-score, CVD, and Basis indicators in {@code O(1)}
     * upon receiving a spot exchange trade.
     *
     * @param timestampMs event receipt timestamp in milliseconds UTC
     * @param price spot execution price
     * @param quantity trade volume in base asset
     * @param isBuyerMaker true if maker was buyer (aggressive sell), false if aggressive buy
     */
    public void onSpotTrade(long timestampMs, double price, double quantity, boolean isBuyerMaker) {
        this.currentSpotPrice = price;
        if (sOpen <= 0.0) {
            sOpen = price;
            sHigh = price;
            sLow = price;
        } else {
            if (price > sHigh) sHigh = price;
            if (price < sLow) sLow = price;
        }
        sClose = price;

        double usd = price * quantity;
        double delta = isBuyerMaker ? -quantity : quantity;
        this.spotVolBtc += quantity;
        this.spotVolUsd += usd;
        this.spotDeltaBtc += delta;

        if (!hasAnyIndicators) {
            return;
        }

        if (needTwap) {
            this.rollingTwapSum += price;
            this.rollingTwapCount++;
            currentValues[IndicatorType.TWAP.ordinal()] = rollingTwapSum / rollingTwapCount;
        }

        if (needVwap) {
            this.rollingVwapUsd += usd;
            this.rollingVwapBtc += quantity;
            if (this.rollingVwapBtc > 0.0) {
                currentValues[IndicatorType.VWAP.ordinal()] = this.rollingVwapUsd / this.rollingVwapBtc;
            }
        }

        if (needVwapZScore) {
            currentValues[IndicatorType.VWAP_ZSCORE.ordinal()] = hasHistoricalVwap
                    ? (price - cachedHistoricalVwap) / cachedHistoricalStdDev
                    : 0.0;
        }

        if (needCvd) {
            this.intraCandleCvdBtc += delta;
            currentValues[IndicatorType.CVD.ordinal()] = this.intraCandleCvdBtc;
        }

        if (needBasis && currentFuturesPrice > 0.0 && currentSpotPrice > 0.0) {
            currentValues[IndicatorType.BASIS.ordinal()] =
                    ((currentFuturesPrice - currentSpotPrice) / currentSpotPrice) * 10000.0;
        }
    }

    /**
     * Updates futures reference price and basis spread upon receiving a futures exchange trade.
     *
     * @param timestampMs event receipt timestamp in milliseconds UTC
     * @param price futures execution price
     * @param quantity trade volume
     * @param isBuyerMaker true if maker was buyer, false if aggressive buy
     */
    public void onFuturesTrade(long timestampMs, double price, double quantity, boolean isBuyerMaker) {
        this.currentFuturesPrice = price;
        if (fOpen <= 0.0) {
            fOpen = price;
            fHigh = price;
            fLow = price;
        } else {
            if (price > fHigh) fHigh = price;
            if (price < fLow) fLow = price;
        }
        fClose = price;
        this.futVolBtc += quantity;
        this.futVolUsd += price * quantity;
        this.futDeltaBtc += isBuyerMaker ? -quantity : quantity;

        if (needBasis && currentSpotPrice > 0.0) {
            currentValues[IndicatorType.BASIS.ordinal()] =
                    ((price - currentSpotPrice) / currentSpotPrice) * 10000.0;
        }
    }

    /**
     * Updates order book imbalance (OBI) and depth-weighted micro price from futures book depth.
     *
     * @param timestampMs event receipt timestamp in milliseconds UTC
     * @param bestBid best bid price
     * @param bestAsk best ask price
     * @param depthBids total depth of top bids
     * @param depthAsks total depth of top asks
     * @param obi order book imbalance ratio
     */
    public void onFuturesOrderBook(long timestampMs, double bestBid, double bestAsk, double depthBids, double depthAsks, double obi) {
        if (needObi) {
            currentValues[IndicatorType.ORDER_BOOK_IMBALANCE.ordinal()] = obi;
        }

        if (needMicroPrice) {
            double totalDepth = depthBids + depthAsks;
            double currentMicroPrice = totalDepth > 0.0
                    ? (depthAsks * bestBid + depthBids * bestAsk) / totalDepth
                    : (bestBid + bestAsk) / 2.0;
            currentValues[IndicatorType.MICRO_PRICE.ordinal()] = currentMicroPrice;
        }
    }

    private void refreshHistoricalVwapCache() {
        if (!needVwapZScore || candleHistory.isEmpty()) {
            hasHistoricalVwap = false;
            cachedHistoricalVwap = 0.0;
            cachedHistoricalStdDev = 1.0;
            return;
        }
        VwapCalculator.VwapResult r0 = vwapCalculator.calculate(candleHistory, 0.0);
        VwapCalculator.VwapResult r1 = vwapCalculator.calculate(candleHistory, 1.0);
        if (r0.vwap() == r1.vwap()) {
            hasHistoricalVwap = true;
            cachedHistoricalVwap = r0.vwap();
            cachedHistoricalStdDev = r0.stdDev();
        } else {
            hasHistoricalVwap = false;
            cachedHistoricalVwap = 0.0;
            cachedHistoricalStdDev = 1.0;
        }
    }

    private void recalculateAll() {
        if (needVol4h) {
            double vol4h = volCalculator.calculate4hRealizedVolatility(candleHistory);
            currentValues[IndicatorType.VOLATILITY_4H.ordinal()] = vol4h;
        }

        if (needVwapZScore) {
            currentValues[IndicatorType.VWAP_ZSCORE.ordinal()] = (hasHistoricalVwap && currentSpotPrice > 0.0)
                    ? (currentSpotPrice - cachedHistoricalVwap) / cachedHistoricalStdDev
                    : 0.0;
        }

        if (needTwap && rollingTwapCount > 0) {
            currentValues[IndicatorType.TWAP.ordinal()] = rollingTwapSum / rollingTwapCount;
        }

        if (needBasis && currentSpotPrice > 0.0 && currentFuturesPrice > 0.0) {
            currentValues[IndicatorType.BASIS.ordinal()] =
                    ((currentFuturesPrice - currentSpotPrice) / currentSpotPrice) * 10000.0;
        }

        if (needCvd) {
            currentValues[IndicatorType.CVD.ordinal()] = intraCandleCvdBtc;
        }
    }

    /**
     * Returns the current on-the-fly value of the requested indicator type.
     *
     * @param type indicator type
     * @return indicator value, or {@link Double#NaN} if uncomputed
     */
    public double getIndicatorValue(IndicatorType type) {
        return type != null ? currentValues[type.ordinal()] : Double.NaN;
    }

    /**
     * Returns an unmodifiable snapshot of all currently computed indicators.
     *
     * @return map of indicator types to values
     */
    public Map<IndicatorType, Double> getAllIndicators() {
        Map<IndicatorType, Double> map = new EnumMap<>(IndicatorType.class);
        for (IndicatorType type : requiredIndicators) {
            double v = currentValues[type.ordinal()];
            if (!Double.isNaN(v)) {
                map.put(type, v);
            }
        }
        return Collections.unmodifiableMap(map);
    }

    /**
     * Returns the latest observed spot asset price.
     *
     * @return current spot price
     */
    public double getCurrentSpotPrice() {
        return currentSpotPrice;
    }
}
