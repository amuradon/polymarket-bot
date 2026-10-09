package cz.polymarket.bot.backtest.stream;

import cz.polymarket.bot.calculator.RealizedVolatilityCalculator;
import cz.polymarket.bot.calculator.VwapCalculator;
import cz.polymarket.bot.domain.MarketCandle;
import cz.polymarket.bot.strategy.IndicatorType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Dynamic technical indicator calculation engine.
 * Computes on-the-fly intra-candle indicators for declared requirements of active strategies,
 * while historical whole-candle indicators are initialized from cached history.
 */
public class DynamicIndicatorEngine {

    private final Set<IndicatorType> requiredIndicators;
    private final RealizedVolatilityCalculator volCalculator = new RealizedVolatilityCalculator();
    private final VwapCalculator vwapCalculator = new VwapCalculator();

    private final List<MarketCandle> candleHistory = new ArrayList<>();
    private final Map<IndicatorType, Double> currentValues = new EnumMap<>(IndicatorType.class);

    // Active candle intra-candle state
    private long activeCandleStartMs = 0L;
    private double twapOpenPrice = 0.0;
    private double rollingTwapSum = 0.0;
    private long rollingTwapCount = 0L;

    private double rollingVwapUsd = 0.0;
    private double rollingVwapBtc = 0.0;

    private double intraCandleCvdBtc = 0.0;

    private double currentSpotPrice = 0.0;
    private double currentFuturesPrice = 0.0;
    private double currentBasisBps = 0.0;

    private double currentObi = 0.0;
    private double currentMicroPrice = 0.0;

    public DynamicIndicatorEngine(Set<IndicatorType> requiredIndicators) {
        this.requiredIndicators = (requiredIndicators != null) ? requiredIndicators : Collections.emptySet();
        reset();
    }

    public void reset() {
        candleHistory.clear();
        currentValues.clear();
        resetActiveCandle(0L, 0.0);
    }

    public void resetActiveCandle(long startMs, double openPrice) {
        this.activeCandleStartMs = startMs;
        this.twapOpenPrice = openPrice;
        this.rollingTwapSum = openPrice > 0 ? openPrice : 0.0;
        this.rollingTwapCount = openPrice > 0 ? 1L : 0L;
        this.rollingVwapUsd = 0.0;
        this.rollingVwapBtc = 0.0;
        this.intraCandleCvdBtc = 0.0;
        recalculateAll();
    }

    public void onMarketCandleCompleted(MarketCandle candle) {
        if (candle != null) {
            candleHistory.add(candle);
            recalculateAll();
        }
    }

    public void onSpotTrade(long timestampMs, double price, double quantity, boolean isBuyerMaker) {
        this.currentSpotPrice = price;

        if (requiredIndicators.contains(IndicatorType.TWAP)) {
            this.rollingTwapSum += price;
            this.rollingTwapCount++;
            double rollingTwap = rollingTwapSum / rollingTwapCount;
            currentValues.put(IndicatorType.TWAP, rollingTwap);
        }

        if (requiredIndicators.contains(IndicatorType.VWAP)) {
            this.rollingVwapUsd += (price * quantity);
            this.rollingVwapBtc += quantity;
            if (this.rollingVwapBtc > 0.0) {
                currentValues.put(IndicatorType.VWAP, this.rollingVwapUsd / this.rollingVwapBtc);
            }
        }

        if (requiredIndicators.contains(IndicatorType.VWAP_ZSCORE)) {
            VwapCalculator.VwapResult res = vwapCalculator.calculate(candleHistory, price);
            currentValues.put(IndicatorType.VWAP_ZSCORE, res.zScore());
        }

        if (requiredIndicators.contains(IndicatorType.CVD)) {
            double delta = isBuyerMaker ? -quantity : quantity;
            this.intraCandleCvdBtc += delta;
            currentValues.put(IndicatorType.CVD, this.intraCandleCvdBtc);
        }

        if (requiredIndicators.contains(IndicatorType.BASIS) && currentFuturesPrice > 0.0 && currentSpotPrice > 0.0) {
            this.currentBasisBps = ((currentFuturesPrice - currentSpotPrice) / currentSpotPrice) * 10000.0;
            currentValues.put(IndicatorType.BASIS, this.currentBasisBps);
        }
    }

    public void onFuturesTrade(long timestampMs, double price, double quantity, boolean isBuyerMaker) {
        this.currentFuturesPrice = price;

        if (requiredIndicators.contains(IndicatorType.BASIS) && currentSpotPrice > 0.0) {
            this.currentBasisBps = ((price - currentSpotPrice) / currentSpotPrice) * 10000.0;
            currentValues.put(IndicatorType.BASIS, this.currentBasisBps);
        }
    }

    public void onFuturesOrderBook(long timestampMs, double bestBid, double bestAsk, double depthBids, double depthAsks, double obi) {
        if (requiredIndicators.contains(IndicatorType.ORDER_BOOK_IMBALANCE)) {
            this.currentObi = obi;
            currentValues.put(IndicatorType.ORDER_BOOK_IMBALANCE, obi);
        }

        if (requiredIndicators.contains(IndicatorType.MICRO_PRICE)) {
            double totalDepth = depthBids + depthAsks;
            if (totalDepth > 0.0) {
                this.currentMicroPrice = (depthAsks * bestBid + depthBids * bestAsk) / totalDepth;
            } else {
                this.currentMicroPrice = (bestBid + bestAsk) / 2.0;
            }
            currentValues.put(IndicatorType.MICRO_PRICE, this.currentMicroPrice);
        }
    }

    private void recalculateAll() {
        if (requiredIndicators.contains(IndicatorType.VOLATILITY_4H)) {
            double vol4h = volCalculator.calculate4hRealizedVolatility(candleHistory);
            currentValues.put(IndicatorType.VOLATILITY_4H, vol4h);
        }

        if (requiredIndicators.contains(IndicatorType.VWAP_ZSCORE)) {
            double spot = currentSpotPrice > 0.0 ? currentSpotPrice : twapOpenPrice;
            VwapCalculator.VwapResult res = vwapCalculator.calculate(candleHistory, spot);
            currentValues.put(IndicatorType.VWAP_ZSCORE, res.zScore());
        }

        if (requiredIndicators.contains(IndicatorType.TWAP) && rollingTwapCount > 0) {
            currentValues.put(IndicatorType.TWAP, rollingTwapSum / rollingTwapCount);
        }

        if (requiredIndicators.contains(IndicatorType.BASIS) && currentSpotPrice > 0.0 && currentFuturesPrice > 0.0) {
            currentValues.put(IndicatorType.BASIS, ((currentFuturesPrice - currentSpotPrice) / currentSpotPrice) * 10000.0);
        }

        if (requiredIndicators.contains(IndicatorType.CVD)) {
            currentValues.put(IndicatorType.CVD, intraCandleCvdBtc);
        }
    }

    public double getIndicatorValue(IndicatorType type) {
        return currentValues.getOrDefault(type, Double.NaN);
    }

    public Map<IndicatorType, Double> getAllIndicators() {
        return Collections.unmodifiableMap(currentValues);
    }

    public double getCurrentSpotPrice() {
        return currentSpotPrice;
    }

    public double getCurrentFuturesPrice() {
        return currentFuturesPrice;
    }

    public double getCurrentBasisBps() {
        return currentBasisBps;
    }

    public double getTwapOpenPrice() {
        return twapOpenPrice;
    }
}
