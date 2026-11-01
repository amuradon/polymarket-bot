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
 * Computes on-the-fly intra-candle indicators strictly for the indicator types declared
 * by the active strategy, and tracks intra-candle OHLCV to construct completed
 * {@link MarketCandle} instances at candle close without extra Parquet scans.
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

    // Active candle intra-candle state
    private double twapOpenPrice = 0.0;
    private double rollingTwapSum = 0.0;
    private long rollingTwapCount = 0L;

    private double rollingVwapUsd = 0.0;
    private double rollingVwapBtc = 0.0;
    private double intraCandleCvdBtc = 0.0;

    private double currentSpotPrice = 0.0;
    private double currentFuturesPrice = 0.0;

    // Active candle OHLCV accumulators
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
        Arrays.fill(currentValues, Double.NaN);
        resetActiveCandle(0.0);
    }

    /**
     * Resets intra-candle rolling accumulators for a new active candle interval.
     *
     * @param openPrice candle opening reference price (or {@code 0.0} / {@link Double#NaN} if not yet known)
     */
    public void resetActiveCandle(double openPrice) {
        double cleanOpen = (!Double.isNaN(openPrice) && openPrice > 0.0) ? openPrice : 0.0;
        this.twapOpenPrice = cleanOpen;
        this.rollingTwapSum = cleanOpen > 0.0 ? cleanOpen : 0.0;
        this.rollingTwapCount = cleanOpen > 0.0 ? 1L : 0L;
        this.rollingVwapUsd = 0.0;
        this.rollingVwapBtc = 0.0;
        this.intraCandleCvdBtc = 0.0;

        this.sOpen = cleanOpen;
        this.sHigh = cleanOpen;
        this.sLow = cleanOpen;
        this.sClose = cleanOpen;
        this.spotVolBtc = 0.0;
        this.spotVolUsd = 0.0;
        this.spotDeltaBtc = 0.0;

        this.fOpen = cleanOpen;
        this.fHigh = cleanOpen;
        this.fLow = cleanOpen;
        this.fClose = cleanOpen;
        this.futVolBtc = 0.0;
        this.futVolUsd = 0.0;
        this.futDeltaBtc = 0.0;

        if (hasAnyIndicators) {
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
            if (hasAnyIndicators) {
                recalculateAll();
            }
        }
    }

    /**
     * Builds a completed {@link MarketCandle} for the current 15-minute interval from accumulated stream data.
     *
     * @param tStart candle start epoch seconds
     * @param tEnd candle end epoch seconds
     * @return constructed MarketCandle
     */
    public MarketCandle buildCompletedCandle(long tStart, long tEnd) {
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
     * Updates spot price and candle OHLC from the {@code polymarket/price_crypto} stream.
     *
     * @param timestampMs local EC2 receipt timestamp in milliseconds UTC
     * @param price crypto spot price in USD
     */
    public void onPolymarketCryptoPrice(long timestampMs, double price) {
        if (Double.isNaN(price) || price <= 0.0) {
            return;
        }
        this.currentSpotPrice = price;
        updateSpotOhlc(price);

        if (needTwap) {
            this.rollingTwapSum += price;
            this.rollingTwapCount++;
            currentValues[IndicatorType.TWAP.ordinal()] = rollingTwapSum / rollingTwapCount;
        }
        if (needVwapZScore) {
            VwapCalculator.VwapResult res = vwapCalculator.calculate(candleHistory, price);
            currentValues[IndicatorType.VWAP_ZSCORE.ordinal()] = res.zScore();
        }
    }

    /**
     * Updates the 60-second TWAP reference from the {@code polymarket/price_crypto_twap_60s} stream.
     *
     * @param timestampMs local EC2 receipt timestamp in milliseconds UTC
     * @param twapPrice 60-second TWAP price in USD
     */
    public void onPolymarketCryptoTwap60s(long timestampMs, double twapPrice) {
        if (Double.isNaN(twapPrice) || twapPrice <= 0.0) {
            return;
        }
        if (this.twapOpenPrice <= 0.0) {
            this.twapOpenPrice = twapPrice;
        }
        if (this.currentSpotPrice <= 0.0) {
            this.currentSpotPrice = twapPrice;
        }
        updateSpotOhlc(twapPrice);
        if (needTwap) {
            currentValues[IndicatorType.TWAP.ordinal()] = twapPrice;
        }
    }

    /**
     * Updates rolling price, TWAP, VWAP, CVD, and Basis indicators upon receiving a spot trade.
     *
     * @param timestampMs event receipt timestamp in milliseconds UTC
     * @param price spot execution price
     * @param quantity trade volume in base asset
     * @param isBuyerMaker true if maker was buyer (aggressive sell), false if aggressive buy
     */
    public void onSpotTrade(long timestampMs, double price, double quantity, boolean isBuyerMaker) {
        this.currentSpotPrice = price;
        updateSpotOhlc(price);
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
            VwapCalculator.VwapResult res = vwapCalculator.calculate(candleHistory, price);
            currentValues[IndicatorType.VWAP_ZSCORE.ordinal()] = res.zScore();
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
     * Updates futures reference price and basis spread upon receiving a futures trade.
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
     * Updates order book imbalance (OBI) and depth-weighted micro price from book depth.
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

    private void updateSpotOhlc(double price) {
        if (sOpen <= 0.0) {
            sOpen = price;
            sHigh = price;
            sLow = price;
        } else {
            if (price > sHigh) sHigh = price;
            if (price < sLow) sLow = price;
        }
        sClose = price;
    }

    private void recalculateAll() {
        if (needVol4h) {
            double vol4h = volCalculator.calculate4hRealizedVolatility(candleHistory);
            currentValues[IndicatorType.VOLATILITY_4H.ordinal()] = vol4h;
        }

        if (needVwapZScore) {
            double spot = currentSpotPrice > 0.0 ? currentSpotPrice : twapOpenPrice;
            VwapCalculator.VwapResult res = vwapCalculator.calculate(candleHistory, spot);
            currentValues[IndicatorType.VWAP_ZSCORE.ordinal()] = res.zScore();
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

    /**
     * Returns the opening TWAP reference price of the active candle.
     *
     * @return TWAP open price
     */
    public double getTwapOpenPrice() {
        return twapOpenPrice;
    }
}
