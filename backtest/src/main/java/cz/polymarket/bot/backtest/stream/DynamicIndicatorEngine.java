package cz.polymarket.bot.backtest.stream;

import cz.polymarket.bot.domain.MarketCandle;
import cz.polymarket.bot.strategy.IndicatorType;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
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
    private final boolean needCvd;

    private final double[] currentValues = new double[INDICATOR_COUNT];

    // Active candle intra-candle state
    private double intraCandleCvdBtc = 0.0;
    private double currentSpotPrice = 0.0;

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
        this.needCvd = this.requiredIndicators.contains(IndicatorType.CVD);
        reset();
    }

    /**
     * Resets the entire engine state including computed indicator caches and active candle accumulators.
     */
    public void reset() {
        Arrays.fill(currentValues, Double.NaN);
        resetActiveCandle();
    }

    /**
     * Resets intra-candle rolling accumulators for a new active candle interval.
     */
    public void resetActiveCandle() {
        this.intraCandleCvdBtc = 0.0;
        this.currentSpotPrice = 0.0;

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
            if (needCvd) {
                currentValues[IndicatorType.CVD.ordinal()] = intraCandleCvdBtc;
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
        return new MarketCandle(
                tStart,
                tEnd,
                sOpen, sHigh, sLow, sClose,
                spotVolBtc, spotVolUsd, spotDeltaBtc,
                fOpen > 0.0 ? fOpen : sOpen,
                fHigh > 0.0 ? fHigh : sHigh,
                fLow > 0.0 ? fLow : sLow,
                fClose > 0.0 ? fClose : sClose,
                futVolBtc, futVolUsd, futDeltaBtc
        );
    }

    /**
     * Updates spot OHLCV metrics and rolling CVD indicator in {@code O(1)}
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

        if (needCvd) {
            this.intraCandleCvdBtc += delta;
            currentValues[IndicatorType.CVD.ordinal()] = this.intraCandleCvdBtc;
        }
    }

    /**
     * Updates futures OHLCV metrics upon receiving a futures exchange trade.
     *
     * @param timestampMs event receipt timestamp in milliseconds UTC
     * @param price futures execution price
     * @param quantity trade volume
     * @param isBuyerMaker true if maker was buyer, false if aggressive buy
     */
    public void onFuturesTrade(long timestampMs, double price, double quantity, boolean isBuyerMaker) {
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
