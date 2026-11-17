package cz.polymarket.bot.backtest.stream;

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
 * Computes on-the-fly intra-candle indicators strictly from Spot exchange streams
 * for the indicator types declared by the active strategy. Polymarket streams are never mixed
 * into Spot indicator calculations.
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

        if (hasAnyIndicators) {
            Arrays.fill(currentValues, Double.NaN);
            if (needCvd) {
                currentValues[IndicatorType.CVD.ordinal()] = intraCandleCvdBtc;
            }
        }
    }

    /**
     * Updates spot reference price and rolling CVD indicator in {@code O(1)}
     * upon receiving a spot exchange trade.
     *
     * @param timestampMs event receipt timestamp in milliseconds UTC
     * @param price spot execution price
     * @param quantity trade volume in base asset
     * @param isBuyerMaker true if maker was buyer (aggressive sell), false if aggressive buy
     */
    public void onSpotTrade(long timestampMs, double price, double quantity, boolean isBuyerMaker) {
        this.currentSpotPrice = price;

        if (!hasAnyIndicators) {
            return;
        }

        if (needCvd) {
            double delta = isBuyerMaker ? -quantity : quantity;
            this.intraCandleCvdBtc += delta;
            currentValues[IndicatorType.CVD.ordinal()] = this.intraCandleCvdBtc;
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
