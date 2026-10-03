package cz.polymarket.bot.calculator;

import cz.polymarket.bot.domain.MarketCandle;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.List;

/**
 * Calculates 4-hour realized volatility across 16 15-minute intervals.
 * Provides regime filtering to avoid trading during compressed, low-volatility regimes.
 */
@ApplicationScoped
public class RealizedVolatilityCalculator {

    public static final int VOLATILITY_WINDOW_CANDLES = 16;
    public static final double DEFAULT_VOLATILITY = 0.001;
    public static final double MIN_VOLATILITY_THRESHOLD = 0.0006;

    public double calculate4hRealizedVolatility(List<MarketCandle> history) {
        if (history == null || history.size() < VOLATILITY_WINDOW_CANDLES) {
            return DEFAULT_VOLATILITY;
        }

        int size = history.size();
        int startIndex = size - VOLATILITY_WINDOW_CANDLES;

        double sumReturns = 0.0;
        double[] returns = new double[VOLATILITY_WINDOW_CANDLES];

        for (int i = 0; i < VOLATILITY_WINDOW_CANDLES; i++) {
            MarketCandle c = history.get(startIndex + i);
            double open = c.spotOpen();
            double ret = (open > 0.0) ? (c.spotClose() - open) / open : 0.0;
            returns[i] = ret;
            sumReturns += ret;
        }

        double mean = sumReturns / VOLATILITY_WINDOW_CANDLES;

        double sumSqDiff = 0.0;
        for (int i = 0; i < VOLATILITY_WINDOW_CANDLES; i++) {
            double diff = returns[i] - mean;
            sumSqDiff += diff * diff;
        }

        return Math.sqrt(sumSqDiff / VOLATILITY_WINDOW_CANDLES);
    }

    public boolean isVolatilityTooLow(double volatility) {
        return volatility < MIN_VOLATILITY_THRESHOLD;
    }
}
