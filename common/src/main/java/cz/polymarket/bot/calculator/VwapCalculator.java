package cz.polymarket.bot.calculator;

import cz.polymarket.bot.domain.MarketCandle;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.List;

/**
 * Calculates rolling Volume-Weighted Average Price (VWAP), volume-weighted variance,
 * standard deviation, and Z-score over up to 96 15-minute intervals (24 hours).
 * Optimized for low latency and minimal allocations.
 */
@ApplicationScoped
public class VwapCalculator {

    public static final int MAX_WINDOW_CANDLES = 96;

    public record VwapResult(double vwap, double stdDev, double zScore) {}

    public VwapResult calculate(List<MarketCandle> history, double currentSpotPrice) {
        if (history == null || history.isEmpty()) {
            return new VwapResult(currentSpotPrice, 1.0, 0.0);
        }

        int size = history.size();
        int startIndex = Math.max(0, size - MAX_WINDOW_CANDLES);

        double totalUsd = 0.0;
        double totalBtc = 0.0;

        for (int i = startIndex; i < size; i++) {
            MarketCandle c = history.get(i);
            totalUsd += c.spotVolumeUsd();
            totalBtc += c.spotVolumeBtc();
        }

        if (totalBtc <= 1e-6) {
            return new VwapResult(currentSpotPrice, 1.0, 0.0);
        }

        double vwap = totalUsd / totalBtc;

        double weightedSumSqDiff = 0.0;
        for (int i = startIndex; i < size; i++) {
            MarketCandle c = history.get(i);
            double diff = c.spotClose() - vwap;
            weightedSumSqDiff += c.spotVolumeBtc() * (diff * diff);
        }

        double variance = weightedSumSqDiff / totalBtc;
        double stdDev = variance > 0.0 ? Math.sqrt(variance) : 1.0;
        if (stdDev < 1.0) {
            stdDev = 1.0;
        }

        double zScore = (currentSpotPrice - vwap) / stdDev;

        return new VwapResult(vwap, stdDev, zScore);
    }
}
