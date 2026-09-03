package cz.polymarket.bot.calculator;

import cz.polymarket.bot.domain.TwapPoint;
import jakarta.enterprise.context.ApplicationScoped;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@ApplicationScoped
public class TwapCalculator {

    private static final int SCALE = 8;
    private static final RoundingMode ROUNDING_MODE = RoundingMode.HALF_UP;

    public TwapPoint createInitialPoint(long timestampSec, BigDecimal openPrice) {
        if (openPrice == null) {
            throw new IllegalArgumentException("Open price cannot be null");
        }
        return new TwapPoint(timestampSec, openPrice.setScale(SCALE, ROUNDING_MODE), openPrice.setScale(SCALE, ROUNDING_MODE));
    }

    public TwapPoint calculateNext(TwapPoint previousPoint, long timestampSec, BigDecimal medianPrice, int previousPointsCount) {
        if (previousPoint == null || medianPrice == null) {
            throw new IllegalArgumentException("Previous point and median price cannot be null");
        }
        if (previousPointsCount < 1) {
            throw new IllegalArgumentException("Previous points count must be at least 1");
        }

        // prevSum = previousTwap * previousPointsCount
        BigDecimal prevSum = previousPoint.twap().multiply(BigDecimal.valueOf(previousPointsCount));
        BigDecimal newSum = prevSum.add(medianPrice);
        BigDecimal newCount = BigDecimal.valueOf(previousPointsCount + 1L);

        BigDecimal newTwap = newSum.divide(newCount, SCALE, ROUNDING_MODE);
        return new TwapPoint(timestampSec, newTwap, medianPrice.setScale(SCALE, ROUNDING_MODE));
    }

    public List<TwapPoint> calculateSeries(long candleStartSec, BigDecimal openPrice, Map<Long, BigDecimal> historicalPrices) {
        if (openPrice == null) {
            throw new IllegalArgumentException("Open price cannot be null");
        }

        List<TwapPoint> result = new ArrayList<>();
        TwapPoint initial = createInitialPoint(candleStartSec, openPrice);
        result.add(initial);

        if (historicalPrices == null || historicalPrices.isEmpty()) {
            return result;
        }

        // Ensure chronological order
        Map<Long, BigDecimal> sorted = new TreeMap<>(historicalPrices);
        TwapPoint current = initial;
        int count = 1;

        for (Map.Entry<Long, BigDecimal> entry : sorted.entrySet()) {
            if (entry.getKey() <= candleStartSec) {
                continue; // Skip points before or at t0
            }
            current = calculateNext(current, entry.getKey(), entry.getValue(), count);
            result.add(current);
            count++;
        }

        return result;
    }
}
