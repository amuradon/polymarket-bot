package cz.polymarket.bot.calculator;

import cz.polymarket.bot.domain.TwapPoint;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TwapCalculatorTest {

    private final TwapCalculator calculator = new TwapCalculator();

    @Test
    void shouldCalculateInitialTwapAtTimeZero() {
        BigDecimal openPrice = new BigDecimal("78000.00");
        long t0 = 1788437700L;

        TwapPoint initial = calculator.createInitialPoint(t0, openPrice);

        assertThat(initial.time()).isEqualTo(t0);
        assertThat(initial.twap()).isEqualByComparingTo("78000.00");
        assertThat(initial.medianPrice()).isEqualByComparingTo("78000.00");
    }

    @Test
    void shouldCalculateNextTwapIncrementally() {
        BigDecimal openPrice = new BigDecimal("100.00");
        long t0 = 1000L;

        TwapPoint p0 = calculator.createInitialPoint(t0, openPrice);

        // Step 1: price is 110. (100 + 110) / 2 = 105
        TwapPoint p1 = calculator.calculateNext(p0, 1001L, new BigDecimal("110.00"), 1);
        assertThat(p1.twap()).isEqualByComparingTo("105.00");
        assertThat(p1.medianPrice()).isEqualByComparingTo("110.00");
        assertThat(p1.time()).isEqualTo(1001L);

        // Step 2: price is 120. (100 + 110 + 120) / 3 = 110
        TwapPoint p2 = calculator.calculateNext(p1, 1002L, new BigDecimal("120.00"), 2);
        assertThat(p2.twap()).isEqualByComparingTo("110.00");
        assertThat(p2.medianPrice()).isEqualByComparingTo("120.00");

        // Step 3: price is 90. (100 + 110 + 120 + 90) / 4 = 105
        TwapPoint p3 = calculator.calculateNext(p2, 1003L, new BigDecimal("90.00"), 3);
        assertThat(p3.twap()).isEqualByComparingTo("105.00");
    }

    @Test
    void shouldCalculateFullSeriesFromHistory() {
        long t0 = 1000L;
        BigDecimal openPrice = new BigDecimal("100.00");

        Map<Long, BigDecimal> historicalPrices = new TreeMap<>();
        historicalPrices.put(1001L, new BigDecimal("110.00"));
        historicalPrices.put(1002L, new BigDecimal("120.00"));
        historicalPrices.put(1003L, new BigDecimal("90.00"));

        List<TwapPoint> points = calculator.calculateSeries(t0, openPrice, historicalPrices);

        assertThat(points).hasSize(4);
        assertThat(points.get(0).twap()).isEqualByComparingTo("100.00");
        assertThat(points.get(1).twap()).isEqualByComparingTo("105.00");
        assertThat(points.get(2).twap()).isEqualByComparingTo("110.00");
        assertThat(points.get(3).twap()).isEqualByComparingTo("105.00");
    }

    @Test
    void shouldThrowWhenNullArguments() {
        assertThatThrownBy(() -> calculator.createInitialPoint(1000L, null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
