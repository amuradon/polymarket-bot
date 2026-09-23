package cz.polymarket.bot.calculator;

import cz.polymarket.bot.domain.TwapPoint;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TwapCalculatorTest {

    private final TwapCalculator calculator = new TwapCalculator();

    @Test
    void shouldCalculateRollingTwapForFull60SecondsWindow() {
        List<BigDecimal> window = new ArrayList<>();
        // 30 prices of 100.00 and 30 prices of 110.00
        for (int i = 0; i < 30; i++) {
            window.add(new BigDecimal("100.00"));
        }
        for (int i = 0; i < 30; i++) {
            window.add(new BigDecimal("110.00"));
        }

        BigDecimal rollingTwap = calculator.calculateRollingTwap(window);

        // (30 * 100 + 30 * 110) / 60 = 105.00
        assertThat(rollingTwap).isEqualByComparingTo("105.00");
    }

    @Test
    void shouldCalculateRollingTwapForPartialWindowWhenStartup() {
        List<BigDecimal> window = List.of(
                new BigDecimal("100.00"),
                new BigDecimal("110.00"),
                new BigDecimal("120.00")
        );

        BigDecimal rollingTwap = calculator.calculateRollingTwap(window);

        // (100 + 110 + 120) / 3 = 110.00
        assertThat(rollingTwap).isEqualByComparingTo("110.00");
    }

    @Test
    void shouldReturnSamePriceWhenAllPricesInWindowAreEqual() {
        List<BigDecimal> window = new ArrayList<>();
        for (int i = 0; i < 60; i++) {
            window.add(new BigDecimal("78823.52"));
        }

        BigDecimal rollingTwap = calculator.calculateRollingTwap(window);

        assertThat(rollingTwap).isEqualByComparingTo("78823.52");
    }

    @Test
    void shouldCreateTwapPoint() {
        long time = 1788444000L;
        BigDecimal rollingTwap = new BigDecimal("78823.52");
        BigDecimal median = new BigDecimal("78820.00");

        TwapPoint point = calculator.createPoint(time, rollingTwap, median);

        assertThat(point.time()).isEqualTo(time);
        assertThat(point.twap()).isEqualByComparingTo("78823.52");
        assertThat(point.medianPrice()).isEqualByComparingTo("78820.00");
    }

    @Test
    void shouldThrowWhenNullOrEmptyPrices() {
        assertThatThrownBy(() -> calculator.calculateRollingTwap(null))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> calculator.calculateRollingTwap(Collections.emptyList()))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> calculator.createPoint(1000L, null, new BigDecimal("100.00")))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
