package cz.polymarket.bot.calculator;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MedianCalculatorTest {

    private final MedianCalculator calculator = new MedianCalculator();

    @Test
    void shouldCalculateMedianForDistinctValuesInAnyOrder() {
        BigDecimal a = new BigDecimal("78000.50");
        BigDecimal b = new BigDecimal("78010.00");
        BigDecimal c = new BigDecimal("77995.25");

        assertThat(calculator.calculate(a, b, c)).isEqualByComparingTo("78000.50");
        assertThat(calculator.calculate(b, a, c)).isEqualByComparingTo("78000.50");
        assertThat(calculator.calculate(c, b, a)).isEqualByComparingTo("78000.50");
        assertThat(calculator.calculate(c, a, b)).isEqualByComparingTo("78000.50");
    }

    @Test
    void shouldCalculateMedianWhenValuesAreEqual() {
        BigDecimal a = new BigDecimal("78000.00");
        BigDecimal b = new BigDecimal("78000.00");
        BigDecimal c = new BigDecimal("78050.00");

        assertThat(calculator.calculate(a, b, c)).isEqualByComparingTo("78000.00");
        assertThat(calculator.calculate(a, b, a)).isEqualByComparingTo("78000.00");
    }

    @Test
    void shouldThrowWhenAnyValueIsNull() {
        BigDecimal val = new BigDecimal("78000.00");
        assertThatThrownBy(() -> calculator.calculate(null, val, val))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> calculator.calculate(val, null, val))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> calculator.calculate(val, val, null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
