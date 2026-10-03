package cz.polymarket.bot.calculator;

import cz.polymarket.bot.domain.MarketCandle;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class RealizedVolatilityCalculatorTest {

    private final RealizedVolatilityCalculator calculator = new RealizedVolatilityCalculator();

    private MarketCandle createCandle(long start, double open, double close) {
        return new MarketCandle(
                start, start + 900,
                open, Math.max(open, close), Math.min(open, close), close,
                10.0, 600000.0, 0.0,
                open, Math.max(open, close), Math.min(open, close), close,
                10.0, 600000.0, 0.0,
                0.0
        );
    }

    @Test
    @DisplayName("Should return default volatility when history has fewer than 16 candles")
    void shouldReturnDefaultVolatilityWhenUnder16Candles() {
        List<MarketCandle> candles = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            candles.add(createCandle(i * 900L + 1, 50000.0, 50100.0));
        }

        double vol = calculator.calculate4hRealizedVolatility(candles);
        assertThat(vol).isEqualTo(0.001);
        assertThat(calculator.isVolatilityTooLow(vol)).isFalse();
    }

    @Test
    @DisplayName("Should calculate exact population standard deviation over last 16 candles")
    void shouldCalculateAccurate4hVolatility() {
        List<MarketCandle> candles = new ArrayList<>();
        // 8 candles with +1% return: open 100, close 101 => return = +0.01
        // 8 candles with -1% return: open 100, close 99  => return = -0.01
        for (int i = 0; i < 8; i++) {
            candles.add(createCandle(i * 900L + 1, 100.0, 101.0));
        }
        for (int i = 8; i < 16; i++) {
            candles.add(createCandle(i * 900L + 1, 100.0, 99.0));
        }

        // Mean return = 0.0
        // Variance = (8 * (0.01)^2 + 8 * (-0.01)^2) / 16 = 0.0001
        // StdDev = sqrt(0.0001) = 0.01

        double vol = calculator.calculate4hRealizedVolatility(candles);
        assertThat(vol).isCloseTo(0.01, within(1e-6));
        assertThat(calculator.isVolatilityTooLow(vol)).isFalse();
    }

    @Test
    @DisplayName("Should detect regime with volatility below 0.0006 threshold")
    void shouldDetectLowVolatilityRegime() {
        List<MarketCandle> candles = new ArrayList<>();
        // Flat market: very tiny changes (0.01%) => std dev ~ 0.0001 < 0.0006
        for (int i = 0; i < 16; i++) {
            double close = (i % 2 == 0) ? 10000.0 : 10001.0;
            candles.add(createCandle(i * 900L + 1, 10000.0, close));
        }

        double vol = calculator.calculate4hRealizedVolatility(candles);
        assertThat(vol).isLessThan(0.0006);
        assertThat(calculator.isVolatilityTooLow(vol)).isTrue();
    }
}
