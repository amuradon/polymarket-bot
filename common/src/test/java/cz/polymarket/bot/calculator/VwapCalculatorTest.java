package cz.polymarket.bot.calculator;

import cz.polymarket.bot.domain.MarketCandle;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

class VwapCalculatorTest {

    private final VwapCalculator calculator = new VwapCalculator();

    private MarketCandle createCandle(long start, double open, double close, double volBtc, double volUsd) {
        return new MarketCandle(
                start, start + 900,
                open, Math.max(open, close), Math.min(open, close), close,
                volBtc, volUsd, 0.0,
                open, Math.max(open, close), Math.min(open, close), close,
                volBtc, volUsd, 0.0,
                0.0
        );
    }

    @Test
    @DisplayName("Should return default state when candle history is empty")
    void shouldHandleEmptyHistory() {
        VwapCalculator.VwapResult result = calculator.calculate(Collections.emptyList(), 60000.0);
        assertThat(result.vwap()).isEqualTo(60000.0);
        assertThat(result.stdDev()).isEqualTo(1.0);
        assertThat(result.zScore()).isEqualTo(0.0);
    }

    @Test
    @DisplayName("Should accurately calculate VWAP, StdDev, and Z-score from multi-candle history")
    void shouldCalculateVwapAndZScoreAccurately() {
        List<MarketCandle> candles = new ArrayList<>();
        // Candle 1: price 60000, 10 BTC, 600000 USD
        candles.add(createCandle(1000, 60000.0, 60000.0, 10.0, 600000.0));
        // Candle 2: price 62000, 10 BTC, 620000 USD
        candles.add(createCandle(1900, 60000.0, 62000.0, 10.0, 620000.0));

        // Total USD = 1,220,000, Total BTC = 20.0 => VWAP = 61,000.0
        // Variance = [ 10 * (60000-61000)^2 + 10 * (62000-61000)^2 ] / 20 = [ 10 * 1,000,000 + 10 * 1,000,000 ] / 20 = 1,000,000
        // StdDev = sqrt(1,000,000) = 1,000.0
        // If current spot price = 63,000.0:
        // Z-score = (63000 - 61000) / 1000.0 = 2.0

        VwapCalculator.VwapResult result = calculator.calculate(candles, 63000.0);

        assertThat(result.vwap()).isCloseTo(61000.0, within(1e-6));
        assertThat(result.stdDev()).isCloseTo(1000.0, within(1e-6));
        assertThat(result.zScore()).isCloseTo(2.0, within(1e-6));
    }

    @Test
    @DisplayName("Should limit calculation window to max 96 candles (24 hours)")
    void shouldLimitWindowTo96Candles() {
        List<MarketCandle> candles = new ArrayList<>();
        // Add 100 candles at price 50000
        for (int i = 0; i < 100; i++) {
            candles.add(createCandle(i * 900L + 1, 50000.0, 50000.0, 1.0, 50000.0));
        }
        // Change the last candle to 60000
        candles.set(99, createCandle(99 * 900L + 1, 60000.0, 60000.0, 1.0, 60000.0));

        // The first 4 candles should be excluded, leaving 95 candles of 50000 and 1 candle of 60000
        VwapCalculator.VwapResult result = calculator.calculate(candles, 60000.0);
        double expectedVwap = (95 * 50000.0 + 1 * 60000.0) / 96.0;
        assertThat(result.vwap()).isCloseTo(expectedVwap, within(1e-4));
    }
}
