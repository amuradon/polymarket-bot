package cz.polymarket.bot.calculator;

import cz.polymarket.bot.domain.TradeRecord;
import cz.polymarket.bot.strategy.TradeDirection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class PerformanceMetricsCalculatorTest {

    private PerformanceMetricsCalculator calculator;

    @BeforeEach
    void setUp() {
        calculator = new PerformanceMetricsCalculator();
    }

    @Test
    @DisplayName("Should return empty metrics when trades list is empty")
    void shouldReturnEmptyMetricsWhenTradesEmpty() {
        PerformanceMetrics metrics = calculator.calculate(Collections.emptyList(), 10000.0);

        assertThat(metrics.totalTrades()).isZero();
        assertThat(metrics.winningTrades()).isZero();
        assertThat(metrics.losingTrades()).isZero();
        assertThat(metrics.winRatePct()).isZero();
        assertThat(metrics.totalNetPnl()).isZero();
        assertThat(metrics.initialCapital()).isEqualTo(10000.0);
        assertThat(metrics.finalBalance()).isEqualTo(10000.0);
        assertThat(metrics.maxDrawdownUsd()).isZero();
        assertThat(metrics.maxDrawdownPct()).isZero();
        assertThat(metrics.sharpeRatio()).isZero();
    }

    @Test
    @DisplayName("Should correctly calculate win rate, PnL, fees, and profit factor")
    void shouldCalculateBasicPerformanceMetrics() {
        TradeRecord trade1 = new TradeRecord(
                "2026-08-07 00:00:00+00:00", 1723000000L, TradeDirection.UP,
                0.50, 1.00, 100.0, 200.0, 3.5, 96.5,
                "Resolution (TWAP 60s)", 0.65, 0.50, 0.15, true, 10096.5
        );
        TradeRecord trade2 = new TradeRecord(
                "2026-08-07 00:15:00+00:00", 1723000900L, TradeDirection.DOWN,
                0.50, 0.00, 100.0, 200.0, 3.5, -103.5,
                "Resolution (TWAP 60s)", 0.65, 0.50, 0.15, false, 9993.0
        );
        TradeRecord trade3 = new TradeRecord(
                "2026-08-07 00:30:00+00:00", 1723001800L, TradeDirection.UP,
                0.60, 0.70, 100.0, 166.67, 4.0, 12.67,
                "Take Profit (0.70)", 0.70, 0.60, 0.10, true, 10005.67
        );

        List<TradeRecord> trades = List.of(trade1, trade2, trade3);
        PerformanceMetrics metrics = calculator.calculate(trades, 10000.0, 0.15);

        assertThat(metrics.totalTrades()).isEqualTo(3);
        assertThat(metrics.winningTrades()).isEqualTo(2);
        assertThat(metrics.losingTrades()).isEqualTo(1);
        assertThat(metrics.winRatePct()).isCloseTo(66.6667, within(0.001));

        double expectedNetPnl = 96.5 - 103.5 + 12.67;
        assertThat(metrics.totalNetPnl()).isCloseTo(expectedNetPnl, within(0.01));
        assertThat(metrics.totalFees()).isCloseTo(11.0, within(0.01));
        assertThat(metrics.grossPnl()).isCloseTo(expectedNetPnl + 11.0, within(0.01));

        double expectedGrossProfit = 96.5 + 12.67;
        double expectedGrossLoss = 103.5;
        assertThat(metrics.grossProfit()).isCloseTo(expectedGrossProfit, within(0.01));
        assertThat(metrics.grossLoss()).isCloseTo(expectedGrossLoss, within(0.01));
        assertThat(metrics.profitFactor()).isCloseTo(expectedGrossProfit / expectedGrossLoss, within(0.01));
        assertThat(metrics.brierScore()).isEqualTo(0.15);
    }

    @Test
    @DisplayName("Should track max drawdown in USD and percentage correctly")
    void shouldTrackMaxDrawdownCorrectly() {
        // Start 10000
        // Trade 1: +500 -> 10500 (peak 10500)
        // Trade 2: -1000 -> 9500 (peak 10500, dd = 1000, ddPct = 1000/10500 * 100 = 9.5238%)
        // Trade 3: +200 -> 9700
        // Trade 4: -300 -> 9400 (peak 10500, dd = 1100, ddPct = 1100/10500 * 100 = 10.476%)
        TradeRecord t1 = new TradeRecord("t1", 1L, TradeDirection.UP, 0.5, 1.0, 500.0, 1000.0, 0.0, 500.0, "win", 0.7, 0.5, 0.2, true, 10500.0);
        TradeRecord t2 = new TradeRecord("t2", 2L, TradeDirection.UP, 0.5, 0.0, 1000.0, 2000.0, 0.0, -1000.0, "loss", 0.7, 0.5, 0.2, false, 9500.0);
        TradeRecord t3 = new TradeRecord("t3", 3L, TradeDirection.UP, 0.5, 0.7, 500.0, 1000.0, 0.0, 200.0, "win", 0.7, 0.5, 0.2, true, 9700.0);
        TradeRecord t4 = new TradeRecord("t4", 4L, TradeDirection.UP, 0.5, 0.2, 500.0, 1000.0, 0.0, -300.0, "loss", 0.7, 0.5, 0.2, false, 9400.0);

        PerformanceMetrics metrics = calculator.calculate(List.of(t1, t2, t3, t4), 10000.0);

        assertThat(metrics.finalBalance()).isEqualTo(9400.0);
        assertThat(metrics.maxDrawdownUsd()).isCloseTo(1100.0, within(0.01));
        assertThat(metrics.maxDrawdownPct()).isCloseTo((1100.0 / 10500.0) * 100.0, within(0.01));
    }
}
