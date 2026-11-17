package cz.polymarket.bot.backtest.stream;

import cz.polymarket.bot.strategy.IndicatorType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class DynamicIndicatorEngineTest {

    @Test
    @DisplayName("Should compute rolling CVD dynamically and reset cleanly between candles")
    void shouldComputeCvdAndResetCleanly() {
        DynamicIndicatorEngine engine = new DynamicIndicatorEngine(Set.of(IndicatorType.CVD));

        engine.resetActiveCandle();
        assertThat(engine.getIndicatorValue(IndicatorType.CVD)).isZero();
        assertThat(engine.getIndicatorValue(null)).isNaN();

        // Trade 0: Opening spot trade @ 60000 qty 0.0
        engine.onSpotTrade(1000000L, 60000.0, 0.0, false);
        // Trade 1: Spot trade @ 60100 qty 1.0 (aggressive buy)
        engine.onSpotTrade(1000100L, 60100.0, 1.0, false);
        // Trade 2: Spot trade @ 60200 qty 2.0 (aggressive sell)
        engine.onSpotTrade(1000200L, 60200.0, 2.0, true);

        // CVD: +1.0 - 2.0 = -1.0
        assertThat(engine.getIndicatorValue(IndicatorType.CVD)).isCloseTo(-1.0, within(0.01));
        assertThat(engine.getCurrentSpotPrice()).isCloseTo(60200.0, within(0.01));

        // Resetting active candle clears intra-candle CVD and spot price
        engine.resetActiveCandle();
        assertThat(engine.getIndicatorValue(IndicatorType.CVD)).isZero();
        assertThat(engine.getCurrentSpotPrice()).isZero();
    }
}
