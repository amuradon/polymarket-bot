package cz.polymarket.bot.backtest.stream;

import cz.polymarket.bot.strategy.IndicatorType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class DynamicIndicatorEngineTest {

    @Test
    @DisplayName("Should compute rolling TWAP, VWAP, CVD, and Basis dynamically")
    void shouldComputeDynamicIndicators() {
        DynamicIndicatorEngine engine = new DynamicIndicatorEngine(Set.of(
                IndicatorType.TWAP,
                IndicatorType.VWAP,
                IndicatorType.CVD,
                IndicatorType.BASIS,
                IndicatorType.ORDER_BOOK_IMBALANCE,
                IndicatorType.MICRO_PRICE
        ));

        engine.resetActiveCandle(1000000L, 60000.0);

        // Trade 1: Spot trade @ 60100 qty 1.0 (aggressive buy)
        engine.onSpotTrade(1000100L, 60100.0, 1.0, false);
        // Trade 2: Spot trade @ 60200 qty 2.0 (aggressive sell)
        engine.onSpotTrade(1000200L, 60200.0, 2.0, true);

        // TWAP: (60000 + 60100 + 60200) / 3 = 60100
        assertThat(engine.getIndicatorValue(IndicatorType.TWAP)).isCloseTo(60100.0, within(0.01));

        // VWAP: (60100*1 + 60200*2) / 3 = 180500 / 3 = 60166.67
        assertThat(engine.getIndicatorValue(IndicatorType.VWAP)).isCloseTo(60166.67, within(0.1));

        // CVD: +1.0 - 2.0 = -1.0
        assertThat(engine.getIndicatorValue(IndicatorType.CVD)).isCloseTo(-1.0, within(0.01));

        // Futures Trade @ 60250 -> Basis vs Spot 60200: (60250 - 60200)/60200 * 10000 ~= 8.3 bps
        engine.onFuturesTrade(1000250L, 60250.0, 1.0, false);
        assertThat(engine.getIndicatorValue(IndicatorType.BASIS)).isCloseTo(8.3, within(0.2));

        // Order Book: bestBid 60240 (qty 10), bestAsk 60260 (qty 5)
        // OBI: (10 - 5) / (10 + 5) = 5 / 15 = 0.333
        // MicroPrice: (5 * 60240 + 10 * 60260) / 15 = 60253.33
        engine.onFuturesOrderBook(1000300L, 60240.0, 60260.0, 10.0, 5.0, 0.333);
        assertThat(engine.getIndicatorValue(IndicatorType.ORDER_BOOK_IMBALANCE)).isCloseTo(0.333, within(0.001));
        assertThat(engine.getIndicatorValue(IndicatorType.MICRO_PRICE)).isCloseTo(60253.33, within(0.1));
    }
}
