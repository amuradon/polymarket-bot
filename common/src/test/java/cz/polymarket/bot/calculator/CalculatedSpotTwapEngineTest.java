package cz.polymarket.bot.calculator;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Unit tests for {@link CalculatedSpotTwapEngine}.
 */
class CalculatedSpotTwapEngineTest {

    @Test
    @DisplayName("Should smooth Kraken USDT/USD peg with 5-second time-weighted EMA and convert Binance USDT spot to USD")
    void shouldSmoothUsdtUsdWith5sEmaAndConvertSpotPrice() {
        CalculatedSpotTwapEngine engine = new CalculatedSpotTwapEngine();

        // Default peg before any Kraken trade is 1.0
        assertThat(engine.getUsdtUsdEma()).isEqualTo(1.0);
        engine.onAssetUsdtTrade(1_000L, 90_000.0);
        assertThat(engine.getLastSpotPriceUsd()).isCloseTo(90_000.0, within(1e-6));

        // First Kraken trade initializes EMA to exact trade price
        double ema0 = engine.onUsdtUsdTrade(1_000L, 0.9990);
        assertThat(ema0).isCloseTo(0.9990, within(1e-8));

        // Second Kraken trade 5,000 ms later (dt = tau = 5000 ms -> alpha = 1 - 1/e = 0.63212056)
        double ema1 = engine.onUsdtUsdTrade(6_000L, 1.0000);
        double expectedAlpha = 1.0 - Math.exp(-1.0);
        double expectedEma = 0.9990 + expectedAlpha * (1.0000 - 0.9990);
        assertThat(ema1).isCloseTo(expectedEma, within(1e-8));

        // Binance trade at 6,000 ms uses the smoothed EMA
        engine.onAssetUsdtTrade(6_000L, 90_000.0);
        assertThat(engine.getLastSpotPriceUsd()).isCloseTo(90_000.0 * expectedEma, within(1e-4));
    }

    @Test
    @DisplayName("Should compute continuous 60-second time-weighted average price across step intervals and evict older than 60s")
    void shouldComputeContinuous60sTwapAndEvictOldIntervals() {
        CalculatedSpotTwapEngine engine = new CalculatedSpotTwapEngine();
        engine.onUsdtUsdTrade(0L, 1.0);

        // [0s, 30s): price is $80,000
        engine.onAssetUsdtTrade(0L, 80_000.0);
        assertThat(engine.getLastTwap60sPriceUsd()).isCloseTo(80_000.0, within(1e-6));

        // At t = 30s, price jumps to $80,600. Over [0s, 30s], price was $80,000 -> TWAP at 30s is still $80,000
        engine.onAssetUsdtTrade(30_000L, 80_600.0);
        assertThat(engine.getLastTwap60sPriceUsd()).isCloseTo(80_000.0, within(1e-6));

        // At t = 60s, price was $80,000 for 30s and $80,600 for 30s -> 60s TWAP = $80,300
        engine.onAssetUsdtTrade(60_000L, 80_600.0);
        assertThat(engine.getLastTwap60sPriceUsd()).isCloseTo(80_300.0, within(1e-6));

        // At t = 75s, the 60s window is [15s, 75s]: 15s at $80,000 and 45s at $80,600 -> TWAP = $80,450
        engine.onAssetUsdtTrade(75_000L, 80_600.0);
        assertThat(engine.getLastTwap60sPriceUsd()).isCloseTo(80_450.0, within(1e-6));

        // At t = 90s, the entire 60s window [30s, 90s] was at $80,600 -> TWAP = $80,600
        engine.onAssetUsdtTrade(90_000L, 80_600.0);
        assertThat(engine.getLastTwap60sPriceUsd()).isCloseTo(80_600.0, within(1e-6));
    }
}
