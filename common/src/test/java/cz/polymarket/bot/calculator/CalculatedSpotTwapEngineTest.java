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
        engine.onBinanceUsdtTrade(1_000L, 90_000.0, 1.0);
        assertThat(engine.getLastSpotPriceUsd()).isCloseTo(90_000.0, within(1e-6));

        // First Kraken trade initializes EMA to exact trade price
        double ema0 = engine.onUsdtUsdTrade(1_000L, 0.9990);
        assertThat(ema0).isCloseTo(0.9990, within(1e-8));

        // Second Kraken trade 5,000 ms later (dt = tau = 5000 ms -> alpha = 1 - 1/e = 0.63212056)
        double ema1 = engine.onUsdtUsdTrade(6_000L, 1.0000);
        double expectedAlpha = 1.0 - Math.exp(-1.0);
        double expectedEma = 0.9990 + expectedAlpha * (1.0000 - 0.9990);
        assertThat(ema1).isCloseTo(expectedEma, within(1e-8));

        // Binance trade at 6,000 ms (5s after first Binance trade so micro-VWAP is dominated by new trade)
        engine.onBinanceUsdtTrade(6_000L, 90_000.0, 10.0);
        assertThat(engine.getLastSpotPriceUsd()).isCloseTo(90_000.0 * expectedEma, within(1e-2));
    }

    @Test
    @DisplayName("Should aggregate Binance, Coinbase, and Kraken spot trades using liquidity and volume weights")
    void shouldAggregateMultiExchangeSpotTradesWithLiquidityAndVolumeWeights() {
        CalculatedSpotTwapEngine engine = new CalculatedSpotTwapEngine();
        engine.onUsdtUsdTrade(1_000L, 1.0);

        // Equal volume (1.0 BTC each) at the exact same timestamp (t = 1,000 ms)
        // Baseline weights: Binance = 0.45, Coinbase = 0.35, Kraken = 0.20
        // Equal volume shares: 1/3 each
        // Blended weights: w_i = 0.70 * W_i + 0.30 * (1/3)
        //   w_binance  = 0.70 * 0.45 + 0.10 = 0.415
        //   w_coinbase = 0.70 * 0.35 + 0.10 = 0.345
        //   w_kraken   = 0.70 * 0.20 + 0.10 = 0.240
        // Prices within 10 bps ($70 tolerance around $70,000) -> no outlier penalty
        engine.onBinanceUsdtTrade(1_000L, 70_000.0, 1.0);
        engine.onCoinbaseUsdTrade(1_000L, 70_020.0, 1.0);
        engine.onKrakenUsdTrade(1_000L, 70_040.0, 1.0);

        double expectedSpot = 0.415 * 70_000.0 + 0.345 * 70_020.0 + 0.240 * 70_040.0;
        assertThat(engine.getLastSpotPriceUsd()).isCloseTo(expectedSpot, within(1e-4));
    }

    @Test
    @DisplayName("Should down-weight single-exchange anomalous flash wick using 3-venue median outlier penalty")
    void shouldRejectSingleExchangeOutlierFlashWick() {
        CalculatedSpotTwapEngine engine = new CalculatedSpotTwapEngine();
        engine.onUsdtUsdTrade(1_000L, 1.0);

        // Binance and Coinbase trade at $70,000, while Kraken prints an anomalous $71,400 wick (+200 bps = 20x tolerance)
        engine.onBinanceUsdtTrade(1_000L, 70_000.0, 1.0);
        engine.onCoinbaseUsdTrade(1_000L, 70_000.0, 1.0);
        engine.onKrakenUsdTrade(1_000L, 71_400.0, 1.0);

        // Without outlier protection, a +$1,400 spike with 0.24 weight would pull the price up by +$336.
        // With 3-venue median Cauchy down-weighting, the impact is suppressed to < $2.00.
        assertThat(engine.getLastSpotPriceUsd()).isBetween(70_000.0, 70_002.0);
    }

    @Test
    @DisplayName("Should decay stale exchange weight exponentially when one exchange stops trading")
    void shouldDecayStaleExchangeWeightOverTime() {
        CalculatedSpotTwapEngine engine = new CalculatedSpotTwapEngine();
        engine.onUsdtUsdTrade(0L, 1.0);

        // All 3 exchanges start at $70,000 at t = 0
        engine.onBinanceUsdtTrade(0L, 70_000.0, 1.0);
        engine.onCoinbaseUsdTrade(0L, 70_000.0, 1.0);
        engine.onKrakenUsdTrade(0L, 70_000.0, 1.0);

        // 25 seconds later (5 * tau_stale), Binance and Coinbase trade at $70,050 while Kraken has not traded since t = 0
        engine.onBinanceUsdtTrade(25_000L, 70_050.0, 2.0);
        engine.onCoinbaseUsdTrade(25_000L, 70_050.0, 2.0);

        // Kraken's staleness factor is exp(-25000 / 5000) = exp(-5) = 0.0067, so spot price is pulled closely to $70,050
        assertThat(engine.getLastSpotPriceUsd()).isBetween(70_049.0, 70_050.0);
    }

    @Test
    @DisplayName("Should compute continuous 60-second time-weighted average price across step intervals and evict older than 60s")
    void shouldComputeContinuous60sTwapAndEvictOldIntervals() {
        CalculatedSpotTwapEngine engine = new CalculatedSpotTwapEngine();
        engine.onUsdtUsdTrade(0L, 1.0);

        // [0s, 30s): price is $80,000
        engine.onBinanceUsdtTrade(0L, 80_000.0, 1.0);
        assertThat(engine.getLastTwap60sPriceUsd()).isCloseTo(80_000.0, within(1e-6));

        // At t = 30s (120 * tau_micro later, so micro-VWAP decay is 0), price jumps to $80,600.
        // Over [0s, 30s], price was $80,000 -> TWAP at 30s is still $80,000
        engine.onBinanceUsdtTrade(30_000L, 80_600.0, 1.0);
        assertThat(engine.getLastSpotPriceUsd()).isCloseTo(80_600.0, within(1e-6));
        assertThat(engine.getLastTwap60sPriceUsd()).isCloseTo(80_000.0, within(1e-6));

        // At t = 60s, price was $80,000 for 30s and $80,600 for 30s -> 60s TWAP = $80,300
        engine.onBinanceUsdtTrade(60_000L, 80_600.0, 1.0);
        assertThat(engine.getLastTwap60sPriceUsd()).isCloseTo(80_300.0, within(1e-6));

        // At t = 75s, the 60s window is [15s, 75s]: 15s at $80,000 and 45s at $80,600 -> TWAP = $80,450
        engine.onBinanceUsdtTrade(75_000L, 80_600.0, 1.0);
        assertThat(engine.getLastTwap60sPriceUsd()).isCloseTo(80_450.0, within(1e-6));

        // At t = 90s, the entire 60s window [30s, 90s] was at $80,600 -> TWAP = $80,600
        engine.onBinanceUsdtTrade(90_000L, 80_600.0, 1.0);
        assertThat(engine.getLastTwap60sPriceUsd()).isCloseTo(80_600.0, within(1e-6));
    }
}
