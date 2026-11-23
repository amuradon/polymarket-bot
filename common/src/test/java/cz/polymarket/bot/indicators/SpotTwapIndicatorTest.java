package cz.polymarket.bot.indicators;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Unit tests for {@link SpotTwapIndicator}.
 */
class SpotTwapIndicatorTest {

    @Test
    @DisplayName("Should smooth Kraken USDT/USD peg with 5-second time-weighted EMA and convert Binance USDT spot to USD")
    void shouldSmoothUsdtUsdWith5sEmaAndConvertSpotPrice() {
        SpotTwapIndicator indicator = new SpotTwapIndicator();

        // Default peg before any Kraken trade is 1.0
        assertThat(indicator.getUsdtUsdEma()).isEqualTo(1.0);
        indicator.onBinanceUsdtTrade(1_000L, 90_000.0, 1.0);
        assertThat(indicator.getLastSpotPriceUsd()).isCloseTo(90_000.0, within(1e-6));

        // First Kraken trade initializes EMA to exact trade price
        double ema0 = indicator.onUsdtUsdTrade(1_000L, 0.9990);
        assertThat(ema0).isCloseTo(0.9990, within(1e-8));

        // Second Kraken trade 5,000 ms later (dt = tau = 5000 ms -> alpha = 1 - 1/e = 0.63212056)
        double ema1 = indicator.onUsdtUsdTrade(6_000L, 1.0000);
        double expectedAlpha = 1.0 - Math.exp(-1.0);
        double expectedEma = 0.9990 + expectedAlpha * (1.0000 - 0.9990);
        assertThat(ema1).isCloseTo(expectedEma, within(1e-8));

        // Binance trade at 6,000 ms (5s after first Binance trade so micro-VWAP is dominated by new trade)
        indicator.onBinanceUsdtTrade(6_000L, 90_000.0, 10.0);
        assertThat(indicator.getLastSpotPriceUsd()).isCloseTo(90_000.0 * expectedEma, within(1e-2));
    }

    @Test
    @DisplayName("Should aggregate Binance, Coinbase, and Kraken spot trades using liquidity and volume weights")
    void shouldAggregateMultiExchangeSpotTradesWithLiquidityAndVolumeWeights() {
        SpotTwapIndicator indicator = new SpotTwapIndicator();
        indicator.onUsdtUsdTrade(1_000L, 1.0);

        indicator.onBinanceUsdtTrade(1_000L, 70_000.0, 1.0);
        indicator.onCoinbaseUsdTrade(1_000L, 70_020.0, 1.0);
        indicator.onKrakenUsdTrade(1_000L, 70_040.0, 1.0);

        double expectedSpot = 0.415 * 70_000.0 + 0.345 * 70_020.0 + 0.240 * 70_040.0;
        assertThat(indicator.getLastSpotPriceUsd()).isCloseTo(expectedSpot, within(1e-4));
    }

    @Test
    @DisplayName("Should down-weight single-exchange anomalous flash wick using 3-venue median outlier penalty")
    void shouldRejectSingleExchangeOutlierFlashWick() {
        SpotTwapIndicator indicator = new SpotTwapIndicator();
        indicator.onUsdtUsdTrade(1_000L, 1.0);

        indicator.onBinanceUsdtTrade(1_000L, 70_000.0, 1.0);
        indicator.onCoinbaseUsdTrade(1_000L, 70_000.0, 1.0);
        indicator.onKrakenUsdTrade(1_000L, 71_400.0, 1.0);

        assertThat(indicator.getLastSpotPriceUsd()).isBetween(70_000.0, 70_002.0);
    }

    @Test
    @DisplayName("Should decay stale exchange weight exponentially when one exchange stops trading")
    void shouldDecayStaleExchangeWeightOverTime() {
        SpotTwapIndicator indicator = new SpotTwapIndicator();
        indicator.onUsdtUsdTrade(0L, 1.0);

        indicator.onBinanceUsdtTrade(0L, 70_000.0, 1.0);
        indicator.onCoinbaseUsdTrade(0L, 70_000.0, 1.0);
        indicator.onKrakenUsdTrade(0L, 70_000.0, 1.0);

        indicator.onBinanceUsdtTrade(25_000L, 70_050.0, 2.0);
        indicator.onCoinbaseUsdTrade(25_000L, 70_050.0, 2.0);

        assertThat(indicator.getLastSpotPriceUsd()).isBetween(70_049.0, 70_050.0);
    }

    @Test
    @DisplayName("Should compute continuous 60-second time-weighted average price across step intervals and evict older than 60s")
    void shouldComputeContinuous60sTwapAndEvictOldIntervals() {
        SpotTwapIndicator indicator = new SpotTwapIndicator();
        indicator.onUsdtUsdTrade(0L, 1.0);

        indicator.onBinanceUsdtTrade(0L, 80_000.0, 1.0);
        assertThat(indicator.getLastTwap60sPriceUsd()).isCloseTo(80_000.0, within(1e-6));

        indicator.onBinanceUsdtTrade(30_000L, 80_600.0, 1.0);
        assertThat(indicator.getLastSpotPriceUsd()).isCloseTo(80_600.0, within(1e-6));
        assertThat(indicator.getLastTwap60sPriceUsd()).isCloseTo(80_000.0, within(1e-6));

        indicator.onBinanceUsdtTrade(60_000L, 80_600.0, 1.0);
        assertThat(indicator.getLastTwap60sPriceUsd()).isCloseTo(80_300.0, within(1e-6));

        indicator.onBinanceUsdtTrade(75_000L, 80_600.0, 1.0);
        assertThat(indicator.getLastTwap60sPriceUsd()).isCloseTo(80_450.0, within(1e-6));

        indicator.onBinanceUsdtTrade(90_000L, 80_600.0, 1.0);
        assertThat(indicator.getLastTwap60sPriceUsd()).isCloseTo(80_600.0, within(1e-6));
        assertThat(indicator.getLastAssetTimestampMs()).isEqualTo(90_000L);

        indicator.onKrakenUsdTrade(89_995L, 80_610.0, 0.5);
        assertThat(indicator.getLastAssetTimestampMs()).isEqualTo(90_000L);

        indicator.reset();
        assertThat(indicator.getLastAssetTimestampMs()).isEqualTo(-1L);
    }

    @Test
    @DisplayName("Should notify multiple registered SpotTwapListeners and ignore duplicates or removed listeners")
    void shouldNotifyMultipleRegisteredListeners() {
        SpotTwapIndicator indicator = new SpotTwapIndicator();
        assertThatThrownBy(() -> indicator.addListener(null))
                .isInstanceOf(IllegalArgumentException.class);

        List<String> firstEvents = new ArrayList<>();
        List<String> secondEvents = new ArrayList<>();

        SpotTwapListener first = (ts, symbol, spot, twap) ->
                firstEvents.add(ts + ":" + symbol + ":" + Math.round(spot) + ":" + Math.round(twap));
        SpotTwapListener second = (ts, symbol, spot, twap) ->
                secondEvents.add(ts + ":" + symbol + ":" + Math.round(spot) + ":" + Math.round(twap));

        indicator.addListener(first);
        indicator.addListener(first); // duplicate ignored
        indicator.addListener(second);

        // Before any trade, notifyListeners does nothing
        indicator.notifyListeners(1_000L, "btcusd");
        assertThat(firstEvents).isEmpty();
        assertThat(secondEvents).isEmpty();

        indicator.onBinanceUsdtTrade(1_000L, 70_000.0, 1.0);
        indicator.notifyListeners(1_000L, "btcusd");

        assertThat(firstEvents).containsExactly("1000:btcusd:70000:70000");
        assertThat(secondEvents).containsExactly("1000:btcusd:70000:70000");

        indicator.removeListener(first);
        indicator.onBinanceUsdtTrade(2_000L, 70_100.0, 1.0);
        indicator.notifyListeners(2_000L, "btcusd");

        assertThat(firstEvents).hasSize(1);
        assertThat(secondEvents).hasSize(2);

        indicator.clearListeners();
        indicator.onBinanceUsdtTrade(3_000L, 70_200.0, 1.0);
        indicator.notifyListeners(3_000L, "btcusd");
        assertThat(secondEvents).hasSize(2);
    }
}
