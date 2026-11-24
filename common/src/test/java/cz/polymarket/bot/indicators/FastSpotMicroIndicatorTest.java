package cz.polymarket.bot.indicators;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Deterministic unit tests for {@link FastSpotMicroIndicator} and {@link FastSpotMicroIndicatorConfig}.
 */
class FastSpotMicroIndicatorTest {

    private FastSpotMicroIndicator indicator;

    @BeforeEach
    void setUp() {
        indicator = new FastSpotMicroIndicator(FastSpotMicroIndicatorConfig.defaults());
    }

    @Test
    @DisplayName("Should validate config parameters and expose lexicographically ordered parameter map")
    void shouldValidateConfigAndParameterMap() {
        FastSpotMicroIndicatorConfig defaults = FastSpotMicroIndicatorConfig.defaults();
        assertThat(FastSpotMicroIndicatorConfig.unwrap(defaults)).isSameAs(defaults);
        assertThat(indicator.getName()).isEqualTo("FastSpotMicroIndicator");
        assertThat(new ArrayList<>(indicator.getConfigurationParameters().keySet()))
                .isSorted();

        assertThatThrownBy(() -> FastSpotMicroIndicatorConfig.unwrap(null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FastSpotMicroIndicatorConfig(
                0.0, 0.30, 1.0, 1.0, 1.0, 0.50, 0.20, 0.30,
                5000.0, 40.0, 1000.0, 6400.0, 500.0, 1500.0, 30000L, 25L, 65000L, 1800000L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("Should weight Direct-USD venues (0.70 CB + 0.30 KR) without Binance contamination and fall back only after silence")
    void shouldPrioritizeDirectUsdAndFallbackAfterSilence() {
        indicator.onUsdtUsdTrade(1_000L, 0.9990);
        assertThat(indicator.getUsdtUsdEma()).isCloseTo(0.9990, within(1e-9));

        // Binance trades at 70100 USDT -> 70029.90 USD; while Direct-USD is silent, fallback applies
        double bnOnly = indicator.onBinanceUsdtTrade(1_100L, 70_100.0, 2.0, true);
        assertThat(bnOnly).isCloseTo(70_100.0 * 0.9990, within(1e-4));

        // Once Coinbase and Kraken print at t=1200ms, Direct-USD strictly uses 0.70*CB + 0.30*KR!
        indicator.onCoinbaseUsdTrade(1_200L, 70_000.0, 1.5, true);
        double direct = indicator.onKrakenUsdTrade(1_200L, 70_020.0, 1.0, true);
        assertThat(direct).isCloseTo(0.70 * 70_000.0 + 0.30 * 70_020.0, within(1e-6));

        // A lagged Binance print at t=1250ms must NOT contaminate fastDirectUsd!
        double afterLaggedBinance = indicator.onBinanceUsdtTrade(1_250L, 69_900.0, 5.0, false);
        assertThat(afterLaggedBinance).isCloseTo(0.70 * 70_000.0 + 0.30 * 70_020.0, within(1e-6));

        // After >30000ms of Coinbase+Kraken silence, Binance USD fallback engages cleanly
        double fallback = indicator.onBinanceUsdtTrade(35_000L, 70_200.0, 1.0, true);
        assertThat(fallback).isCloseTo(70_200.0 * 0.9990, within(1e-4));
    }

    @Test
    @DisplayName("Should regularize dust OFI while preserving strong sweep OFI and firing 500ms sync strictly on 3-venue agreement")
    void shouldComputeSignedOfiAndThreeVenueSyncImpulse() {
        // Dust trade (0.001 BTC = $70 notional) must NOT saturate OFI near +1.0!
        indicator.onCoinbaseUsdTrade(5_000L, 70_000.0, 0.001, true);
        assertThat(indicator.getLastSignedOfi1s()).isCloseTo(70.0 / (70.0 + 6_400.0), within(1e-6));

        // Seed institutional 1 BTC sweeps ($210k total notional) at 70000 across all 3 venues
        indicator.reset();
        indicator.onCoinbaseUsdTrade(10_000L, 70_000.0, 1.0, true);
        indicator.onKrakenUsdTrade(10_000L, 70_000.0, 1.0, true);
        indicator.onBinanceUsdtTrade(10_000L, 70_000.0, 1.0, true);
        assertThat(indicator.getLastSignedOfi1s()).isCloseTo(210_000.0 / (210_000.0 + 6_400.0), within(1e-6));
        assertThat(indicator.getLastSyncMove500Usd()).isCloseTo(0.0, within(1e-6));

        // Mixed movement (CB & KR up, BN down) -> syncMove500Usd must be 0.0!
        indicator.onCoinbaseUsdTrade(10_050L, 70_025.0, 2.0, true);
        indicator.onKrakenUsdTrade(10_050L, 70_020.0, 1.0, true);
        indicator.onBinanceUsdtTrade(10_050L, 69_995.0, 2.0, false);
        assertThat(indicator.getLastSyncMove500Usd()).isEqualTo(0.0);
        assertThat(indicator.getLastSignedOfi1s()).isBetween(-1.0, 1.0);

        // Now Binance joins upward breakout -> all 3 venues > 500ms EMA -> positive syncMove500Usd!
        indicator.onBinanceUsdtTrade(10_080L, 70_030.0, 4.0, true);
        assertThat(indicator.getLastSyncMove500Usd()).isGreaterThan(10.0);
        assertThat(indicator.getLastSignedOfi1s()).isGreaterThan(0.40);

        // Listener dispatch & reset verification
        List<double[]> received = new ArrayList<>();
        FastSpotMicroListener listener = (ts, sym, px, ofi, sync) ->
                received.add(new double[]{ts, px, ofi, sync});
        indicator.addListener(listener);
        indicator.notifyListeners(10_080L, "btcusd");
        assertThat(received).hasSize(1);
        assertThat(received.getFirst()[1]).isGreaterThan(70_015.0);

        indicator.reset();
        assertThat(indicator.getLastFastDirectUsd()).isNaN();
        assertThat(indicator.getLastAssetTimestampMs()).isEqualTo(-1L);
    }
}
