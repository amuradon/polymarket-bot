package cz.polymarket.bot.indicators;

/**
 * Zero-allocation listener notified when {@link FastSpotMicroIndicator} computes an updated
 * direct-USD fast micro-VWAP price, 1-second 3-venue signed order flow imbalance (OFI),
 * and 500-millisecond 3-venue synchronized price impulse.
 */
@FunctionalInterface
public interface FastSpotMicroListener {

    /**
     * Invoked when an asset trade updates the fast microstructure indicator state.
     *
     * @param timestampMs     effective receipt timestamp in milliseconds UTC
     * @param symbol          lowercase USD crypto symbol (e.g. {@code "btcusd"})
     * @param fastDirectUsd   40ms micro-VWAP direct-USD spot price ({@code 0.70 Coinbase + 0.30 Kraken}, Binance USD fallback)
     * @param signedOfi1s     1000ms continuous-time 3-venue signed order flow imbalance in {@code [-1.0, +1.0]}
     * @param syncMove500Usd  500ms 3-venue synchronized directional impulse in USD ({@code 0.0} unless all 3 venues agree in sign)
     */
    void onFastSpotMicro(
            long timestampMs,
            String symbol,
            double fastDirectUsd,
            double signedOfi1s,
            double syncMove500Usd);
}
