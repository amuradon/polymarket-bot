package cz.polymarket.bot.indicators;

/**
 * Listener interface implemented by trading strategies that consume real-time or replayed
 * Chainlink-emulated USD spot price and 60-second rolling TWAP updates from {@link SpotTwapIndicator}.
 */
public interface SpotTwapListener {

    /**
     * Triggered when a Chainlink-emulated USD spot price and 60-second rolling TWAP are calculated
     * for the asset by aggregating Binance Spot ({@code <ASSET>USDT} converted via Kraken
     * {@code USDT/USD} 5s EMA), Coinbase Spot ({@code <ASSET>USD}), and Kraken Spot
     * ({@code <ASSET>USD}) trades.
     *
     * @param timestampMs local EC2 receipt timestamp in milliseconds UTC
     * @param symbol lowercase USD asset symbol (e.g. {@code "btcusd"})
     * @param spotPrice calculated multi-exchange USD spot price
     * @param twap60sPrice calculated 60-second time-weighted average USD price
     */
    void onSpotTwap(
            long timestampMs,
            String symbol,
            double spotPrice,
            double twap60sPrice);
}
