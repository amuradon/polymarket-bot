package cz.polymarket.bot.domain;

/**
 * Immutable quote containing best bid/ask prices, depths, and estimated execution fill prices
 * for Polymarket UP/DOWN binary contracts.
 */
public record OrderBookQuote(
        double bestAskUp,
        double bestBidUp,
        double bestAskDown,
        double bestBidDown,
        double depth1cUp,
        double depth1cDown,
        double estimatedFillPriceUp,
        double estimatedFillPriceDown,
        long timestampMs
) {
    public OrderBookQuote {
        if (bestAskUp <= 0 || bestBidUp <= 0 || bestAskDown <= 0 || bestBidDown <= 0) {
            throw new IllegalArgumentException("Order book quotes must be positive");
        }
        if (estimatedFillPriceUp <= 0 || estimatedFillPriceDown <= 0) {
            throw new IllegalArgumentException("Estimated fill prices must be positive");
        }
    }
}
