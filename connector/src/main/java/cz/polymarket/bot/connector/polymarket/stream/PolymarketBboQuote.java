package cz.polymarket.bot.connector.polymarket.stream;

/**
 * Best Bid/Offer (BBO) snapshot across both Up (YES) and Down (NO) conditional tokens
 * extracted from the Polymarket CLOB Market WebSocket stream.
 *
 * @param bestAskUp   best ask price for the Up (YES) token in {@code [0.0, 1.0]}
 * @param bestBidUp   best bid price for the Up (YES) token in {@code [0.0, 1.0]}
 * @param bestAskDown best ask price for the Down (NO) token in {@code [0.0, 1.0]}
 * @param bestBidDown best bid price for the Down (NO) token in {@code [0.0, 1.0]}
 * @param askSizeUp   available share size at {@code bestAskUp}
 * @param askSizeDown available share size at {@code bestAskDown}
 * @param midUp       mid price for the Up (YES) token
 * @param midDown     mid price for the Down (NO) token
 * @param timestampMs local receipt timestamp in epoch milliseconds
 */
public record PolymarketBboQuote(
        double bestAskUp,
        double bestBidUp,
        double bestAskDown,
        double bestBidDown,
        double askSizeUp,
        double askSizeDown,
        double midUp,
        double midDown,
        long timestampMs
) {
}
