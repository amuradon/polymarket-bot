package cz.polymarket.bot.connector.polymarket.stream;

/**
 * Normalized trade execution event parsed from the authenticated Polymarket CLOB User WebSocket channel ({@code /ws/user}).
 *
 * @param tradeId    unique trade identifier ({@code id})
 * @param status     trade settlement lifecycle status (e.g. {@code "MATCHED"}, {@code "MINED"}, {@code "CONFIRMED"}, {@code "FAILED"})
 * @param side       trade side for the account ({@code "BUY"} or {@code "SELL"})
 * @param size       executed share quantity
 * @param price      execution price per share in USD ({@code 0.0..1.0})
 * @param feeRateBps fee rate in basis points (e.g. {@code 700} for 7.00%, or {@code -1} if unspecified)
 * @param assetId    CLOB conditional token ID ({@code asset_id})
 * @param orderId    taker order ID ({@code taker_order_id})
 */
public record PolymarketUserTradeEvent(
        String tradeId,
        String status,
        String side,
        double size,
        double price,
        int feeRateBps,
        String assetId,
        String orderId
) {
}
