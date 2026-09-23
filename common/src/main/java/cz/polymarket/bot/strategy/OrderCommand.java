package cz.polymarket.bot.strategy;

import cz.polymarket.bot.domain.Timeframe;
import java.math.BigDecimal;

/**
 * Immutable command to submit or cancel an order.
 */
public record OrderCommand(
        String clientOrderId,
        String marketId,
        String token,
        Timeframe timeframe,
        String side, // "BUY" or "SELL"
        BigDecimal price,
        BigDecimal size
) {
    public OrderCommand {
        if (clientOrderId == null || clientOrderId.isBlank()) {
            throw new IllegalArgumentException("clientOrderId cannot be null or blank");
        }
        if (marketId == null || marketId.isBlank()) {
            throw new IllegalArgumentException("marketId cannot be null or blank");
        }
        if (side == null || side.isBlank()) {
            throw new IllegalArgumentException("side cannot be null or blank");
        }
        if (price == null || price.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("price must be positive");
        }
        if (size == null || size.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("size must be positive");
        }
    }
}
