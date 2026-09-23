package cz.polymarket.bot.strategy;

import java.math.BigDecimal;

/**
 * Execution report notification for an order (fill, partial fill, cancellation, rejection).
 */
public record ExecutionReport(
        String clientOrderId,
        String exchangeOrderId,
        String status, // "NEW", "FILLED", "PARTIALLY_FILLED", "CANCELLED", "REJECTED"
        BigDecimal executedPrice,
        BigDecimal executedSize,
        long timestampMs,
        String message
) {
    public ExecutionReport {
        if (clientOrderId == null || clientOrderId.isBlank()) {
            throw new IllegalArgumentException("clientOrderId cannot be null or blank");
        }
        if (status == null || status.isBlank()) {
            throw new IllegalArgumentException("status cannot be null or blank");
        }
    }
}
