package cz.polymarket.bot.domain;

import java.math.BigDecimal;

/**
 * Immutable representation of a Binance ticker event containing the event timestamp and last traded price.
 *
 * @param eventTimeMs exchange event timestamp in epoch milliseconds
 * @param price       latest traded price
 */
public record BinanceTicker(long eventTimeMs, BigDecimal price) {

    /**
     * Validates that the ticker price is non-null and the event timestamp is positive.
     */
    public BinanceTicker {
        if (price == null) {
            throw new IllegalArgumentException("Price cannot be null");
        }
        if (eventTimeMs <= 0) {
            throw new IllegalArgumentException("Event time must be positive");
        }
    }
}
