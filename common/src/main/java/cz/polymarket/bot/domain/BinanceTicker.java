package cz.polymarket.bot.domain;

import java.math.BigDecimal;

public record BinanceTicker(long eventTimeMs, BigDecimal price) {
    public BinanceTicker {
        if (price == null) {
            throw new IllegalArgumentException("Price cannot be null");
        }
        if (eventTimeMs <= 0) {
            throw new IllegalArgumentException("Event time must be positive");
        }
    }
}
