package cz.polymarket.bot.domain;

import java.math.BigDecimal;
import java.time.Instant;

public record PriceSnapshot(
        Instant timestamp,
        BigDecimal binancePrice,
        BigDecimal coinbasePrice,
        BigDecimal krakenPrice,
        BigDecimal medianPrice
) {}
