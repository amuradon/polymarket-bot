package cz.polymarket.bot.domain;

import java.math.BigDecimal;

public record TwapPoint(
        long time,
        BigDecimal twap,
        BigDecimal medianPrice
) {}
