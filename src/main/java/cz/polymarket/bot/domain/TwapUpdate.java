package cz.polymarket.bot.domain;

import java.math.BigDecimal;

public record TwapUpdate(
        Timeframe timeframe,
        long candleStart,
        long candleEnd,
        BigDecimal openPrice,
        TwapPoint point,
        boolean isNewCandle
) {}
