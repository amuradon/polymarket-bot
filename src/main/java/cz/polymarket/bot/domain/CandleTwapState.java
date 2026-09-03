package cz.polymarket.bot.domain;

import java.math.BigDecimal;
import java.util.List;

public record CandleTwapState(
        Timeframe timeframe,
        long candleStart,
        long candleEnd,
        BigDecimal openPrice,
        List<TwapPoint> points
) {}
