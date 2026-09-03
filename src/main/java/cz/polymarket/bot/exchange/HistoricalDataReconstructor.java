package cz.polymarket.bot.exchange;

import cz.polymarket.bot.cache.HourlyPriceCache;
import cz.polymarket.bot.calculator.TwapCalculator;
import cz.polymarket.bot.domain.CandleTwapState;
import cz.polymarket.bot.domain.Timeframe;
import cz.polymarket.bot.domain.TwapPoint;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@ApplicationScoped
public class HistoricalDataReconstructor {

    private final HourlyPriceCache cache;
    private final BinanceHistoricalClient binanceClient;
    private final TwapCalculator twapCalculator;

    @Inject
    public HistoricalDataReconstructor(
            HourlyPriceCache cache,
            BinanceHistoricalClient binanceClient,
            TwapCalculator twapCalculator) {
        this.cache = cache;
        this.binanceClient = binanceClient;
        this.twapCalculator = twapCalculator;
    }

    public CandleTwapState reconstructCandle(Timeframe timeframe, Instant now) {
        if (timeframe == null || now == null) {
            throw new IllegalArgumentException("Timeframe and timestamp cannot be null");
        }

        Instant candleStart = timeframe.getCandleStart(now);
        Instant candleEnd = timeframe.getCandleEnd(candleStart);

        long startSec = candleStart.getEpochSecond();
        long nowSec = now.getEpochSecond();

        Map<Long, BigDecimal> priceMap;

        if (cache.hasFullRange(startSec, nowSec)) {
            priceMap = cache.getRange(startSec, nowSec);
        } else {
            // Cache miss (startup or incomplete range) -> fetch from Binance 1s klines
            priceMap = binanceClient.fetch1sKlines(startSec, nowSec);
            // Backfill cache
            for (Map.Entry<Long, BigDecimal> entry : priceMap.entrySet()) {
                cache.put(entry.getKey(), entry.getValue());
            }
        }

        if (priceMap.isEmpty()) {
            throw new IllegalStateException("Unable to reconstruct candle history: no price data available");
        }

        // Get open price at candle start (or the earliest available in candle)
        BigDecimal openPrice = priceMap.get(startSec);
        if (openPrice == null) {
            openPrice = priceMap.values().iterator().next();
        }

        List<TwapPoint> points = twapCalculator.calculateSeries(startSec, openPrice, priceMap);

        return new CandleTwapState(
                timeframe,
                startSec,
                candleEnd.getEpochSecond(),
                openPrice,
                points
        );
    }
}
