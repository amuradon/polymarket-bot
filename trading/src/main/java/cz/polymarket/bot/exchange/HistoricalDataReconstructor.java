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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

@ApplicationScoped
public class HistoricalDataReconstructor {

    private static final int ROLLING_WINDOW_SECONDS = 60;

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

        List<TwapPoint> points = new ArrayList<>();
        BigDecimal openPrice;

        if (cache.hasFullRange(startSec, nowSec)) {
            Map<Long, BigDecimal> range = cache.getRange(startSec, nowSec);
            openPrice = range.get(startSec);
            for (Map.Entry<Long, BigDecimal> entry : range.entrySet()) {
                points.add(twapCalculator.createPoint(entry.getKey(), entry.getValue(), entry.getValue()));
            }
        } else {
            // Cache miss -> fetch Binance 1s klines including 60s lookback before candle start
            long fetchStart = startSec - (ROLLING_WINDOW_SECONDS - 1L);
            NavigableMap<Long, BigDecimal> binanceKlines = new TreeMap<>(binanceClient.fetch1sKlines(fetchStart, nowSec));

            if (binanceKlines.isEmpty()) {
                throw new IllegalStateException("Unable to reconstruct candle history: no price data available from Binance");
            }

            // Calculate 60s rolling TWAP for each second in [startSec, nowSec]
            for (long t = startSec; t <= nowSec; t++) {
                long windowStart = t - (ROLLING_WINDOW_SECONDS - 1L);
                NavigableMap<Long, BigDecimal> windowMap = binanceKlines.subMap(windowStart, true, t, true);
                List<BigDecimal> windowPrices = new ArrayList<>(windowMap.values());
                if (windowPrices.isEmpty()) {
                    continue;
                }
                BigDecimal rollingTwap = twapCalculator.calculateRollingTwap(windowPrices);
                cache.put(t, rollingTwap);

                BigDecimal latestPrice = windowPrices.get(windowPrices.size() - 1);
                points.add(twapCalculator.createPoint(t, rollingTwap, latestPrice));
            }

            if (points.isEmpty()) {
                throw new IllegalStateException("Failed to calculate any TWAP points from fetched history");
            }

            openPrice = cache.get(startSec);
            if (openPrice == null) {
                openPrice = points.get(0).twap();
            }
        }

        return new CandleTwapState(
                timeframe,
                startSec,
                candleEnd.getEpochSecond(),
                openPrice,
                points
        );
    }
}
