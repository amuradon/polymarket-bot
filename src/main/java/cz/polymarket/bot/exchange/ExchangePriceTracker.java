package cz.polymarket.bot.exchange;

import cz.polymarket.bot.calculator.MedianCalculator;
import cz.polymarket.bot.domain.Exchange;
import cz.polymarket.bot.domain.PriceSnapshot;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;

@ApplicationScoped
public class ExchangePriceTracker {

    private static final int RETENTION_SECONDS = 120;
    private static final int WINDOW_SIZE_SECONDS = 60;

    private final MedianCalculator medianCalculator;
    private final AtomicReference<BigDecimal> binancePrice = new AtomicReference<>();
    private final AtomicReference<BigDecimal> coinbasePrice = new AtomicReference<>();
    private final AtomicReference<BigDecimal> krakenPrice = new AtomicReference<>();
    private final AtomicReference<BigDecimal> lastKnownMedian = new AtomicReference<>();
    private final ConcurrentSkipListMap<Long, BigDecimal> recentMedians = new ConcurrentSkipListMap<>();

    @Inject
    public ExchangePriceTracker(MedianCalculator medianCalculator) {
        this.medianCalculator = medianCalculator;
    }

    public void updatePrice(Exchange exchange, BigDecimal price) {
        if (exchange == null || price == null) {
            throw new IllegalArgumentException("Exchange and price cannot be null");
        }
        switch (exchange) {
            case BINANCE -> binancePrice.set(price);
            case COINBASE -> coinbasePrice.set(price);
            case KRAKEN -> krakenPrice.set(price);
        }
    }

    public Optional<PriceSnapshot> getSnapshot(Instant timestamp) {
        BigDecimal b = binancePrice.get();
        BigDecimal c = coinbasePrice.get();
        BigDecimal k = krakenPrice.get();

        if (b == null || c == null || k == null) {
            return Optional.empty();
        }

        BigDecimal median = medianCalculator.calculate(b, c, k);
        recordMedian(timestamp.getEpochSecond(), median);
        return Optional.of(new PriceSnapshot(timestamp, b, c, k, median));
    }

    public void recordMedian(long timestampSec, BigDecimal medianPrice) {
        if (medianPrice == null) {
            throw new IllegalArgumentException("Median price cannot be null");
        }
        lastKnownMedian.set(medianPrice);
        recentMedians.put(timestampSec, medianPrice);
        evictOldEntries(timestampSec);
    }

    public void recordForwardFilledMedian(long timestampSec) {
        BigDecimal last = lastKnownMedian.get();
        if (last != null) {
            recordMedian(timestampSec, last);
        }
    }

    public void seedMedianHistory(Map<Long, BigDecimal> historicalMedians) {
        if (historicalMedians != null && !historicalMedians.isEmpty()) {
            recentMedians.putAll(historicalMedians);
            Map.Entry<Long, BigDecimal> lastEntry = recentMedians.lastEntry();
            if (lastEntry != null) {
                lastKnownMedian.set(lastEntry.getValue());
            }
        }
    }

    public List<BigDecimal> getLast60SecondsMedians(long timestampSec) {
        long startSec = timestampSec - (WINDOW_SIZE_SECONDS - 1L);
        NavigableMap<Long, BigDecimal> subMap = recentMedians.subMap(startSec, true, timestampSec, true);
        return new ArrayList<>(subMap.values());
    }

    private void evictOldEntries(long currentTimestampSec) {
        long threshold = currentTimestampSec - RETENTION_SECONDS;
        recentMedians.headMap(threshold, true).clear();
    }
}
