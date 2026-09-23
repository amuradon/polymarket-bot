package cz.polymarket.bot.cache;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.math.BigDecimal;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

@ApplicationScoped
public class HourlyPriceCache {

    private final long maxRetentionSeconds;
    private final ConcurrentSkipListMap<Long, BigDecimal> cache = new ConcurrentSkipListMap<>();

    @Inject
    public HourlyPriceCache(@ConfigProperty(name = "polymarket.twap.cache-seconds", defaultValue = "3600") long maxRetentionSeconds) {
        this.maxRetentionSeconds = maxRetentionSeconds;
    }

    public void put(long timestampSec, BigDecimal price) {
        if (price == null) {
            throw new IllegalArgumentException("Price cannot be null");
        }
        cache.put(timestampSec, price);
        evictOldEntries(timestampSec);
    }

    public BigDecimal get(long timestampSec) {
        return cache.get(timestampSec);
    }

    public boolean hasFullRange(long startSec, long endSec) {
        if (startSec > endSec) {
            throw new IllegalArgumentException("Start time must be less than or equal to end time");
        }
        for (long t = startSec; t <= endSec; t++) {
            if (!cache.containsKey(t)) {
                return false;
            }
        }
        return true;
    }

    public Map<Long, BigDecimal> getRange(long startSec, long endSec) {
        if (startSec > endSec) {
            throw new IllegalArgumentException("Start time must be less than or equal to end time");
        }
        NavigableMap<Long, BigDecimal> subMap = cache.subMap(startSec, true, endSec, true);
        return new TreeMap<>(subMap);
    }

    private void evictOldEntries(long currentTimestampSec) {
        long threshold = currentTimestampSec - maxRetentionSeconds;
        cache.headMap(threshold, true).clear();
    }
}
