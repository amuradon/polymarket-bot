package cz.polymarket.bot.cache;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class HourlyPriceCacheTest {

    private HourlyPriceCache cache;

    @BeforeEach
    void setUp() {
        cache = new HourlyPriceCache(3600);
    }

    @Test
    void shouldStoreAndRetrievePrice() {
        long t = 1000L;
        BigDecimal price = new BigDecimal("78000.00");

        cache.put(t, price);

        assertThat(cache.get(t)).isEqualByComparingTo(price);
        assertThat(cache.get(1001L)).isNull();
    }

    @Test
    void shouldCheckIfFullRangeIsAvailable() {
        long start = 1000L;
        long end = 1005L;

        for (long t = start; t <= end; t++) {
            cache.put(t, new BigDecimal("78000.00"));
        }

        assertThat(cache.hasFullRange(start, end)).isTrue();

        // If one second is missing in range
        HourlyPriceCache incompleteCache = new HourlyPriceCache(3600);
        incompleteCache.put(1000L, new BigDecimal("78000.00"));
        incompleteCache.put(1001L, new BigDecimal("78000.00"));
        // 1002 missing
        incompleteCache.put(1003L, new BigDecimal("78000.00"));

        assertThat(incompleteCache.hasFullRange(1000L, 1003L)).isFalse();
    }

    @Test
    void shouldReturnRangeOfPrices() {
        for (long t = 1000L; t <= 1005L; t++) {
            cache.put(t, BigDecimal.valueOf(t));
        }

        Map<Long, BigDecimal> range = cache.getRange(1001L, 1004L);
        assertThat(range).hasSize(4);
        assertThat(range.get(1001L)).isEqualByComparingTo("1001");
        assertThat(range.get(1004L)).isEqualByComparingTo("1004");
    }

    @Test
    void shouldEvictEntriesOlderThanMaxRetention() {
        HourlyPriceCache smallCache = new HourlyPriceCache(5); // retains 5 seconds
        smallCache.put(100L, new BigDecimal("100"));
        smallCache.put(101L, new BigDecimal("101"));
        smallCache.put(102L, new BigDecimal("102"));
        smallCache.put(103L, new BigDecimal("103"));
        smallCache.put(104L, new BigDecimal("104"));
        smallCache.put(105L, new BigDecimal("105"));
        smallCache.put(106L, new BigDecimal("106")); // 100 is older than (106 - 5)

        assertThat(smallCache.get(100L)).isNull();
        assertThat(smallCache.get(106L)).isNotNull();
    }
}
