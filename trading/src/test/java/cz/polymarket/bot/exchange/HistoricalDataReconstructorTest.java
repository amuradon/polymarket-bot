package cz.polymarket.bot.exchange;

import cz.polymarket.bot.cache.HourlyPriceCache;
import cz.polymarket.bot.calculator.TwapCalculator;
import cz.polymarket.bot.domain.CandleTwapState;
import cz.polymarket.bot.domain.Timeframe;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

class HistoricalDataReconstructorTest {

    private HourlyPriceCache cache;
    private BinanceHistoricalClient binanceClient;
    private TwapCalculator twapCalculator;
    private HistoricalDataReconstructor reconstructor;

    @BeforeEach
    void setUp() {
        cache = new HourlyPriceCache(3600);
        binanceClient = mock(BinanceHistoricalClient.class);
        twapCalculator = new TwapCalculator();
        reconstructor = new HistoricalDataReconstructor(cache, binanceClient, twapCalculator);
    }

    @Test
    void shouldReconstructFromCacheWhenFullRangeAvailable() {
        Instant now = Instant.parse("2026-09-03T14:02:00Z"); // 5m candle starts at 14:00:00 (120s elapsed)
        long startSec = Instant.parse("2026-09-03T14:00:00Z").getEpochSecond();
        long nowSec = now.getEpochSecond();

        for (long t = startSec; t <= nowSec; t++) {
            cache.put(t, new BigDecimal("78823.52"));
        }

        CandleTwapState state = reconstructor.reconstructCandle(Timeframe.FIVE_MINUTES, now);

        assertThat(state.candleStart()).isEqualTo(startSec);
        assertThat(state.openPrice()).isEqualByComparingTo("78823.52");
        assertThat(state.points()).hasSize(121);
        assertThat(state.points().get(0).twap()).isEqualByComparingTo("78823.52");
        verify(binanceClient, never()).fetch1sKlines(anyLong(), anyLong());
    }

    @Test
    void shouldReconstructFromBinanceWhenCacheMiss() {
        Instant now = Instant.parse("2026-09-03T14:02:00Z");
        long startSec = Instant.parse("2026-09-03T14:00:00Z").getEpochSecond();
        long nowSec = now.getEpochSecond();

        // 60 seconds before startSec up to nowSec
        long fetchStart = startSec - 59;
        Map<Long, BigDecimal> klines = new TreeMap<>();
        for (long t = fetchStart; t <= nowSec; t++) {
            klines.put(t, new BigDecimal("78050.00"));
        }
        when(binanceClient.fetch1sKlines(fetchStart, nowSec)).thenReturn(klines);

        CandleTwapState state = reconstructor.reconstructCandle(Timeframe.FIVE_MINUTES, now);

        assertThat(state.candleStart()).isEqualTo(startSec);
        assertThat(state.openPrice()).isEqualByComparingTo("78050.00");
        assertThat(state.points()).hasSize(121);
        verify(binanceClient, times(1)).fetch1sKlines(fetchStart, nowSec);

        // Verify 60s TWAPs were backfilled into cache
        assertThat(cache.hasFullRange(startSec, nowSec)).isTrue();
        assertThat(cache.get(startSec)).isEqualByComparingTo("78050.00");
    }
}
