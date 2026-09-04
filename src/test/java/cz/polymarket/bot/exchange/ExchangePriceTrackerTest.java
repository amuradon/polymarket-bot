package cz.polymarket.bot.exchange;

import cz.polymarket.bot.calculator.MedianCalculator;
import cz.polymarket.bot.domain.Exchange;
import cz.polymarket.bot.domain.PriceSnapshot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class ExchangePriceTrackerTest {

    private ExchangePriceTracker tracker;

    @BeforeEach
    void setUp() {
        tracker = new ExchangePriceTracker(new MedianCalculator());
    }

    @Test
    void shouldReturnEmptySnapshotWhenPricesMissing() {
        assertThat(tracker.getSnapshot(Instant.now())).isEmpty();

        tracker.updatePrice(Exchange.BINANCE, new BigDecimal("78000"));
        assertThat(tracker.getSnapshot(Instant.now())).isEmpty();

        tracker.updatePrice(Exchange.COINBASE, new BigDecimal("78010"));
        assertThat(tracker.getSnapshot(Instant.now())).isEmpty();
    }

    @Test
    void shouldCreateSnapshotWithMedianWhenAllPricesPresent() {
        tracker.updatePrice(Exchange.BINANCE, new BigDecimal("78000"));
        tracker.updatePrice(Exchange.COINBASE, new BigDecimal("78010"));
        tracker.updatePrice(Exchange.KRAKEN, new BigDecimal("77990"));

        Instant now = Instant.parse("2026-09-03T14:00:00Z");
        Optional<PriceSnapshot> snapshot = tracker.getSnapshot(now);

        assertThat(snapshot).isPresent();
        assertThat(snapshot.get().binancePrice()).isEqualByComparingTo("78000");
        assertThat(snapshot.get().coinbasePrice()).isEqualByComparingTo("78010");
        assertThat(snapshot.get().krakenPrice()).isEqualByComparingTo("77990");
        assertThat(snapshot.get().medianPrice()).isEqualByComparingTo("78000");
        assertThat(snapshot.get().timestamp()).isEqualTo(now);
    }

    @Test
    void shouldTrackRecentMediansAndReturnLast60Seconds() {
        long t0 = 1000L;
        for (long t = t0; t < t0 + 60; t++) {
            tracker.recordMedian(t, BigDecimal.valueOf(t));
        }

        List<BigDecimal> window = tracker.getLast60SecondsMedians(t0 + 59);
        assertThat(window).hasSize(60);
        assertThat(window.get(0)).isEqualByComparingTo(String.valueOf(t0));
        assertThat(window.get(59)).isEqualByComparingTo(String.valueOf(t0 + 59));
    }

    @Test
    void shouldSeedMedianHistory() {
        Map<Long, BigDecimal> seed = Map.of(
                100L, new BigDecimal("78000"),
                101L, new BigDecimal("78005")
        );
        tracker.seedMedianHistory(seed);

        List<BigDecimal> window = tracker.getLast60SecondsMedians(101L);
        assertThat(window).hasSize(2);
    }
}
