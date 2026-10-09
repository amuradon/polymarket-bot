package cz.polymarket.bot.backtest.stream;

import cz.polymarket.bot.backtest.data.BacktestMarketRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ChronologicalEventStreamerTest {

    @Test
    @DisplayName("Should pop events strictly in ascending timestamp order")
    void shouldPopEventsInAscendingOrder() {
        ChronologicalEventStreamer streamer = new ChronologicalEventStreamer();

        streamer.scheduleEvent(new BacktestEvent.BinanceSpotTradeEvent(3000L, 60200.0, 1.0, false));
        streamer.scheduleEvent(new BacktestEvent.BinanceSpotTradeEvent(1000L, 60000.0, 1.0, false));
        streamer.scheduleEvent(new BacktestEvent.BinanceFuturesTradeEvent(2000L, 60100.0, 1.0, false));

        List<Long> timestamps = new ArrayList<>();
        while (streamer.hasNext()) {
            timestamps.add(streamer.next().timestampMs());
        }

        assertThat(timestamps).containsExactly(1000L, 2000L, 3000L);
    }

    @Test
    @DisplayName("Should synthesize candle events in chronological order when raw files unavailable")
    void shouldSynthesizeCandleEventsChronologically() {
        ChronologicalEventStreamer streamer = new ChronologicalEventStreamer();

        BacktestMarketRow row = new BacktestMarketRow(
                1000000L, 1000900L, "2026-08-12T00:00:00Z", "2026-08-12",
                60000.0, 60100.0, "UP",
                60000.0, 60200.0, 59900.0, 60100.0,
                10.0, 600000.0, 2.0,
                60010.0, 60110.0,
                15.0, 900000.0, 3.0,
                1.5, 1.6,
                0.52, 0.50, 0.53, 0.51, 0.54, 0.52, 0.55, 0.53,
                0.53, 0.015, 0.49, 0.015,
                0.60, 0.45, 300.0, 300.0
        );

        streamer.loadCandleEvents(row, false, null, "BTCUSDT");

        long prevTime = 0L;
        int count = 0;
        while (streamer.hasNext()) {
            BacktestEvent event = streamer.next();
            assertThat(event.timestampMs()).isGreaterThanOrEqualTo(prevTime);
            prevTime = event.timestampMs();
            count++;
        }

        assertThat(count).isGreaterThan(5);
    }
}
