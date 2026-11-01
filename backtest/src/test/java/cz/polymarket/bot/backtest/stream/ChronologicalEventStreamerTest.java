package cz.polymarket.bot.backtest.stream;

import cz.polymarket.bot.backtest.cache.BinaryMarketCacheService;
import cz.polymarket.bot.backtest.cache.MissingRawDataRegistry;
import cz.polymarket.bot.backtest.cache.RawMarketDataProcessor;
import cz.polymarket.bot.backtest.data.BacktestMarketRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
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
    @DisplayName("Should not synthesize any trade or quote events when raw files are missing")
    void shouldNotSynthesizeEventsWhenRawFilesMissing(@TempDir Path tempDir) {
        ChronologicalEventStreamer streamer = new ChronologicalEventStreamer();
        BinaryMarketCacheService cacheService = new BinaryMarketCacheService(tempDir.resolve("cache").toString());
        MissingRawDataRegistry missingRegistry = new MissingRawDataRegistry("BTCUSDT");
        RawMarketDataProcessor processor = new RawMarketDataProcessor(
                cacheService, missingRegistry,
                tempDir.resolve("Binance").toString(),
                tempDir.resolve("Polymarket").toString()
        );

        BacktestMarketRow row = new BacktestMarketRow(
                1000000L, 1000900L, "2026-08-12T00:00:00Z", "2026-08-12",
                60000.0, 60100.0, "UP",
                60000.0, 60200.0, 59900.0, 60100.0,
                10.0, 600000.0, 2.0,
                60010.0, 60110.0,
                15.0, 900000.0, 3.0,
                1.5, 1.6,
                Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN,
                Double.NaN, Double.NaN, Double.NaN, Double.NaN,
                Double.NaN, Double.NaN, 0.0, 0.0
        );

        streamer.loadCandleEvents(row, processor, "BTCUSDT");

        List<BacktestEvent> events = new ArrayList<>();
        while (streamer.hasNext()) {
            events.add(streamer.next());
        }

        // Only the 3 mandatory candle lifecycle events exist; zero synthesized trade/quote events
        assertThat(events).hasSize(3);
        assertThat(events).allMatch(e -> e instanceof BacktestEvent.CandleLifecycleEvent);
    }

    @Test
    @DisplayName("Should stream raw Polymarket and Binance events in chronological order when raw Parquet files exist")
    void shouldStreamRawEventsChronologically(@TempDir Path tempDir) throws Exception {
        ChronologicalEventStreamer streamer = new ChronologicalEventStreamer();
        BinaryMarketCacheService cacheService = new BinaryMarketCacheService(tempDir.resolve("cache").toString());
        MissingRawDataRegistry missingRegistry = new MissingRawDataRegistry("BTCUSDT");
        RawMarketDataProcessor processor = new RawMarketDataProcessor(
                cacheService, missingRegistry,
                tempDir.resolve("Binance").toString(),
                tempDir.resolve("Polymarket").toString()
        );

        long tStart = 1785542400L; // 2026-08-01 00:00:00 UTC
        long tEnd = tStart + 900L;
        String dateStr = "2026-08-01";

        Path spotDir = processor.getBinanceSpotAggTradesDir("BTCUSDT");
        Path futDir = processor.getBinanceFuturesAggTradesDir("BTCUSDT");
        Path pmDir = processor.getPolymarketDir("BTCUSDT");
        Files.createDirectories(spotDir);
        Files.createDirectories(futDir);
        Files.createDirectories(pmDir);

        Path spotFile = spotDir.resolve("BTCUSDT-aggTrades-" + dateStr + ".parquet");
        Path futFile = futDir.resolve("BTCUSDT-aggTrades-" + dateStr + ".parquet");
        Path pmFile = pmDir.resolve(tStart + ".parquet");

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement()) {
            String normSpot = spotFile.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + (tStart * 1000L + 1000L) + "::BIGINT, 60000.0::DOUBLE, 0.5::DOUBLE, false::BOOLEAN), " +
                    "(" + ((tStart + 60L) * 1000L) + "::BIGINT, 60120.0::DOUBLE, 1.2::DOUBLE, false::BOOLEAN)" +
                    ") AS t(transact_time, price, quantity, is_buyer_maker)) TO '" + normSpot + "' (FORMAT PARQUET)");

            String normFut = futFile.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + (tStart * 1000L + 1500L) + "::BIGINT, 60010.0::DOUBLE, 1.0::DOUBLE, false::BOOLEAN)" +
                    ") AS t(transact_time, price, quantity, is_buyer_maker)) TO '" + normFut + "' (FORMAT PARQUET)");

            String normPm = pmFile.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "('snapshot'::VARCHAR, " + (tStart * 1000L + 500L) + "::BIGINT, 0.52::DOUBLE, 0.0::DOUBLE, 'BUY'::VARCHAR, '[{\"price\":\"0.50\",\"size\":\"200.0\"}]'::VARCHAR, '[{\"price\":\"0.52\",\"size\":\"200.0\"}]'::VARCHAR)" +
                    ") AS t(event_type, t, price, size, side, bids, asks)) TO '" + normPm + "' (FORMAT PARQUET)");
        }

        BacktestMarketRow row = new BacktestMarketRow(
                tStart, tEnd, "2026-08-01T00:00:00Z", dateStr,
                60000.0, 60120.0, "UP",
                60000.0, 60120.0, 60000.0, 60120.0,
                1.7, 102144.0, 1.7,
                60010.0, 60010.0,
                1.0, 60010.0, 1.0,
                1.6, 1.6,
                0.52, 0.50, 0.52, 0.50, 0.52, 0.50, 0.52, 0.50,
                0.52, 0.015, 0.50, 0.015,
                0.52, 0.50, 200.0, 200.0
        );

        streamer.loadCandleEvents(row, processor, "BTCUSDT");

        long prevTime = 0L;
        int l2Count = 0;
        int spotCount = 0;
        int futCount = 0;
        while (streamer.hasNext()) {
            BacktestEvent event = streamer.next();
            assertThat(event.timestampMs()).isGreaterThanOrEqualTo(prevTime);
            prevTime = event.timestampMs();
            if (event instanceof BacktestEvent.PolymarketL2UpdateEvent) l2Count++;
            if (event instanceof BacktestEvent.BinanceSpotTradeEvent) spotCount++;
            if (event instanceof BacktestEvent.BinanceFuturesTradeEvent) futCount++;
        }

        assertThat(l2Count).isEqualTo(1);
        assertThat(spotCount).isEqualTo(2);
        assertThat(futCount).isEqualTo(1);
    }
}
