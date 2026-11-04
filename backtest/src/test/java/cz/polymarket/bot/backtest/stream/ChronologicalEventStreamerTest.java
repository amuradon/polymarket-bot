package cz.polymarket.bot.backtest.stream;

import cz.polymarket.bot.backtest.data.CollectorMarketMetadata;
import cz.polymarket.bot.backtest.service.CollectorParquetDatasetService;
import cz.polymarket.bot.domain.Timeframe;
import cz.polymarket.bot.strategy.ExternalStreamType;
import cz.polymarket.bot.strategy.OrderCommand;
import cz.polymarket.bot.strategy.TradeDirection;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class ChronologicalEventStreamerTest {

    @Test
    @DisplayName("Should pop events strictly in ascending receipt timestamp (ts) order")
    void shouldPopEventsInAscendingOrder() {
        try (ChronologicalEventStreamer streamer = new ChronologicalEventStreamer()) {
            streamer.scheduleEvent(new BacktestEvent.BinanceSpotTradeEvent(3000L, 60200.0, 1.0, false));
            streamer.scheduleEvent(new BacktestEvent.BinanceSpotTradeEvent(1000L, 60000.0, 1.0, false));
            streamer.scheduleEvent(new BacktestEvent.BinanceFuturesTradeEvent(2000L, 60100.0, 1.0, false));

            List<Long> timestamps = new ArrayList<>();
            while (streamer.hasNext()) {
                timestamps.add(streamer.next().timestampMs());
            }

            assertThat(timestamps).containsExactly(1000L, 2000L, 3000L);
        }
    }

    @Test
    @DisplayName("Should stream Polymarket CLOB, price_crypto, and price_crypto_twap_60s while skipping external streams when not requested")
    void shouldStreamPolymarketAndSkipUnrequestedExternalStreams(@TempDir Path tempDir) throws Exception {
        CollectorParquetDatasetService datasetService = new CollectorParquetDatasetService(tempDir.toString());

        long tStart = 1785542400L; // 2026-08-01 00:00:00 UTC
        long tEnd = tStart + 900L;
        String dateStr = "2026-08-01";

        Path pmDir = datasetService.getPolymarket15mDir("BTCUSDT");
        Path cryptoPriceFile = datasetService.getPolymarketCryptoPriceFile("BTCUSDT", dateStr);
        Path twap60sFile = datasetService.getPolymarketCryptoTwap60sFile("BTCUSDT", dateStr);
        Path spotFile = datasetService.getBinanceSpotAggTradesFile("BTCUSDT", dateStr);
        Path krakenFile = datasetService.getKrakenSpotUsdtUsdTradesFile(dateStr);
        Path futFile = datasetService.getBinanceFuturesAggTradesFile("BTCUSDT", dateStr);

        Files.createDirectories(pmDir);
        Files.createDirectories(cryptoPriceFile.getParent());
        Files.createDirectories(twap60sFile.getParent());
        Files.createDirectories(spotFile.getParent());
        Files.createDirectories(krakenFile.getParent());
        Files.createDirectories(futFile.getParent());

        Path pmFile = pmDir.resolve("btc-updown-15m-" + tStart + ".parquet");

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement()) {

            String normPm = pmFile.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + (tStart * 1000L + 500L) + "::UBIGINT, 'clob_ws'::VARCHAR, 'price_change'::VARCHAR, " +
                    "'0x1'::VARCHAR, NULL::VARCHAR, NULL::DOUBLE, NULL::DOUBLE, NULL::VARCHAR, " +
                    "'{\"market\":\"0x1\",\"price_changes\":[{\"asset_id\":\"1001\",\"price\":\"0.52\",\"size\":\"200\",\"side\":\"SELL\"}],\"timestamp\":\"" + (tStart * 1000L + 490L) + "\"}'::JSON)" +
                    ") AS t(ts, msg_type, event_type, market, asset_id, price, size, side, payload)) TO '" + normPm + "' (FORMAT PARQUET)");

            String normCrypto = cryptoPriceFile.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + (tStart * 1000L + 700L) + "::UBIGINT, 1::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60050.0::DOUBLE, '60050.0'::VARCHAR, " + (tStart * 1000L + 690L) + "::UBIGINT)" +
                    ") AS t(ts, seq, symbol, source, price, full_accuracy_price, price_timestamp)) TO '" + normCrypto + "' (FORMAT PARQUET)");

            String normTwap = twap60sFile.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + (tStart * 1000L + 800L) + "::UBIGINT, 1::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60025.0::DOUBLE, '60025.0'::VARCHAR, 60::INT, " + (tStart * 1000L + 790L) + "::UBIGINT)" +
                    ") AS t(ts, seq, symbol, source, twap_price, full_accuracy_twap_price, window_seconds, twap_timestamp)) TO '" + normTwap + "' (FORMAT PARQUET)");

            String normSpot = spotFile.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + (tStart * 1000L + 1000L) + "::UBIGINT, 1::BIGINT, 60000.0::DOUBLE, 0.5::DOUBLE, false::BOOLEAN)" +
                    ") AS t(ts, aggregate_trade_id, price, quantity, is_buyer_maker)) TO '" + normSpot + "' (FORMAT PARQUET)");

            String normKraken = krakenFile.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + (tStart * 1000L + 900L) + "::UBIGINT, 0.9990::DOUBLE)" +
                    ") AS t(ts, price)) TO '" + normKraken + "' (FORMAT PARQUET)");

            String normFut = futFile.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + (tStart * 1000L + 1500L) + "::UBIGINT, 1::BIGINT, 60010.0::DOUBLE, 1.0::DOUBLE, false::BOOLEAN)" +
                    ") AS t(ts, aggregate_trade_id, price, quantity, is_buyer_maker)) TO '" + normFut + "' (FORMAT PARQUET)");
        }

        CollectorMarketMetadata metadata = new CollectorMarketMetadata(
                tStart, tEnd, "2026-08-01T00:00:00Z", dateStr, pmFile,
                "1001", "1002", 60000.0, 60120.0, TradeDirection.UP
        );

        // Case 1: Strategy requires NO external streams -> CalculatedSpotTwapEvent IS always streamed,
        // while raw Binance Spot/Futures events must NOT be loaded
        try (ChronologicalEventStreamer streamer = new ChronologicalEventStreamer()) {
            streamer.loadCandleEvents(metadata, datasetService, "BTCUSDT", Set.of());

            int clobCount = 0;
            int calcSpotTwapCount = 0;
            int cryptoPriceCount = 0;
            int twap60sCount = 0;
            int spotCount = 0;
            int futCount = 0;

            while (streamer.hasNext()) {
                BacktestEvent event = streamer.next();
                if (event instanceof BacktestEvent.PolymarketClobEvent) clobCount++;
                if (event instanceof BacktestEvent.CalculatedSpotTwapEvent calcEv) {
                    calcSpotTwapCount++;
                    assertThat(calcEv.symbol()).isEqualTo("btcusd");
                    assertThat(calcEv.spotPrice()).isCloseTo(60000.0 * 0.9990, org.assertj.core.api.Assertions.within(1e-4));
                }
                if (event instanceof BacktestEvent.PolymarketCryptoPriceEvent) cryptoPriceCount++;
                if (event instanceof BacktestEvent.PolymarketCryptoTwap60sEvent) twap60sCount++;
                if (event instanceof BacktestEvent.BinanceSpotTradeEvent) spotCount++;
                if (event instanceof BacktestEvent.BinanceFuturesTradeEvent) futCount++;
            }

            assertThat(clobCount).isEqualTo(1);
            assertThat(calcSpotTwapCount).isEqualTo(1);
            assertThat(cryptoPriceCount).isEqualTo(1);
            assertThat(twap60sCount).isEqualTo(1);
            assertThat(spotCount).isZero();
            assertThat(futCount).isZero();
        }

        // Case 2: Strategy requests Binance Spot and Futures aggTrades -> all streams loaded and ordered by ts
        try (ChronologicalEventStreamer streamer = new ChronologicalEventStreamer()) {
            streamer.loadCandleEvents(metadata, datasetService, "BTCUSDT", Set.of(
                    ExternalStreamType.BINANCE_SPOT_AGG_TRADES,
                    ExternalStreamType.BINANCE_FUTURES_AGG_TRADES
            ));

            long prevTs = 0L;
            int spotCount = 0;
            int futCount = 0;
            while (streamer.hasNext()) {
                BacktestEvent event = streamer.next();
                assertThat(event.timestampMs()).isGreaterThanOrEqualTo(prevTs);
                prevTs = event.timestampMs();
                if (event instanceof BacktestEvent.BinanceSpotTradeEvent) spotCount++;
                if (event instanceof BacktestEvent.BinanceFuturesTradeEvent) futCount++;
            }

            assertThat(spotCount).isEqualTo(1);
            assertThat(futCount).isEqualTo(1);
        }
    }

    @Test
    @DisplayName("Should lazily merge-stream CLOB events with dynamically scheduled pending order events and reset cleanly")
    void shouldLazilyMergeStreamClobEventsWithDynamicEvents(@TempDir Path tempDir) throws Exception {
        long tStart = 1785542400L;
        long tEnd = tStart + 900L;
        long baseMs = tStart * 1000L;
        Path pmFile = tempDir.resolve("btc-updown-15m-" + tStart + ".parquet");

        try (Connection conn = ChronologicalEventStreamer.openStreamingConnection();
             Statement stmt = conn.createStatement()) {
            String normPm = pmFile.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + (baseMs + 100L) + "::UBIGINT, 'clob'::VARCHAR, 'price_change'::VARCHAR, '0x1'::VARCHAR, '1001'::VARCHAR, 0.51::DOUBLE, 100.0::DOUBLE, 'BUY'::VARCHAR, '{\"price_changes\":[]}'::JSON), " +
                    "(" + (baseMs + 200L) + "::UBIGINT, 'clob'::VARCHAR, 'clob'::VARCHAR, '0x1'::VARCHAR, NULL::VARCHAR, NULL::DOUBLE, NULL::DOUBLE, NULL::VARCHAR, '[]'::JSON), " +
                    "(" + (baseMs + 500L) + "::UBIGINT, 'clob'::VARCHAR, 'price_change'::VARCHAR, '0x1'::VARCHAR, '1001'::VARCHAR, 0.52::DOUBLE, 150.0::DOUBLE, 'BUY'::VARCHAR, '{\"price_changes\":[]}'::JSON), " +
                    "(" + (baseMs + 900L) + "::UBIGINT, 'clob'::VARCHAR, 'last_trade_price'::VARCHAR, '0x1'::VARCHAR, '1001'::VARCHAR, 0.53::DOUBLE, 50.0::DOUBLE, 'BUY'::VARCHAR, '{\"price\":\"0.53\"}'::JSON)" +
                    ") AS t(ts, msg_type, event_type, market, asset_id, price, size, side, payload)) TO '" + normPm + "' (FORMAT PARQUET)");
        }

        CollectorMarketMetadata metadata = new CollectorMarketMetadata(
                tStart, tEnd, "2026-08-01T00:00:00Z", "2026-08-01", pmFile,
                "1001", "1002", 60000.0, 60100.0, TradeDirection.UP
        );

        try (ChronologicalEventStreamer streamer = new ChronologicalEventStreamer()) {
            streamer.loadCandleEvents(metadata, new BacktestEvent[0]);

            List<BacktestEvent> observed = new ArrayList<>();
            boolean scheduledDynamicOrder = false;

            while (streamer.hasNext()) {
                BacktestEvent ev = streamer.next();
                observed.add(ev);
                if (!scheduledDynamicOrder && ev instanceof BacktestEvent.PolymarketClobEvent clob && clob.timestampMs() == baseMs + 100L) {
                    scheduledDynamicOrder = true;
                    streamer.scheduleEvent(new BacktestEvent.PendingOrderExecutionEvent(
                            baseMs + 300L,
                            new OrderCommand("ord-1", "0x1", "1001", Timeframe.FIFTEEN_MINUTES, "BUY", BigDecimal.valueOf(0.52), BigDecimal.TEN),
                            50L,
                            1L
                    ));
                }
            }

            // Empty 'clob' [] payload at +200L must be skipped; dynamic order at +300L must be interleaved before +500L CLOB event
            List<Long> clobAndOrderTimestamps = observed.stream()
                    .filter(e -> e instanceof BacktestEvent.PolymarketClobEvent || e instanceof BacktestEvent.PendingOrderExecutionEvent)
                    .map(BacktestEvent::timestampMs)
                    .toList();

            assertThat(clobAndOrderTimestamps).containsExactly(
                    baseMs + 100L,
                    baseMs + 300L,
                    baseMs + 500L,
                    baseMs + 900L
            );

            // Verify clear() mid-stream closes active ResultSet cleanly and allows reloading
            streamer.loadCandleEvents(metadata, new BacktestEvent[0]);
            assertThat(streamer.hasNext()).isTrue();
            streamer.clear();
            assertThat(streamer.hasNext()).isFalse();
        }
    }
}
