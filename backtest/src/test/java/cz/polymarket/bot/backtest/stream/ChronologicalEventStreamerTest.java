package cz.polymarket.bot.backtest.stream;

import cz.polymarket.bot.backtest.data.CollectorMarketMetadata;
import cz.polymarket.bot.backtest.service.CollectorParquetDatasetService;
import cz.polymarket.bot.strategy.ExternalStreamType;
import cz.polymarket.bot.strategy.TradeDirection;
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
        Path futFile = datasetService.getBinanceFuturesAggTradesFile("BTCUSDT", dateStr);

        Files.createDirectories(pmDir);
        Files.createDirectories(cryptoPriceFile.getParent());
        Files.createDirectories(twap60sFile.getParent());
        Files.createDirectories(spotFile.getParent());
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

            String normFut = futFile.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + (tStart * 1000L + 1500L) + "::UBIGINT, 1::BIGINT, 60010.0::DOUBLE, 1.0::DOUBLE, false::BOOLEAN)" +
                    ") AS t(ts, aggregate_trade_id, price, quantity, is_buyer_maker)) TO '" + normFut + "' (FORMAT PARQUET)");
        }

        CollectorMarketMetadata metadata = new CollectorMarketMetadata(
                tStart, tEnd, "2026-08-01T00:00:00Z", dateStr, pmFile,
                "1001", "1002", 60000.0, 60120.0, TradeDirection.UP
        );

        // Case 1: Strategy requires NO external streams -> Binance Spot/Futures must NOT be loaded
        try (ChronologicalEventStreamer streamer = new ChronologicalEventStreamer()) {
            streamer.loadCandleEvents(metadata, datasetService, "BTCUSDT", Set.of());

            int clobCount = 0;
            int cryptoPriceCount = 0;
            int twap60sCount = 0;
            int spotCount = 0;
            int futCount = 0;

            while (streamer.hasNext()) {
                BacktestEvent event = streamer.next();
                if (event instanceof BacktestEvent.PolymarketClobEvent) clobCount++;
                if (event instanceof BacktestEvent.PolymarketCryptoPriceEvent) cryptoPriceCount++;
                if (event instanceof BacktestEvent.PolymarketCryptoTwap60sEvent) twap60sCount++;
                if (event instanceof BacktestEvent.BinanceSpotTradeEvent) spotCount++;
                if (event instanceof BacktestEvent.BinanceFuturesTradeEvent) futCount++;
            }

            assertThat(clobCount).isEqualTo(1);
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
}
