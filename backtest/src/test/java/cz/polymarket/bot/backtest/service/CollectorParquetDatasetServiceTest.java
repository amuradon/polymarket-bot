package cz.polymarket.bot.backtest.service;

import cz.polymarket.bot.backtest.data.CollectorMarketMetadata;
import cz.polymarket.bot.backtest.stream.BacktestEvent;
import cz.polymarket.bot.backtest.stream.ChronologicalEventStreamer;
import cz.polymarket.bot.strategy.TradeDirection;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class CollectorParquetDatasetServiceTest {

    @Test
    @DisplayName("Should discover market files purely by filename timestamp without reading Parquet metadata")
    void shouldDiscoverMarketsByFilenameTimestampWithoutReadingParquet(@TempDir Path tempDir) throws Exception {
        CollectorParquetDatasetService service = new CollectorParquetDatasetService(tempDir.toString());

        long beforeRange = 1785542400L - 900L; // 2026-07-31 23:45:00 UTC
        long c1Start = 1785542400L;            // 2026-08-01 00:00:00 UTC
        long c2Start = c1Start + 900L;         // 2026-08-01 00:15:00 UTC
        long afterRange = 1785628800L;         // 2026-08-02 00:00:00 UTC

        Path pmDir = service.getPolymarket15mDir("BTCUSDT");
        Files.createDirectories(pmDir);

        // Create empty dummy files (not valid Parquet) to prove discoverMarkets does NOT read file contents
        Files.writeString(pmDir.resolve("btc-updown-15m-" + beforeRange + ".parquet"), "dummy");
        Files.writeString(pmDir.resolve("btc-updown-15m-" + c2Start + ".parquet"), "dummy");
        Files.writeString(pmDir.resolve("btc-updown-15m-" + c1Start + ".parquet"), "dummy");
        Files.writeString(pmDir.resolve("btc-updown-15m-" + afterRange + ".parquet"), "dummy");

        List<CollectorMarketMetadata> markets = service.discoverMarkets("BTCUSDT", "2026-08-01", "2026-08-01");
        assertThat(markets).hasSize(2);

        CollectorMarketMetadata m1 = markets.get(0);
        assertThat(m1.tStart()).isEqualTo(c1Start);
        assertThat(m1.tEnd()).isEqualTo(c1Start + 900L);
        assertThat(m1.dateStr()).isEqualTo("2026-08-01");
        assertThat(m1.datetimeUtc()).isEqualTo("2026-08-01T00:00:00Z");
        assertThat(m1.upTokenId()).isNull();
        assertThat(m1.downTokenId()).isNull();
        assertThat(m1.twapOpen()).isNaN();
        assertThat(m1.twapClose()).isNaN();
        assertThat(m1.actualOutcome()).isNull();

        CollectorMarketMetadata m2 = markets.get(1);
        assertThat(m2.tStart()).isEqualTo(c2Start);
        assertThat(m2.tEnd()).isEqualTo(c2Start + 900L);
        assertThat(m2.upTokenId()).isNull();
        assertThat(m2.downTokenId()).isNull();
        assertThat(m2.twapOpen()).isNaN();
        assertThat(m2.twapClose()).isNaN();
        assertThat(m2.actualOutcome()).isNull();
    }

    @Test
    @DisplayName("Should lazily load market metadata when processing market and skip markets with missing data")
    void shouldLazyLoadMarketMetadataAndSkipWhenDataMissing(@TempDir Path tempDir) throws Exception {
        CollectorParquetDatasetService service = new CollectorParquetDatasetService(tempDir.toString());

        long c1Start = 1785542400L;        // 2026-08-01 00:00:00 UTC (valid UP)
        long c2Start = c1Start + 900L;     // 2026-08-01 00:15:00 UTC (valid DOWN)
        long c3Start = c2Start + 900L;     // 2026-08-01 00:30:00 UTC (missing clobTokenIds)
        long c4Start = c3Start + 900L;     // 2026-08-01 00:45:00 UTC (missing twapClose and market_resolved)
        long c5Start = c4Start + 900L;     // 2026-08-01 01:00:00 UTC (missing twapOpen)

        Path pmDir = service.getPolymarket15mDir("BTCUSDT");
        Path twapDir = service.getPolymarketCryptoTwap60sDir("BTCUSDT");
        Files.createDirectories(pmDir);
        Files.createDirectories(twapDir);

        Path pmFile1 = pmDir.resolve("btc-updown-15m-" + c1Start + ".parquet");
        Path pmFile2 = pmDir.resolve("btc-updown-15m-" + c2Start + ".parquet");
        Path pmFile3 = pmDir.resolve("btc-updown-15m-" + c3Start + ".parquet");
        Path pmFile4 = pmDir.resolve("btc-updown-15m-" + c4Start + ".parquet");
        Path pmFile5 = pmDir.resolve("btc-updown-15m-" + c5Start + ".parquet");
        Path twapFile = service.getPolymarketCryptoTwap60sFile("BTCUSDT", "2026-08-01");

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement()) {

            String normPm1 = pmFile1.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + (c1Start * 1000L - 5000L) + "::UBIGINT, 'market_lifecycle'::VARCHAR, 'market_discovered'::VARCHAR, " +
                    "'0x111'::VARCHAR, NULL::VARCHAR, NULL::DOUBLE, NULL::DOUBLE, NULL::VARCHAR, " +
                    "'{\"slug\":\"btc-updown-15m-" + c1Start + "\",\"clobTokenIds\":\"[\\\"10001\\\", \\\"10002\\\"]\"}'::JSON)" +
                    ") AS t(ts, msg_type, event_type, market, asset_id, price, size, side, payload)) " +
                    "TO '" + normPm1 + "' (FORMAT PARQUET)");

            String normPm2 = pmFile2.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + (c2Start * 1000L - 5000L) + "::UBIGINT, 'market_lifecycle'::VARCHAR, 'market_discovered'::VARCHAR, " +
                    "'0x222'::VARCHAR, NULL::VARCHAR, NULL::DOUBLE, NULL::DOUBLE, NULL::VARCHAR, " +
                    "'{\"slug\":\"btc-updown-15m-" + c2Start + "\",\"clobTokenIds\":\"[\\\"20001\\\", \\\"20002\\\"]\"}'::JSON)" +
                    ") AS t(ts, msg_type, event_type, market, asset_id, price, size, side, payload)) " +
                    "TO '" + normPm2 + "' (FORMAT PARQUET)");

            // c3: missing market_discovered token IDs
            String normPm3 = pmFile3.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + (c3Start * 1000L + 1000L) + "::UBIGINT, 'clob_ws'::VARCHAR, 'book'::VARCHAR, " +
                    "'0x333'::VARCHAR, '30001'::VARCHAR, NULL::DOUBLE, NULL::DOUBLE, NULL::VARCHAR, " +
                    "'{\"asset_id\":\"30001\",\"bids\":[],\"asks\":[]}'::JSON)" +
                    ") AS t(ts, msg_type, event_type, market, asset_id, price, size, side, payload)) " +
                    "TO '" + normPm3 + "' (FORMAT PARQUET)");

            // c4: has market_discovered and twapOpen (at c4Start), but no twapClose (at c4Start + 900) and no market_resolved
            String normPm4 = pmFile4.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + (c4Start * 1000L - 5000L) + "::UBIGINT, 'market_lifecycle'::VARCHAR, 'market_discovered'::VARCHAR, " +
                    "'0x444'::VARCHAR, NULL::VARCHAR, NULL::DOUBLE, NULL::DOUBLE, NULL::VARCHAR, " +
                    "'{\"slug\":\"btc-updown-15m-" + c4Start + "\",\"clobTokenIds\":\"[\\\"40001\\\", \\\"40002\\\"]\"}'::JSON)" +
                    ") AS t(ts, msg_type, event_type, market, asset_id, price, size, side, payload)) " +
                    "TO '" + normPm4 + "' (FORMAT PARQUET)");

            // c5: has market_discovered, but no twapOpen at c5Start
            String normPm5 = pmFile5.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + (c5Start * 1000L - 5000L) + "::UBIGINT, 'market_lifecycle'::VARCHAR, 'market_discovered'::VARCHAR, " +
                    "'0x555'::VARCHAR, NULL::VARCHAR, NULL::DOUBLE, NULL::DOUBLE, NULL::VARCHAR, " +
                    "'{\"slug\":\"btc-updown-15m-" + c5Start + "\",\"clobTokenIds\":\"[\\\"50001\\\", \\\"50002\\\"]\"}'::JSON)" +
                    ") AS t(ts, msg_type, event_type, market, asset_id, price, size, side, payload)) " +
                    "TO '" + normPm5 + "' (FORMAT PARQUET)");

            String normTwap = twapFile.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + (c1Start * 1000L) + "::UBIGINT, 1::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60000.0::DOUBLE, '60000.0'::VARCHAR, 60::INT, " + (c1Start * 1000L) + "::UBIGINT), " +
                    "(" + (c2Start * 1000L) + "::UBIGINT, 2::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60120.0::DOUBLE, '60120.0'::VARCHAR, 60::INT, " + (c2Start * 1000L) + "::UBIGINT), " +
                    "(" + (c3Start * 1000L) + "::UBIGINT, 3::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 59950.0::DOUBLE, '59950.0'::VARCHAR, 60::INT, " + (c3Start * 1000L) + "::UBIGINT), " +
                    "(" + (c4Start * 1000L) + "::UBIGINT, 4::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60010.0::DOUBLE, '60010.0'::VARCHAR, 60::INT, " + (c4Start * 1000L) + "::UBIGINT)" +
                    ") AS t(ts, seq, symbol, source, twap_price, full_accuracy_twap_price, window_seconds, twap_timestamp)) " +
                    "TO '" + normTwap + "' (FORMAT PARQUET)");
        }

        List<CollectorMarketMetadata> discovered = service.discoverMarkets("BTCUSDT", "2026-08-01", "2026-08-01");
        assertThat(discovered).hasSize(5);

        BacktestEvent[] sharedDailyEvents = ChronologicalEventStreamer.loadSharedDailyStreams(
                service, "BTCUSDT", "2026-08-01", Set.of());

        Optional<CollectorMarketMetadata> loaded1 = service.loadMarketMetadata(discovered.get(0), sharedDailyEvents);
        assertThat(loaded1).isPresent();
        CollectorMarketMetadata m1 = loaded1.get();
        assertThat(m1.tStart()).isEqualTo(c1Start);
        assertThat(m1.tEnd()).isEqualTo(c2Start);
        assertThat(m1.upTokenId()).isEqualTo("10001");
        assertThat(m1.downTokenId()).isEqualTo("10002");
        assertThat(m1.twapOpen()).isCloseTo(60000.0, within(0.01));
        assertThat(m1.twapClose()).isCloseTo(60120.0, within(0.01));
        assertThat(m1.actualOutcome()).isEqualTo(TradeDirection.UP);

        Optional<CollectorMarketMetadata> loaded2 = service.loadMarketMetadata(discovered.get(1), sharedDailyEvents);
        assertThat(loaded2).isPresent();
        CollectorMarketMetadata m2 = loaded2.get();
        assertThat(m2.tStart()).isEqualTo(c2Start);
        assertThat(m2.upTokenId()).isEqualTo("20001");
        assertThat(m2.downTokenId()).isEqualTo("20002");
        assertThat(m2.twapOpen()).isCloseTo(60120.0, within(0.01));
        assertThat(m2.twapClose()).isCloseTo(59950.0, within(0.01));
        assertThat(m2.actualOutcome()).isEqualTo(TradeDirection.DOWN);

        // Missing clobTokenIds -> skipped (empty)
        assertThat(service.loadMarketMetadata(discovered.get(2), sharedDailyEvents)).isEmpty();
        // Missing twapClose and market_resolved -> skipped (empty)
        assertThat(service.loadMarketMetadata(discovered.get(3), sharedDailyEvents)).isEmpty();
        // Missing twapOpen -> skipped (empty)
        assertThat(service.loadMarketMetadata(discovered.get(4), sharedDailyEvents)).isEmpty();
    }
}
