package cz.polymarket.bot.backtest.service;

import cz.polymarket.bot.backtest.data.CollectorMarketMetadata;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class CollectorParquetDatasetServiceTest {

    @Test
    @DisplayName("Should discover collector 15m market files, extract token IDs, and resolve TWAP open/close outcomes")
    void shouldDiscoverCollectorMarketsAndResolveMetadata(@TempDir Path tempDir) throws Exception {
        CollectorParquetDatasetService service = new CollectorParquetDatasetService(tempDir.toString());

        long c1Start = 1785542400L; // 2026-08-01 00:00:00 UTC
        long c2Start = c1Start + 900L; // 2026-08-01 00:15:00 UTC

        Path pmDir = service.getPolymarket15mDir("BTCUSDT");
        Path twapDir = service.getPolymarketCryptoTwap60sDir("BTCUSDT");
        Files.createDirectories(pmDir);
        Files.createDirectories(twapDir);

        Path pmFile1 = pmDir.resolve("btc-updown-15m-" + c1Start + ".parquet");
        Path pmFile2 = pmDir.resolve("btc-updown-15m-" + c2Start + ".parquet");
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

            String normTwap = twapFile.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + (c1Start * 1000L) + "::UBIGINT, 1::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60000.0::DOUBLE, '60000.0'::VARCHAR, 60::INT, " + (c1Start * 1000L) + "::UBIGINT), " +
                    "(" + (c2Start * 1000L) + "::UBIGINT, 2::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60120.0::DOUBLE, '60120.0'::VARCHAR, 60::INT, " + (c2Start * 1000L) + "::UBIGINT), " +
                    "(" + ((c2Start + 900L) * 1000L) + "::UBIGINT, 3::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 59950.0::DOUBLE, '59950.0'::VARCHAR, 60::INT, " + ((c2Start + 900L) * 1000L) + "::UBIGINT)" +
                    ") AS t(ts, seq, symbol, source, twap_price, full_accuracy_twap_price, window_seconds, twap_timestamp)) " +
                    "TO '" + normTwap + "' (FORMAT PARQUET)");
        }

        List<CollectorMarketMetadata> markets = service.discoverMarkets("BTCUSDT", "2026-08-01", "2026-08-01");
        assertThat(markets).hasSize(2);

        CollectorMarketMetadata m1 = markets.get(0);
        assertThat(m1.tStart()).isEqualTo(c1Start);
        assertThat(m1.tEnd()).isEqualTo(c2Start);
        assertThat(m1.upTokenId()).isEqualTo("10001");
        assertThat(m1.downTokenId()).isEqualTo("10002");
        assertThat(m1.twapOpen()).isCloseTo(60000.0, within(0.01));
        assertThat(m1.twapClose()).isCloseTo(60120.0, within(0.01));
        assertThat(m1.actualOutcome()).isEqualTo(TradeDirection.UP);

        CollectorMarketMetadata m2 = markets.get(1);
        assertThat(m2.tStart()).isEqualTo(c2Start);
        assertThat(m2.upTokenId()).isEqualTo("20001");
        assertThat(m2.downTokenId()).isEqualTo("20002");
        assertThat(m2.twapOpen()).isCloseTo(60120.0, within(0.01));
        assertThat(m2.twapClose()).isCloseTo(59950.0, within(0.01));
        assertThat(m2.actualOutcome()).isEqualTo(TradeDirection.DOWN);
    }
}
