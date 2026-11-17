package cz.polymarket.bot.data.storage;

import cz.polymarket.bot.connector.binance.BinanceStreamType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link DailyBinanceStorageManager} verifying separate per-stream daily files for
 * Binance Spot and USD-M Futures {@code bookTicker} streams, automatic UTC midnight file rotation,
 * and fail-fast rejection of unsupported {@code aggTrades} stream types.
 */
class DailyBinanceStorageManagerTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldWriteSpotAndFuturesBookTickerStreamsToSeparateDailyFilesAndRotateAtUtcMidnight() throws Exception {
        DailyBinanceStorageManager manager = new DailyBinanceStorageManager(tempDir.toString(), "BTCUSDT");

        long day1Ts = Instant.parse("2026-10-10T23:59:59.500Z").toEpochMilli();
        long day2Ts = Instant.parse("2026-10-11T00:00:00.100Z").toEpochMilli();

        manager.recordBytes(
                BinanceStreamType.SPOT_BOOK_TICKER,
                "{\"u\":400900217,\"s\":\"BTCUSDT\",\"b\":\"78099.90\",\"B\":\"1.20\",\"a\":\"78100.10\",\"A\":\"0.85\"}".getBytes(StandardCharsets.UTF_8),
                day1Ts
        );
        manager.record(
                BinanceStreamType.FUTURES_BOOK_TICKER,
                "{\"e\":\"bookTicker\",\"u\":400900218,\"s\":\"BTCUSDT\",\"b\":\"78119.50\",\"B\":\"2.50\",\"a\":\"78120.00\",\"A\":\"1.75\",\"T\":1789041600115,\"E\":1789041600116}",
                day1Ts
        );

        // Next day on Spot bookTicker -> should rotate to 2026-10-11 file using direct byte[] recordBytes
        manager.recordBytes(
                BinanceStreamType.SPOT_BOOK_TICKER,
                "{\"u\":400900219,\"s\":\"BTCUSDT\",\"b\":\"78200.00\",\"B\":\"1.10\",\"a\":\"78200.20\",\"A\":\"0.90\"}".getBytes(StandardCharsets.UTF_8),
                day2Ts
        );

        manager.closeAll();

        Path spotBookTickerDay1 = tempDir.resolve("binance/spot/BTCUSDT/bookTicker/BTCUSDT-bookTicker-2026-10-10.jsonl.zst");
        Path spotBookTickerDay2 = tempDir.resolve("binance/spot/BTCUSDT/bookTicker/BTCUSDT-bookTicker-2026-10-11.jsonl.zst");
        Path futBookTickerDay1 = tempDir.resolve("binance/futures/BTCUSDT/bookTicker/BTCUSDT-bookTicker-2026-10-10.jsonl.zst");

        assertThat(Files.exists(spotBookTickerDay1)).isTrue();
        assertThat(Files.exists(spotBookTickerDay2)).isTrue();
        assertThat(Files.exists(futBookTickerDay1)).isTrue();

        List<String> spotDay1Lines = ZstdJsonlWriterTest.readAllZstdLines(spotBookTickerDay1);
        assertThat(spotDay1Lines).hasSize(1);
        assertThat(spotDay1Lines.get(0))
                .startsWith("{\"ts\":" + day1Ts + ",\"payload\":")
                .contains("\"u\":400900217")
                .contains("\"b\":\"78099.90\"");

        List<String> spotDay2Lines = ZstdJsonlWriterTest.readAllZstdLines(spotBookTickerDay2);
        assertThat(spotDay2Lines).hasSize(1);
        assertThat(spotDay2Lines.get(0))
                .startsWith("{\"ts\":" + day2Ts + ",\"payload\":")
                .contains("\"u\":400900219")
                .contains("\"b\":\"78200.00\"");

        List<String> futBookTickerLines = ZstdJsonlWriterTest.readAllZstdLines(futBookTickerDay1);
        assertThat(futBookTickerLines).hasSize(1);
        assertThat(futBookTickerLines.get(0))
                .startsWith("{\"ts\":" + day1Ts + ",\"payload\":")
                .contains("\"e\":\"bookTicker\"")
                .contains("\"b\":\"78119.50\"");
    }

    @Test
    void shouldRejectUnsupportedAggTradesStreams() {
        DailyBinanceStorageManager manager = new DailyBinanceStorageManager(tempDir.toString(), "BTCUSDT");
        long ts = Instant.parse("2026-10-10T12:00:00Z").toEpochMilli();

        assertThatThrownBy(() -> manager.record(BinanceStreamType.SPOT_AGG_TRADES, "{\"p\":\"78000\"}", ts))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SPOT_AGG_TRADES");
        assertThatThrownBy(() -> manager.recordBytes(BinanceStreamType.FUTURES_AGG_TRADES, "{\"p\":\"78000\"}".getBytes(StandardCharsets.UTF_8), ts))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("FUTURES_AGG_TRADES");
    }
}
