package cz.polymarket.bot.data.storage;

import cz.polymarket.bot.connector.binance.BinanceStreamType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link DailyBinanceStorageManager} verifying separate per-stream daily files
 * and automatic UTC midnight file rotation.
 */
class DailyBinanceStorageManagerTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldWriteEachBinanceStreamToSeparateDailyFileAndRotateAtUtcMidnight() throws Exception {
        DailyBinanceStorageManager manager = new DailyBinanceStorageManager(tempDir.toString(), "BTCUSDT");

        long day1Ts = Instant.parse("2026-10-10T23:59:59.500Z").toEpochMilli();
        long day2Ts = Instant.parse("2026-10-11T00:00:00.100Z").toEpochMilli();

        manager.record(BinanceStreamType.SPOT_AGG_TRADES, "{\"e\":\"aggTrade\",\"s\":\"BTCUSDT\",\"p\":\"78100\"}", day1Ts);
        manager.recordBytes(
                BinanceStreamType.SPOT_BOOK_TICKER,
                "{\"u\":400900217,\"s\":\"BTCUSDT\",\"b\":\"78099.90\",\"B\":\"1.20\",\"a\":\"78100.10\",\"A\":\"0.85\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                day1Ts
        );
        manager.record(BinanceStreamType.FUTURES_AGG_TRADES, "{\"e\":\"aggTrade\",\"s\":\"BTCUSDT\",\"p\":\"78120\"}", day1Ts);
        manager.recordBytes(
                BinanceStreamType.FUTURES_BOOK_TICKER,
                "{\"e\":\"bookTicker\",\"u\":400900218,\"s\":\"BTCUSDT\",\"b\":\"78119.50\",\"B\":\"2.50\",\"a\":\"78120.00\",\"A\":\"1.75\",\"T\":1789041600115,\"E\":1789041600116}".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                day1Ts
        );

        // Next day on Spot aggTrades -> should rotate to 2026-10-11 file using direct byte[] recordBytes
        manager.recordBytes(
                BinanceStreamType.SPOT_AGG_TRADES,
                "{\"e\":\"aggTrade\",\"s\":\"BTCUSDT\",\"p\":\"78200\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                day2Ts
        );

        manager.closeAll();

        Path spotDay1 = tempDir.resolve("binance/spot/BTCUSDT/aggTrades/BTCUSDT-aggTrades-2026-10-10.jsonl.zst");
        Path spotDay2 = tempDir.resolve("binance/spot/BTCUSDT/aggTrades/BTCUSDT-aggTrades-2026-10-11.jsonl.zst");
        Path spotBookTickerDay1 = tempDir.resolve("binance/spot/BTCUSDT/bookTicker/BTCUSDT-bookTicker-2026-10-10.jsonl.zst");
        Path futAggDay1 = tempDir.resolve("binance/futures/BTCUSDT/aggTrades/BTCUSDT-aggTrades-2026-10-10.jsonl.zst");
        Path futBookTickerDay1 = tempDir.resolve("binance/futures/BTCUSDT/bookTicker/BTCUSDT-bookTicker-2026-10-10.jsonl.zst");

        assertThat(Files.exists(spotDay1)).isTrue();
        assertThat(Files.exists(spotDay2)).isTrue();
        assertThat(Files.exists(spotBookTickerDay1)).isTrue();
        assertThat(Files.exists(futAggDay1)).isTrue();
        assertThat(Files.exists(futBookTickerDay1)).isTrue();

        List<String> spotBookTickerLines = ZstdJsonlWriterTest.readAllZstdLines(spotBookTickerDay1);
        assertThat(spotBookTickerLines).hasSize(1);
        assertThat(spotBookTickerLines.get(0))
                .startsWith("{\"ts\":" + day1Ts + ",\"payload\":")
                .contains("\"u\":400900217")
                .contains("\"b\":\"78099.90\"");

        List<String> futBookTickerLines = ZstdJsonlWriterTest.readAllZstdLines(futBookTickerDay1);
        assertThat(futBookTickerLines).hasSize(1);
        assertThat(futBookTickerLines.get(0))
                .startsWith("{\"ts\":" + day1Ts + ",\"payload\":")
                .contains("\"e\":\"bookTicker\"")
                .contains("\"b\":\"78119.50\"");

        List<String> spotDay1Lines = ZstdJsonlWriterTest.readAllZstdLines(spotDay1);
        assertThat(spotDay1Lines).hasSize(1);
        assertThat(spotDay1Lines.get(0)).contains("78100");

        List<String> spotDay2Lines = ZstdJsonlWriterTest.readAllZstdLines(spotDay2);
        assertThat(spotDay2Lines).hasSize(1);
        assertThat(spotDay2Lines.get(0)).contains("78200");
    }
}
