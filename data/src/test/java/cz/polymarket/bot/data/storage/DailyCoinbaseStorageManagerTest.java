package cz.polymarket.bot.data.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link DailyCoinbaseStorageManager} verifying daily {@code .jsonl.zst} file path
 * resolution, zero-decode {@code byte[]} writing, and automatic UTC midnight rotation.
 */
class DailyCoinbaseStorageManagerTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldWriteCoinbaseSpotTradesToDailyFileAndRotateAtUtcMidnight() throws Exception {
        DailyCoinbaseStorageManager manager = new DailyCoinbaseStorageManager(tempDir.toString(), "BTCUSDT");

        assertThat(manager.getSymbol()).isEqualTo("BTCUSD");

        long day1Ts = Instant.parse("2026-10-10T23:59:59.500Z").toEpochMilli();
        long day2Ts = Instant.parse("2026-10-11T00:00:00.100Z").toEpochMilli();

        String matchDay1 = "{\"type\":\"match\",\"trade_id\":1120371409,\"side\":\"buy\",\"size\":\"0.00012577\",\"price\":\"71369.9\",\"product_id\":\"BTC-USD\",\"time\":\"2026-10-10T23:59:59.450000Z\"}";
        String matchDay2 = "{\"type\":\"match\",\"trade_id\":1120371410,\"side\":\"sell\",\"size\":\"0.50000000\",\"price\":\"71380.0\",\"product_id\":\"BTC-USD\",\"time\":\"2026-10-11T00:00:00.050000Z\"}";

        manager.record("BTCUSD", matchDay1, day1Ts);
        manager.recordBytes("BTCUSD", matchDay2.getBytes(StandardCharsets.UTF_8), day2Ts);

        manager.flushAll();
        manager.closeAll();

        Path day1File = tempDir.resolve("coinbase/spot/BTCUSD/trades/BTCUSD-trades-2026-10-10.jsonl.zst");
        Path day2File = tempDir.resolve("coinbase/spot/BTCUSD/trades/BTCUSD-trades-2026-10-11.jsonl.zst");

        assertThat(Files.exists(day1File)).isTrue();
        assertThat(Files.exists(day2File)).isTrue();

        List<String> day1Lines = ZstdJsonlWriterTest.readAllZstdLines(day1File);
        assertThat(day1Lines).hasSize(1);
        assertThat(day1Lines.getFirst()).contains("\"ts\":1791676799500").contains("\"trade_id\":1120371409");

        List<String> day2Lines = ZstdJsonlWriterTest.readAllZstdLines(day2File);
        assertThat(day2Lines).hasSize(1);
        assertThat(day2Lines.getFirst()).contains("\"ts\":1791676800100").contains("\"trade_id\":1120371410");

        assertThat(manager.getTotalRecordsWritten()).isEqualTo(2L);
    }
}
