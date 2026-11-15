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
 * Unit tests for {@link DailyKrakenStorageManager} verifying separate daily {@code .jsonl.zst} files
 * for {@code USDTUSD} and the configured symbol (e.g. {@code BTCUSD}), zero-decode {@code byte[]} writing,
 * and automatic UTC midnight rotation.
 */
class DailyKrakenStorageManagerTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldWriteKrakenSpotStreamsToSeparateDailyFilesAndRotateAtUtcMidnight() throws Exception {
        DailyKrakenStorageManager manager = new DailyKrakenStorageManager(tempDir.toString(), "BTCUSDT");

        assertThat(manager.getPrimarySymbol()).isEqualTo("BTCUSD");

        long day1Ts = Instant.parse("2026-10-10T23:59:59.500Z").toEpochMilli();
        long day2Ts = Instant.parse("2026-10-11T00:00:00.100Z").toEpochMilli();

        String usdtDay1Json = "{\"channel\":\"trade\",\"type\":\"update\",\"data\":[{\"symbol\":\"USDT/USD\",\"side\":\"sell\",\"price\":0.99879,\"qty\":99.0099,\"ord_type\":\"limit\",\"trade_id\":85674723,\"timestamp\":\"2026-10-10T23:59:59.450000Z\"}]}";
        String btcDay1Json = "{\"channel\":\"trade\",\"type\":\"update\",\"data\":[{\"symbol\":\"BTC/USD\",\"side\":\"buy\",\"price\":71370.7,\"qty\":0.00013941,\"ord_type\":\"limit\",\"trade_id\":115529595,\"timestamp\":\"2026-10-10T23:59:59.480000Z\"}]}";
        String btcDay2Json = "{\"channel\":\"trade\",\"type\":\"update\",\"data\":[{\"symbol\":\"BTC/USD\",\"side\":\"buy\",\"price\":71420.0,\"qty\":0.25,\"ord_type\":\"market\",\"trade_id\":115529596,\"timestamp\":\"2026-10-11T00:00:00.050000Z\"}]}";

        manager.record("USDTUSD", usdtDay1Json, day1Ts);
        manager.record("BTCUSD", btcDay1Json, day1Ts);
        manager.recordBytes("BTCUSD", btcDay2Json.getBytes(StandardCharsets.UTF_8), day2Ts);

        manager.flushAll();
        manager.closeAll();

        Path usdtDay1File = tempDir.resolve("kraken/spot/USDTUSD/trades/USDTUSD-trades-2026-10-10.jsonl.zst");
        Path btcDay1File = tempDir.resolve("kraken/spot/BTCUSD/trades/BTCUSD-trades-2026-10-10.jsonl.zst");
        Path btcDay2File = tempDir.resolve("kraken/spot/BTCUSD/trades/BTCUSD-trades-2026-10-11.jsonl.zst");

        assertThat(Files.exists(usdtDay1File)).isTrue();
        assertThat(Files.exists(btcDay1File)).isTrue();
        assertThat(Files.exists(btcDay2File)).isTrue();

        List<String> usdtLines = ZstdJsonlWriterTest.readAllZstdLines(usdtDay1File);
        assertThat(usdtLines).hasSize(1);
        assertThat(usdtLines.getFirst()).contains("\"ts\":1791676799500").contains("\"trade_id\":85674723");

        List<String> btcDay1Lines = ZstdJsonlWriterTest.readAllZstdLines(btcDay1File);
        assertThat(btcDay1Lines).hasSize(1);
        assertThat(btcDay1Lines.getFirst()).contains("\"ts\":1791676799500").contains("\"trade_id\":115529595");

        List<String> btcDay2Lines = ZstdJsonlWriterTest.readAllZstdLines(btcDay2File);
        assertThat(btcDay2Lines).hasSize(1);
        assertThat(btcDay2Lines.getFirst()).contains("\"ts\":1791676800100").contains("\"trade_id\":115529596");

        assertThat(manager.getTotalRecordsWritten()).isEqualTo(3L);
    }
}
