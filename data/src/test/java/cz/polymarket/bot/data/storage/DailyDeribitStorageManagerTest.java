package cz.polymarket.bot.data.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link DailyDeribitStorageManager} verifying daily file path resolution,
 * Zstandard record writing, and automatic UTC midnight rotation.
 */
class DailyDeribitStorageManagerTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldWriteDeribitTradesToDailyFileAndRotateAtUtcMidnight() throws Exception {
        DailyDeribitStorageManager manager = new DailyDeribitStorageManager(tempDir.toString(), "BTCUSDT");

        assertThat(manager.getInstrument()).isEqualTo("BTC-PERPETUAL");

        long day1Ts = Instant.parse("2026-10-10T23:59:59.500Z").toEpochMilli();
        long day2Ts = Instant.parse("2026-10-11T00:00:00.100Z").toEpochMilli();

        String tradePayload1 = "{\"jsonrpc\":\"2.0\",\"method\":\"subscription\",\"params\":{\"channel\":\"trades.BTC-PERPETUAL.raw\",\"data\":[{\"price\":78100.0,\"amount\":10.0}]}}";
        String tradePayload2 = "{\"jsonrpc\":\"2.0\",\"method\":\"subscription\",\"params\":{\"channel\":\"trades.BTC-PERPETUAL.raw\",\"data\":[{\"price\":78200.0,\"amount\":20.0}]}}";

        manager.record(tradePayload1, day1Ts);
        manager.record(tradePayload2, day2Ts);

        manager.closeAll();

        Path day1File = tempDir.resolve("deribit/futures/BTC-PERPETUAL/trades/BTC-PERPETUAL-trades-2026-10-10.jsonl.zst");
        Path day2File = tempDir.resolve("deribit/futures/BTC-PERPETUAL/trades/BTC-PERPETUAL-trades-2026-10-11.jsonl.zst");

        assertThat(Files.exists(day1File)).isTrue();
        assertThat(Files.exists(day2File)).isTrue();

        List<String> day1Lines = ZstdJsonlWriterTest.readAllZstdLines(day1File);
        assertThat(day1Lines).hasSize(1);
        assertThat(day1Lines.get(0)).contains("\"ts\":1791676799500");
        assertThat(day1Lines.get(0)).contains("78100.0");

        List<String> day2Lines = ZstdJsonlWriterTest.readAllZstdLines(day2File);
        assertThat(day2Lines).hasSize(1);
        assertThat(day2Lines.get(0)).contains("\"ts\":1791676800100");
        assertThat(day2Lines.get(0)).contains("78200.0");

        assertThat(manager.getTotalRecordsWritten()).isEqualTo(2L);
    }
}
