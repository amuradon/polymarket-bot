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
        manager.record(BinanceStreamType.FUTURES_AGG_TRADES, "{\"e\":\"aggTrade\",\"s\":\"BTCUSDT\",\"p\":\"78120\"}", day1Ts);
        manager.record(BinanceStreamType.FUTURES_DIFF_DEPTH_100MS, "{\"e\":\"depthUpdate\",\"b\":[],\"a\":[]}", day1Ts);
        manager.record(BinanceStreamType.FUTURES_PARTIAL_DEPTH_100MS, "{\"e\":\"depthUpdate\",\"levels\":20}", day1Ts);
        manager.record(BinanceStreamType.FUTURES_FORCE_ORDER, "{\"e\":\"forceOrder\",\"o\":{\"s\":\"BTCUSDT\"}}", day1Ts);

        // Next day on Spot aggTrades -> should rotate to 2026-10-11 file
        manager.record(BinanceStreamType.SPOT_AGG_TRADES, "{\"e\":\"aggTrade\",\"s\":\"BTCUSDT\",\"p\":\"78200\"}", day2Ts);

        manager.closeAll();

        Path spotDay1 = tempDir.resolve("binance/spot/BTCUSDT/aggTrades/BTCUSDT-aggTrades-2026-10-10.jsonl.zst");
        Path spotDay2 = tempDir.resolve("binance/spot/BTCUSDT/aggTrades/BTCUSDT-aggTrades-2026-10-11.jsonl.zst");
        Path futAggDay1 = tempDir.resolve("binance/futures/BTCUSDT/aggTrades/BTCUSDT-aggTrades-2026-10-10.jsonl.zst");
        Path futDiffDay1 = tempDir.resolve("binance/futures/BTCUSDT/depthDiff/BTCUSDT-depthDiff-2026-10-10.jsonl.zst");
        Path futPartialDay1 = tempDir.resolve("binance/futures/BTCUSDT/depthPartial/BTCUSDT-depthPartial-2026-10-10.jsonl.zst");
        Path futForceDay1 = tempDir.resolve("binance/futures/BTCUSDT/forceOrder/BTCUSDT-forceOrder-2026-10-10.jsonl.zst");

        assertThat(Files.exists(spotDay1)).isTrue();
        assertThat(Files.exists(spotDay2)).isTrue();
        assertThat(Files.exists(futAggDay1)).isTrue();
        assertThat(Files.exists(futDiffDay1)).isTrue();
        assertThat(Files.exists(futPartialDay1)).isTrue();
        assertThat(Files.exists(futForceDay1)).isTrue();

        List<String> spotDay1Lines = ZstdJsonlWriterTest.readAllZstdLines(spotDay1);
        assertThat(spotDay1Lines).hasSize(1);
        assertThat(spotDay1Lines.get(0)).contains("78100");

        List<String> spotDay2Lines = ZstdJsonlWriterTest.readAllZstdLines(spotDay2);
        assertThat(spotDay2Lines).hasSize(1);
        assertThat(spotDay2Lines.get(0)).contains("78200");
    }
}
