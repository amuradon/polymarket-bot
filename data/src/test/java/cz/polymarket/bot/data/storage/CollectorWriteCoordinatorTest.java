package cz.polymarket.bot.data.storage;

import com.github.luben.zstd.ZstdInputStream;
import cz.polymarket.bot.connector.binance.BinanceStreamType;
import cz.polymarket.bot.data.web.CutoverActivateResponseDto;
import cz.polymarket.bot.data.web.CutoverFreezeResponseDto;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link CollectorWriteCoordinator} validating STANDBY memory buffering,
 * retention window pruning, and zero-loss / zero-duplicate Blue-Green cutover across
 * shared {@code .jsonl.zst} files.
 */
class CollectorWriteCoordinatorTest {

    private static final long BASE_TS_MS = 1794828600000L; // 2026-11-16T11:30:00Z
    private static final LocalDate BASE_UTC_DATE = LocalDate.of(2026, 11, 16);

    @TempDir
    Path tempDir;

    @Test
    void shouldBufferEventsInStandbyWithoutCreatingFilesAndPruneExpiredEntries() throws Exception {
        CollectorWriteCoordinator coordinator = createCoordinator(tempDir, "STANDBY", 5_000L);

        assertThat(coordinator.getWriteMode()).isEqualTo(CollectorWriteMode.STANDBY);

        coordinator.recordBinanceBytes(
                BinanceStreamType.SPOT_AGG_TRADES,
                "{\"a\":1,\"p\":\"90000.0\"}".getBytes(StandardCharsets.UTF_8),
                BASE_TS_MS
        );
        coordinator.recordKrakenBytes(
                "BTCUSD",
                "{\"symbol\":\"BTC/USD\",\"price\":90001.0}".getBytes(StandardCharsets.UTF_8),
                BASE_TS_MS + 1_000L
        );
        coordinator.recordCoinbaseBytes(
                "BTCUSD",
                "{\"product_id\":\"BTC-USD\",\"price\":\"90002.0\"}".getBytes(StandardCharsets.UTF_8),
                BASE_TS_MS + 2_000L
        );

        // Flush in STANDBY must be a no-op on disk
        coordinator.flushAllIfActive();
        assertThat(listZstFiles(tempDir)).isEmpty();
        assertThat(coordinator.getStandbyBufferedRecords()).isEqualTo(3L);

        // Recording an event at BASE_TS_MS + 6_500L (with 5_000ms retention) should prune events < BASE_TS_MS + 1_500L
        coordinator.recordPolyBoltCryptoPriceBytes(
                "{\"channel\":\"price.crypto\",\"price\":90003.0}".getBytes(StandardCharsets.UTF_8),
                BASE_TS_MS + 6_500L
        );

        // BASE_TS_MS and BASE_TS_MS + 1_000L were pruned; BASE_TS_MS + 2_000L and BASE_TS_MS + 6_500L remain
        assertThat(coordinator.getStandbyBufferedRecords()).isEqualTo(2L);
        assertThat(listZstFiles(tempDir)).isEmpty();
    }

    @Test
    void shouldPerformSeamlessBlueGreenCutoverWithZeroLossAndZeroDuplicates() throws Exception {
        CollectorWriteCoordinator blue = createCoordinator(tempDir, "ACTIVE", 30_000L);
        CollectorWriteCoordinator green = createCoordinator(tempDir, "STANDBY", 30_000L);

        String slugExisting = "btc-updown-15m-1794828600";
        String slugNewInGreen = "btc-updown-15m-1794829500";

        // 1. Blue discovers existing market and records events up to t = BASE_TS_MS + 200
        blue.recordPolymarketEvent(slugExisting, BASE_TS_MS, "market_discovered", "{\"slug\":\"" + slugExisting + "\"}");
        blue.recordBinanceBytes(
                BinanceStreamType.SPOT_AGG_TRADES,
                "{\"a\":101}".getBytes(StandardCharsets.UTF_8),
                BASE_TS_MS + 100L
        );
        blue.recordBinanceBytes(
                BinanceStreamType.SPOT_AGG_TRADES,
                "{\"a\":102}".getBytes(StandardCharsets.UTF_8),
                BASE_TS_MS + 200L
        );
        blue.recordPolymarketEventBytes(
                slugExisting,
                BASE_TS_MS + 200L,
                "clob",
                "{\"seq\":1}".getBytes(StandardCharsets.UTF_8)
        );

        // 2. Green is warming up in STANDBY concurrently, receiving overlapping events (100, 200) and newer events (300, 400)
        green.recordPolymarketEvent(slugExisting, BASE_TS_MS + 50L, "market_discovered", "{\"slug\":\"" + slugExisting + "\"}");
        green.recordPolymarketEvent(slugNewInGreen, BASE_TS_MS + 60L, "market_discovered", "{\"slug\":\"" + slugNewInGreen + "\"}");

        green.recordBinanceBytes(
                BinanceStreamType.SPOT_AGG_TRADES,
                "{\"a\":101}".getBytes(StandardCharsets.UTF_8),
                BASE_TS_MS + 100L
        );
        green.recordBinanceBytes(
                BinanceStreamType.SPOT_AGG_TRADES,
                "{\"a\":102}".getBytes(StandardCharsets.UTF_8),
                BASE_TS_MS + 200L
        );
        green.recordBinanceBytes(
                BinanceStreamType.SPOT_AGG_TRADES,
                "{\"a\":103}".getBytes(StandardCharsets.UTF_8),
                BASE_TS_MS + 300L
        );
        green.recordPolymarketEventBytes(
                slugExisting,
                BASE_TS_MS + 200L,
                "clob",
                "{\"seq\":1}".getBytes(StandardCharsets.UTF_8)
        );
        green.recordPolymarketEventBytes(
                slugExisting,
                BASE_TS_MS + 300L,
                "clob",
                "{\"seq\":2}".getBytes(StandardCharsets.UTF_8)
        );

        // 3. Blue freezes at cutoverTimestampMs = BASE_TS_MS + 200L
        long cutoverTs = BASE_TS_MS + 200L;
        CutoverFreezeResponseDto freezeResp = blue.freeze(cutoverTs);
        assertThat(freezeResp.writeMode()).isEqualTo("FROZEN");
        assertThat(freezeResp.cutoverTimestampMs()).isEqualTo(cutoverTs);

        // Late event arriving at Blue after freeze must be ignored
        blue.recordBinanceBytes(
                BinanceStreamType.SPOT_AGG_TRADES,
                "{\"a\":999}".getBytes(StandardCharsets.UTF_8),
                BASE_TS_MS + 250L
        );

        // 4. Green activates after cutoverTimestampMs = BASE_TS_MS + 200L
        CutoverActivateResponseDto activateResp = green.activate(cutoverTs);
        assertThat(activateResp.writeMode()).isEqualTo("ACTIVE");
        assertThat(activateResp.activatedAfterTimestampMs()).isEqualTo(cutoverTs);
        // Replayed: a=103 (ts+300), seq=2 (ts+300), plus market_discovered for slugNewInGreen (since file didn't exist)
        assertThat(activateResp.replayedRecords()).isEqualTo(3L);
        // Dropped duplicates: market_discovered for slugExisting (file already existed from Blue), a=101, a=102, seq=1
        assertThat(activateResp.droppedDuplicateRecords()).isEqualTo(4L);

        // 5. Green continues writing live events in ACTIVE mode
        green.recordBinanceBytes(
                BinanceStreamType.SPOT_AGG_TRADES,
                "{\"a\":104}".getBytes(StandardCharsets.UTF_8),
                BASE_TS_MS + 400L
        );
        green.recordPolymarketEventBytes(
                slugExisting,
                BASE_TS_MS + 400L,
                "clob",
                "{\"seq\":3}".getBytes(StandardCharsets.UTF_8)
        );
        green.closeAll();

        // 6. Verify concatenated Zstandard frames in Binance file contain exact chronological sequence 101, 102, 103, 104
        DailyBinanceStorageManager helperBinance = new DailyBinanceStorageManager(tempDir.toString(), "BTCUSDT");
        Path binanceFile = helperBinance.resolveDailyFilePath(BinanceStreamType.SPOT_AGG_TRADES, BASE_UTC_DATE);
        List<String> binanceLines = readZstLines(binanceFile);
        assertThat(binanceLines).containsExactly(
                "{\"ts\":" + (BASE_TS_MS + 100L) + ",\"payload\":{\"a\":101}}",
                "{\"ts\":" + (BASE_TS_MS + 200L) + ",\"payload\":{\"a\":102}}",
                "{\"ts\":" + (BASE_TS_MS + 300L) + ",\"payload\":{\"a\":103}}",
                "{\"ts\":" + (BASE_TS_MS + 400L) + ",\"payload\":{\"a\":104}}"
        );

        // 7. Verify existing Polymarket market file has 1 market_discovered and seq 1, 2, 3 without duplicates
        PolymarketMarketStorageManager helperPoly = new PolymarketMarketStorageManager(tempDir.toString(), "BTCUSDT", "15m");
        Path existingMarketFile = helperPoly.resolveMarketFilePath(slugExisting);
        List<String> existingMarketLines = readZstLines(existingMarketFile);
        assertThat(existingMarketLines).containsExactly(
                "{\"ts\":" + BASE_TS_MS + ",\"type\":\"market_discovered\",\"payload\":{\"slug\":\"" + slugExisting + "\"}}",
                "{\"ts\":" + (BASE_TS_MS + 200L) + ",\"type\":\"clob\",\"payload\":{\"seq\":1}}",
                "{\"ts\":" + (BASE_TS_MS + 300L) + ",\"type\":\"clob\",\"payload\":{\"seq\":2}}",
                "{\"ts\":" + (BASE_TS_MS + 400L) + ",\"type\":\"clob\",\"payload\":{\"seq\":3}}"
        );

        // 8. Verify brand-new Polymarket market file discovered by Green during STANDBY has its market_discovered written
        Path newMarketFile = helperPoly.resolveMarketFilePath(slugNewInGreen);
        List<String> newMarketLines = readZstLines(newMarketFile);
        assertThat(newMarketLines).containsExactly(
                "{\"ts\":" + (BASE_TS_MS + 60L) + ",\"type\":\"market_discovered\",\"payload\":{\"slug\":\"" + slugNewInGreen + "\"}}"
        );
    }

    private static CollectorWriteCoordinator createCoordinator(Path dir, String initialMode, long retentionMs) {
        String dirStr = dir.toString();
        return new CollectorWriteCoordinator(
                new DailyBinanceStorageManager(dirStr, "BTCUSDT"),
                new DailyKrakenStorageManager(dirStr, "BTCUSDT"),
                new DailyCoinbaseStorageManager(dirStr, "BTCUSDT"),
                new DailyPolyBoltStorageManager(dirStr, "BTCUSDT"),
                new PolymarketMarketStorageManager(dirStr, "BTCUSDT", "15m"),
                initialMode,
                retentionMs
        );
    }

    private static List<Path> listZstFiles(Path root) throws Exception {
        if (!Files.exists(root)) {
            return List.of();
        }
        try (Stream<Path> walk = Files.walk(root)) {
            return walk.filter(p -> p.toString().endsWith(".jsonl.zst")).toList();
        }
    }

    private static List<String> readZstLines(Path zstFile) throws Exception {
        List<String> lines = new ArrayList<>();
        try (ZstdInputStream zis = new ZstdInputStream(Files.newInputStream(zstFile));
             BufferedReader reader = new BufferedReader(new InputStreamReader(zis, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        }
        return lines;
    }
}
