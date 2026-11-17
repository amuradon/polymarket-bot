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
                BinanceStreamType.SPOT_BOOK_TICKER,
                "{\"u\":1,\"b\":\"90000.0\"}".getBytes(StandardCharsets.UTF_8),
                BASE_TS_MS
        );
        coordinator.recordPolyBoltTwapBytes(
                "{\"channel\":\"price.crypto.twap\",\"value\":90001.0}".getBytes(StandardCharsets.UTF_8),
                BASE_TS_MS + 1_000L
        );
        coordinator.recordPolymarketEventBytes(
                "btc-updown-15m-1794828600",
                BASE_TS_MS + 2_000L,
                "clob",
                "{\"seq\":1}".getBytes(StandardCharsets.UTF_8)
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
    void shouldPerformSeamlessBlueGreenCutoverFromFullOldSlotToPolymarketOnlyNewSlotWithZeroLossAndZeroDuplicates() throws Exception {
        CollectorWriteCoordinator blue = createCoordinator(tempDir, "ACTIVE", 30_000L);
        CollectorWriteCoordinator green = createCoordinator(tempDir, "STANDBY", 30_000L);

        String slugExisting = "btc-updown-15m-1794828600";
        String slugNewInGreen = "btc-updown-15m-1794829500";

        // 1. Blue (simulating previous deployment still recording bookTicker + PolyBolt + CLOB) records up to t = BASE_TS_MS + 200
        blue.recordPolymarketEvent(slugExisting, BASE_TS_MS, "market_discovered", "{\"slug\":\"" + slugExisting + "\"}");
        blue.recordBinanceBytes(
                BinanceStreamType.SPOT_BOOK_TICKER,
                "{\"u\":101}".getBytes(StandardCharsets.UTF_8),
                BASE_TS_MS + 100L
        );
        blue.recordPolyBoltCryptoPriceBytes(
                "{\"channel\":\"price.crypto\",\"seq\":1}".getBytes(StandardCharsets.UTF_8),
                BASE_TS_MS + 100L
        );
        blue.recordPolyBoltCryptoPriceBytes(
                "{\"channel\":\"price.crypto\",\"seq\":2}".getBytes(StandardCharsets.UTF_8),
                BASE_TS_MS + 200L
        );
        blue.recordPolyBoltTwapBytes(
                "{\"channel\":\"price.crypto.twap\",\"seq\":10}".getBytes(StandardCharsets.UTF_8),
                BASE_TS_MS + 200L
        );
        blue.recordPolymarketEventBytes(
                slugExisting,
                BASE_TS_MS + 200L,
                "clob",
                "{\"seq\":1}".getBytes(StandardCharsets.UTF_8)
        );

        // 2. Green (new deployment with bookTicker disabled) warms up in STANDBY receiving only PolyBolt + CLOB
        green.recordPolymarketEvent(slugExisting, BASE_TS_MS + 50L, "market_discovered", "{\"slug\":\"" + slugExisting + "\"}");
        green.recordPolymarketEvent(slugNewInGreen, BASE_TS_MS + 60L, "market_discovered", "{\"slug\":\"" + slugNewInGreen + "\"}");

        green.recordPolyBoltCryptoPriceBytes(
                "{\"channel\":\"price.crypto\",\"seq\":1}".getBytes(StandardCharsets.UTF_8),
                BASE_TS_MS + 100L
        );
        green.recordPolyBoltCryptoPriceBytes(
                "{\"channel\":\"price.crypto\",\"seq\":2}".getBytes(StandardCharsets.UTF_8),
                BASE_TS_MS + 200L
        );
        green.recordPolyBoltCryptoPriceBytes(
                "{\"channel\":\"price.crypto\",\"seq\":3}".getBytes(StandardCharsets.UTF_8),
                BASE_TS_MS + 300L
        );
        green.recordPolyBoltTwapBytes(
                "{\"channel\":\"price.crypto.twap\",\"seq\":10}".getBytes(StandardCharsets.UTF_8),
                BASE_TS_MS + 200L
        );
        green.recordPolyBoltTwapBytes(
                "{\"channel\":\"price.crypto.twap\",\"seq\":11}".getBytes(StandardCharsets.UTF_8),
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
        blue.recordPolyBoltCryptoPriceBytes(
                "{\"channel\":\"price.crypto\",\"seq\":999}".getBytes(StandardCharsets.UTF_8),
                BASE_TS_MS + 250L
        );

        // 4. Green activates after cutoverTimestampMs = BASE_TS_MS + 200L
        CutoverActivateResponseDto activateResp = green.activate(cutoverTs);
        assertThat(activateResp.writeMode()).isEqualTo("ACTIVE");
        assertThat(activateResp.activatedAfterTimestampMs()).isEqualTo(cutoverTs);
        // Replayed: price.crypto seq=3 (ts+300), twap seq=11 (ts+300), clob seq=2 (ts+300), plus market_discovered for slugNewInGreen
        assertThat(activateResp.replayedRecords()).isEqualTo(4L);
        // Dropped duplicates: market_discovered for slugExisting, crypto seq=1,2, twap seq=10, clob seq=1
        assertThat(activateResp.droppedDuplicateRecords()).isEqualTo(5L);

        // 5. Green continues writing live PolyBolt + CLOB events in ACTIVE mode
        green.recordPolyBoltCryptoPriceBytes(
                "{\"channel\":\"price.crypto\",\"seq\":4}".getBytes(StandardCharsets.UTF_8),
                BASE_TS_MS + 400L
        );
        green.recordPolymarketEventBytes(
                slugExisting,
                BASE_TS_MS + 400L,
                "clob",
                "{\"seq\":3}".getBytes(StandardCharsets.UTF_8)
        );
        green.closeAll();

        // 6. Verify Blue's Binance bookTicker file was flushed and closed cleanly at freeze without ts wrapper
        DailyBinanceStorageManager helperBinance = new DailyBinanceStorageManager(tempDir.toString(), "BTCUSDT");
        Path binanceFile = helperBinance.resolveDailyFilePath(BinanceStreamType.SPOT_BOOK_TICKER, BASE_UTC_DATE);
        assertThat(readZstLines(binanceFile)).containsExactly(
                "{\"u\":101}"
        );

        // 7. Verify concatenated Zstandard frames in PolyBolt price_crypto file contain exact sequence 1, 2, 3, 4 without duplicates
        DailyPolyBoltStorageManager helperPolyBolt = new DailyPolyBoltStorageManager(tempDir.toString(), "BTCUSDT");
        Path cryptoFile = helperPolyBolt.resolveDailyFilePath("price_crypto", BASE_UTC_DATE);
        assertThat(readZstLines(cryptoFile)).containsExactly(
                "{\"ts\":" + (BASE_TS_MS + 100L) + ",\"payload\":{\"channel\":\"price.crypto\",\"seq\":1}}",
                "{\"ts\":" + (BASE_TS_MS + 200L) + ",\"payload\":{\"channel\":\"price.crypto\",\"seq\":2}}",
                "{\"ts\":" + (BASE_TS_MS + 300L) + ",\"payload\":{\"channel\":\"price.crypto\",\"seq\":3}}",
                "{\"ts\":" + (BASE_TS_MS + 400L) + ",\"payload\":{\"channel\":\"price.crypto\",\"seq\":4}}"
        );

        Path twapFile = helperPolyBolt.resolveDailyFilePath("price_crypto_twap_60s", BASE_UTC_DATE);
        assertThat(readZstLines(twapFile)).containsExactly(
                "{\"ts\":" + (BASE_TS_MS + 200L) + ",\"payload\":{\"channel\":\"price.crypto.twap\",\"seq\":10}}",
                "{\"ts\":" + (BASE_TS_MS + 300L) + ",\"payload\":{\"channel\":\"price.crypto.twap\",\"seq\":11}}"
        );

        // 8. Verify existing Polymarket market file has 1 market_discovered and seq 1, 2, 3 without duplicates
        PolymarketMarketStorageManager helperPoly = new PolymarketMarketStorageManager(tempDir.toString(), "BTCUSDT", "15m");
        Path existingMarketFile = helperPoly.resolveMarketFilePath(slugExisting);
        List<String> existingMarketLines = readZstLines(existingMarketFile);
        assertThat(existingMarketLines).containsExactly(
                "{\"ts\":" + BASE_TS_MS + ",\"type\":\"market_discovered\",\"payload\":{\"slug\":\"" + slugExisting + "\"}}",
                "{\"ts\":" + (BASE_TS_MS + 200L) + ",\"type\":\"clob\",\"payload\":{\"seq\":1}}",
                "{\"ts\":" + (BASE_TS_MS + 300L) + ",\"type\":\"clob\",\"payload\":{\"seq\":2}}",
                "{\"ts\":" + (BASE_TS_MS + 400L) + ",\"type\":\"clob\",\"payload\":{\"seq\":3}}"
        );

        // 9. Verify brand-new Polymarket market file discovered by Green during STANDBY has its market_discovered written
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
