package cz.polymarket.bot.collector.storage;

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
 * Unit tests for {@link PolymarketMarketStorageManager} verifying UTC daily subdirectory partitioning
 * across timeframes ({@code 15m} and {@code 5m}), restart-append into daily archives, and fail-fast
 * slug validation.
 */
class PolymarketMarketStorageManagerTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldStoreMarketsInUtcDailySubdirectoriesFor15mAnd5mTimeframes() throws Exception {
        PolymarketMarketStorageManager storage15m = new PolymarketMarketStorageManager(tempDir.toString(), "BTCUSDT", "15m");
        PolymarketMarketStorageManager storage5m = new PolymarketMarketStorageManager(tempDir.toString(), "ETHUSDT", "5m");

        long day1LateStartSec = Instant.parse("2026-11-27T23:45:00Z").getEpochSecond();
        long day2OpenStartSec = Instant.parse("2026-11-28T00:00:00Z").getEpochSecond();

        String slugDay1 = "btc-updown-15m-" + day1LateStartSec;
        String slugDay2Lookahead = "btc-updown-15m-" + day2OpenStartSec;
        String slug5mDay2 = "eth-updown-5m-" + day2OpenStartSec;

        // Even when lookahead slugDay2Lookahead is discovered on 2026-11-27T23:46Z, its path belongs to 2026-11-28
        long discoveredAtDay1Ms = Instant.parse("2026-11-27T23:46:00Z").toEpochMilli();
        storage15m.recordMarketEvent(slugDay1, discoveredAtDay1Ms, "market_discovered", "{\"slug\":\"" + slugDay1 + "\"}");
        storage15m.recordMarketEvent(slugDay2Lookahead, discoveredAtDay1Ms, "market_discovered", "{\"slug\":\"" + slugDay2Lookahead + "\"}");
        storage5m.recordMarketEventBytes(
                slug5mDay2,
                discoveredAtDay1Ms,
                "clob",
                "{\"event_type\":\"book\"}".getBytes(StandardCharsets.UTF_8)
        );

        storage15m.closeAll();
        storage5m.closeAll();

        Path expectedDay1 = tempDir.resolve("polymarket/btc-updown-15m/2026-11-27/" + slugDay1 + ".jsonl.zst");
        Path expectedDay2 = tempDir.resolve("polymarket/btc-updown-15m/2026-11-28/" + slugDay2Lookahead + ".jsonl.zst");
        Path expected5mDay2 = tempDir.resolve("polymarket/eth-updown-5m/2026-11-28/" + slug5mDay2 + ".jsonl.zst");

        assertThat(Files.isRegularFile(expectedDay1)).isTrue();
        assertThat(Files.isRegularFile(expectedDay2)).isTrue();
        assertThat(Files.isRegularFile(expected5mDay2)).isTrue();
        assertThat(storage15m.marketArchiveExists(slugDay1)).isTrue();
        assertThat(storage15m.marketArchiveExists(slugDay2Lookahead)).isTrue();
        assertThat(storage5m.marketArchiveExists(slug5mDay2)).isTrue();
    }

    @Test
    void shouldAppendToExistingDailyArchiveAcrossRestarts() throws Exception {
        long startSec = Instant.parse("2026-11-27T16:45:00Z").getEpochSecond();
        String slug = "btc-updown-15m-" + startSec;

        PolymarketMarketStorageManager storage1 = new PolymarketMarketStorageManager(tempDir.toString(), "BTCUSDT", "15m");
        assertThat(storage1.marketArchiveExists(slug)).isFalse();
        storage1.recordMarketEvent(slug, startSec * 1000L, "market_discovered", "{\"slug\":\"" + slug + "\"}");
        storage1.recordMarketEvent(slug, startSec * 1000L + 100L, "clob", "{\"event_type\":\"book\"}");
        storage1.closeAll();

        PolymarketMarketStorageManager storage2 = new PolymarketMarketStorageManager(tempDir.toString(), "BTCUSDT", "15m");
        assertThat(storage2.marketArchiveExists(slug)).isTrue();
        storage2.recordMarketEventBytes(
                slug,
                startSec * 1000L + 5000L,
                "clob",
                "{\"event_type\":\"last_trade_price\"}".getBytes(StandardCharsets.UTF_8)
        );
        storage2.closeAll();

        Path dailyFile = tempDir.resolve("polymarket/btc-updown-15m/2026-11-27/" + slug + ".jsonl.zst");
        assertThat(Files.isRegularFile(dailyFile)).isTrue();

        List<String> lines = ZstdJsonlWriterTest.readAllZstdLines(dailyFile);
        assertThat(lines).hasSize(3);
        assertThat(lines.get(0)).contains("\"type\":\"market_discovered\"");
        assertThat(lines.get(1)).contains("\"event_type\":\"book\"");
        assertThat(lines.get(2)).contains("\"event_type\":\"last_trade_price\"");
    }

    @Test
    void shouldFailFastOnInvalidMarketSlugWithoutTrailingEpochSeconds() {
        PolymarketMarketStorageManager storage = new PolymarketMarketStorageManager(tempDir.toString(), "BTCUSDT", "15m");

        assertThatThrownBy(() -> storage.resolveMarketFilePath("btc-updown-15m"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("numeric epoch seconds");
        assertThatThrownBy(() -> storage.resolveMarketFilePath("invalidslug"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("-<epochSeconds>");
        assertThatThrownBy(() -> PolymarketMarketStorageManager.extractUtcDateFromSlug(" "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("null or blank");
    }
}
