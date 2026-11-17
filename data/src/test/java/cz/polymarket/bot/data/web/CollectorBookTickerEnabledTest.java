package cz.polymarket.bot.data.web;

import com.github.luben.zstd.ZstdInputStream;
import cz.polymarket.bot.data.MockMarketStreamsTestResource;
import cz.polymarket.bot.data.storage.DailyBinanceStorageManager;
import cz.polymarket.bot.data.storage.DailyPolyBoltStorageManager;
import cz.polymarket.bot.data.storage.PolymarketMarketStorageManager;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

/**
 * Integration test verifying that enabling {@code polymarket.collector.binance.spot-book-ticker-enabled}
 * and {@code polymarket.collector.binance.futures-book-ticker-enabled} collects and persists only the two
 * Binance {@code bookTicker} streams alongside PolyBolt and Polymarket CLOB (never {@code aggTrades}).
 */
@QuarkusTest
@QuarkusTestResource(value = MockMarketStreamsTestResource.class, restrictToAnnotatedClass = true)
@TestProfile(CollectorBookTickerEnabledTest.BookTickerEnabledProfile.class)
class CollectorBookTickerEnabledTest {

    public static class BookTickerEnabledProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of(
                    "polymarket.collector.binance.spot-book-ticker-enabled", "true",
                    "polymarket.collector.binance.futures-book-ticker-enabled", "true"
            );
        }
    }

    @Inject
    DailyBinanceStorageManager binanceStorage;

    @Inject
    DailyPolyBoltStorageManager polyBoltDailyStorage;

    @Inject
    PolymarketMarketStorageManager polymarketStorage;

    @ConfigProperty(name = "polymarket.collector.data-dir")
    String dataDir;

    @Test
    void shouldCollectSpotAndFuturesBookTickerWhenEnabledAlongsidePolymarketStreams() throws Exception {
        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> given()
                        .when().get("/api/collector/status")
                        .then()
                        .statusCode(200)
                        .body("readyForCutover", equalTo(true))
                        .body("binanceSpotBookTickerEnabled", equalTo(true))
                        .body("binanceFuturesBookTickerEnabled", equalTo(true))
                        .body("binanceStreams.SPOT_BOOK_TICKER", equalTo("CONNECTED"))
                        .body("binanceStreams.FUTURES_BOOK_TICKER", equalTo("CONNECTED"))
                        .body("binanceStreams.SPOT_AGG_TRADES", nullValue())
                        .body("binanceStreams.FUTURES_AGG_TRADES", nullValue())
                        .body("polymarketPolyBoltTwapState", equalTo("CONNECTED"))
                        .body("polymarketPolyBoltCryptoPriceState", equalTo("CONNECTED"))
                        .body("trackedPolymarketSlugs", hasSize(6))
                        .body("binanceRecordsWritten", greaterThanOrEqualTo(2))
                        .body("polyBoltRecordsWritten", greaterThanOrEqualTo(2))
                        .body("polymarketRecordsWritten", greaterThanOrEqualTo(18)));

        binanceStorage.flushAll();
        polyBoltDailyStorage.flushAll();
        polymarketStorage.flushAll();

        Path root = Path.of(dataDir);
        List<Path> zstFiles;
        try (Stream<Path> walk = Files.walk(root)) {
            zstFiles = walk.filter(p -> p.toString().endsWith(".jsonl.zst")).toList();
        }

        // 2 daily Binance bookTicker + 2 daily PolyBolt + 6 Polymarket = 10 .jsonl.zst files
        assertThat(zstFiles).hasSize(10);
        assertThat(Files.exists(root.resolve("binance/spot/BTCUSDT/aggTrades"))).isFalse();
        assertThat(Files.exists(root.resolve("binance/futures/BTCUSDT/aggTrades"))).isFalse();

        Path spotBookTickerDir = root.resolve("binance/spot/BTCUSDT/bookTicker");
        List<Path> spotBookTickerFiles;
        try (Stream<Path> list = Files.list(spotBookTickerDir)) {
            spotBookTickerFiles = list.filter(p -> p.toString().endsWith(".jsonl.zst")).toList();
        }
        assertThat(spotBookTickerFiles).hasSize(1);
        assertThat(readZstLines(spotBookTickerFiles.get(0)))
                .anyMatch(l -> l.startsWith("{\"ts\":") && l.contains("\"u\":400900217") && l.contains("\"b\":\"78450.00\""));

        Path futuresBookTickerDir = root.resolve("binance/futures/BTCUSDT/bookTicker");
        List<Path> futuresBookTickerFiles;
        try (Stream<Path> list = Files.list(futuresBookTickerDir)) {
            futuresBookTickerFiles = list.filter(p -> p.toString().endsWith(".jsonl.zst")).toList();
        }
        assertThat(futuresBookTickerFiles).hasSize(1);
        assertThat(readZstLines(futuresBookTickerFiles.get(0)))
                .anyMatch(l -> l.startsWith("{\"ts\":") && l.contains("\"e\":\"bookTicker\"") && l.contains("\"u\":400900218"));
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
