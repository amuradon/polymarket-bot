package cz.polymarket.bot.web;

import cz.polymarket.bot.MockMarketStreamsTestResource;
import cz.polymarket.bot.collector.storage.DailyBinanceStorageManager;
import cz.polymarket.bot.collector.storage.DailyPolyBoltStorageManager;
import cz.polymarket.bot.collector.storage.PolymarketMarketStorageManager;
import cz.polymarket.bot.collector.storage.ZstdJsonlWriterTest;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
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
 * Component test verifying that enabling {@code polymarket.collector.binance.spot-book-ticker-enabled}
 * and {@code polymarket.collector.binance.futures-book-ticker-enabled} collects and persists only the two
 * Binance {@code bookTicker} streams alongside PolyBolt and Polymarket CLOB ({@code lookahead-count = 1}).
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
    void shouldCollectSpotAndFuturesBookTickerAlongsideCurrentPlusOneLookaheadPolymarketMarketInOffMode() throws Exception {
        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> given()
                        .when().get("/api/collector/status")
                        .then()
                        .statusCode(200)
                        .body("healthy", equalTo(true))
                        .body("lookaheadCount", equalTo(1))
                        .body("binanceSpotBookTickerEnabled", equalTo(true))
                        .body("binanceFuturesBookTickerEnabled", equalTo(true))
                        .body("binanceStreams.SPOT_BOOK_TICKER", equalTo("CONNECTED"))
                        .body("binanceStreams.FUTURES_BOOK_TICKER", equalTo("CONNECTED"))
                        .body("binanceStreams.SPOT_AGG_TRADES", nullValue())
                        .body("binanceStreams.FUTURES_AGG_TRADES", nullValue())
                        .body("polymarketPolyBoltTwapState", equalTo("CONNECTED"))
                        .body("polymarketPolyBoltCryptoPriceState", equalTo("CONNECTED"))
                        .body("trackedPolymarketSlugs", hasSize(4))
                        .body("binanceRecordsWritten", greaterThanOrEqualTo(2))
                        .body("polyBoltRecordsWritten", greaterThanOrEqualTo(2))
                        .body("polymarketRecordsWritten", greaterThanOrEqualTo(6)));

        // /api/health must also return 200 OK in OFF mode while Collector is healthy
        given()
                .when().get("/api/health")
                .then()
                .statusCode(200)
                .body("healthy", equalTo(true))
                .body("strategyMode", equalTo("OFF"))
                .body("collectorHealthy", equalTo(true))
                .body("strategyHealthy", equalTo(true));

        binanceStorage.flushAll();
        polyBoltDailyStorage.flushAll();
        polymarketStorage.flushAll();

        Path root = Path.of(dataDir);
        List<Path> zstFiles;
        try (Stream<Path> walk = Files.walk(root)) {
            zstFiles = walk.filter(p -> p.toString().endsWith(".jsonl.zst")).toList();
        }

        // 2 daily Binance bookTicker + 2 daily PolyBolt + 2 Polymarket 15m + 2 Polymarket 5m = 8 .jsonl.zst files
        assertThat(zstFiles).hasSize(8);
        assertThat(Files.exists(root.resolve("binance/spot/BTCUSDT/aggTrades"))).isFalse();

        // Polymarket CLOB archives for both 15m and 5m must reside inside UTC daily yyyy-MM-dd subdirectories
        for (String series : List.of("btc-updown-15m", "btc-updown-5m")) {
            List<Path> seriesFiles = zstFiles.stream()
                    .filter(p -> p.toString().contains(series))
                    .toList();
            assertThat(seriesFiles).hasSize(2);
            assertThat(seriesFiles).allSatisfy(p -> {
                assertThat(p.getParent().getFileName().toString()).matches("\\d{4}-\\d{2}-\\d{2}");
                assertThat(p.getParent().getParent().getFileName().toString()).isEqualTo(series);
            });
        }

        Path spotBookTickerDir = root.resolve("binance/spot/BTCUSDT/bookTicker");
        List<Path> spotFiles;
        try (Stream<Path> list = Files.list(spotBookTickerDir)) {
            spotFiles = list.filter(p -> p.toString().endsWith(".jsonl.zst")).toList();
        }
        assertThat(spotFiles).hasSize(1);
        assertThat(ZstdJsonlWriterTest.readAllZstdLines(spotFiles.getFirst()))
                .anyMatch(l -> l.contains("\"u\":400900217") && l.contains("\"b\":\"78450.00\""));
    }
}
