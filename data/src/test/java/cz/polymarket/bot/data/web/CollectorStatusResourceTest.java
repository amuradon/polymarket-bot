package cz.polymarket.bot.data.web;

import com.github.luben.zstd.ZstdInputStream;
import cz.polymarket.bot.data.MockMarketStreamsTestResource;
import cz.polymarket.bot.data.storage.DailyBinanceStorageManager;
import cz.polymarket.bot.data.storage.DailyDeribitStorageManager;
import cz.polymarket.bot.data.storage.DailyPolyBoltStorageManager;
import cz.polymarket.bot.data.storage.PolymarketMarketStorageManager;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
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
import java.util.stream.Stream;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Full-system integration test verifying that the Data Collector application automatically connects
 * on startup to all mocked Binance and Polymarket streams, tracks the current market plus 5 lookahead
 * future markets (6 total), writes daily Binance and PolyBolt ({@code price.crypto} and {@code price.crypto.twap})
 * files as well as per-market {@code .jsonl.zst} files to disk, and reports live status.
 */
@QuarkusTest
@QuarkusTestResource(MockMarketStreamsTestResource.class)
class CollectorStatusResourceTest {

    @Inject
    DailyBinanceStorageManager binanceStorage;

    @Inject
    DailyDeribitStorageManager deribitStorage;

    @Inject
    DailyPolyBoltStorageManager polyBoltDailyStorage;

    @Inject
    PolymarketMarketStorageManager polymarketStorage;

    @ConfigProperty(name = "polymarket.collector.data-dir")
    String dataDir;

    @Test
    void shouldAutomaticallyCollectAllStreamsOnStartupAndPersistZstdFiles() throws Exception {
        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> given()
                        .when().get("/api/collector/status")
                        .then()
                        .statusCode(200)
                        .body("symbol", equalTo("BTCUSDT"))
                        .body("timeframe", equalTo("15m"))
                        .body("binanceStreams.SPOT_AGG_TRADES", equalTo("CONNECTED"))
                        .body("binanceStreams.FUTURES_AGG_TRADES", equalTo("CONNECTED"))
                        .body("binanceStreams.FUTURES_DIFF_DEPTH_100MS", equalTo("CONNECTED"))
                        .body("binanceStreams.FUTURES_PARTIAL_DEPTH_100MS", equalTo("CONNECTED"))
                        .body("binanceStreams.FUTURES_FORCE_ORDER", equalTo("CONNECTED"))
                        .body("deribitStreamState", equalTo("CONNECTED"))
                        .body("polymarketPolyBoltTwapState", equalTo("CONNECTED"))
                        .body("polymarketPolyBoltCryptoPriceState", equalTo("CONNECTED"))
                        .body("trackedPolymarketSlugs", hasSize(6))
                        .body("binanceRecordsWritten", greaterThanOrEqualTo(5))
                        .body("deribitRecordsWritten", greaterThanOrEqualTo(1))
                        .body("polyBoltRecordsWritten", greaterThanOrEqualTo(2))
                        .body("polymarketRecordsWritten", greaterThanOrEqualTo(18)));

        binanceStorage.flushAll();
        deribitStorage.flushAll();
        polyBoltDailyStorage.flushAll();
        polymarketStorage.flushAll();

        Path root = Path.of(dataDir);
        List<Path> zstFiles;
        try (Stream<Path> walk = Files.walk(root)) {
            zstFiles = walk.filter(p -> p.toString().endsWith(".jsonl.zst")).toList();
        }

        // 5 daily Binance stream files + 1 daily Deribit trades file + 2 daily PolyBolt files + 6 Polymarket market files = 14 .jsonl.zst files
        assertThat(zstFiles).hasSize(14);

        // Verify daily Deribit trades file
        Path deribitTradesDir = root.resolve("deribit/futures/BTC-PERPETUAL/trades");
        List<Path> deribitFiles;
        try (Stream<Path> list = Files.list(deribitTradesDir)) {
            deribitFiles = list.filter(p -> p.toString().endsWith(".jsonl.zst")).toList();
        }
        assertThat(deribitFiles).hasSize(1);
        List<String> deribitLines = readZstLines(deribitFiles.get(0));
        assertThat(deribitLines).anyMatch(l -> l.contains("\"channel\":\"trades.BTC-PERPETUAL.raw\"") && l.contains("78452.0"));

        // Verify daily PolyBolt price.crypto file
        Path polyBoltCryptoDir = root.resolve("polymarket/price_crypto/btcusd");
        List<Path> cryptoFiles;
        try (Stream<Path> list = Files.list(polyBoltCryptoDir)) {
            cryptoFiles = list.filter(p -> p.toString().endsWith(".jsonl.zst")).toList();
        }
        assertThat(cryptoFiles).hasSize(1);
        List<String> cryptoLines = readZstLines(cryptoFiles.get(0));
        assertThat(cryptoLines).anyMatch(l -> l.contains("\"channel\":\"price.crypto\"") && l.contains("78460.0"));

        // Verify daily PolyBolt price.crypto.twap 60s file
        Path polyBoltTwapDir = root.resolve("polymarket/price_crypto_twap_60s/btcusd");
        List<Path> twapFiles;
        try (Stream<Path> list = Files.list(polyBoltTwapDir)) {
            twapFiles = list.filter(p -> p.toString().endsWith(".jsonl.zst")).toList();
        }
        assertThat(twapFiles).hasSize(1);
        List<String> twapLines = readZstLines(twapFiles.get(0));
        assertThat(twapLines).anyMatch(l -> l.contains("\"channel\":\"price.crypto.twap\"") && l.contains("78450.5"));

        // Verify 6 Polymarket per-market files
        Path polymarketDir = root.resolve("polymarket/btc-updown-15m");
        List<Path> marketFiles;
        try (Stream<Path> list = Files.list(polymarketDir)) {
            marketFiles = list.filter(p -> p.toString().endsWith(".jsonl.zst")).toList();
        }
        assertThat(marketFiles).hasSize(6);

        List<String> marketLines = readZstLines(marketFiles.get(0));
        assertThat(marketLines).anyMatch(l -> l.contains("\"type\":\"market_discovered\""));
        assertThat(marketLines).anyMatch(l -> l.contains("\"type\":\"clob\"") && l.contains("\"event_type\":\"book\""));
        assertThat(marketLines).anyMatch(l -> l.contains("\"type\":\"clob\"") && l.contains("\"event_type\":\"last_trade_price\""));
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
