package cz.polymarket.bot.data.web;

import com.github.luben.zstd.ZstdInputStream;
import cz.polymarket.bot.data.MockMarketStreamsTestResource;
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
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Full-system integration test verifying that by default the Data Collector application connects
 * on startup only to the two Polymarket PolyBolt streams ({@code price.crypto} and {@code price.crypto.twap})
 * and Polymarket CLOB streams (current + 5 lookahead markets = 6 total), writes only their {@code .jsonl.zst}
 * archives to disk while keeping Binance {@code bookTicker} streams disabled, reports {@code readyForCutover=true},
 * and executes cutover REST endpoints.
 */
@QuarkusTest
@QuarkusTestResource(value = MockMarketStreamsTestResource.class, restrictToAnnotatedClass = true)
class CollectorStatusResourceTest {

    @Inject
    DailyPolyBoltStorageManager polyBoltDailyStorage;

    @Inject
    PolymarketMarketStorageManager polymarketStorage;

    @ConfigProperty(name = "polymarket.collector.data-dir")
    String dataDir;

    @Test
    void shouldCollectOnlyPolyBoltAndClobStreamsByDefaultAndPersistZstdFiles() throws Exception {
        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> given()
                        .when().get("/api/collector/status")
                        .then()
                        .statusCode(200)
                        .body("symbol", equalTo("BTCUSDT"))
                        .body("timeframe", equalTo("15m"))
                        .body("writeMode", equalTo("ACTIVE"))
                        .body("readyForCutover", equalTo(true))
                        .body("standbyBufferedRecords", equalTo(0))
                        .body("binanceSpotBookTickerEnabled", equalTo(false))
                        .body("binanceFuturesBookTickerEnabled", equalTo(false))
                        .body("binanceStreams", anEmptyMap())
                        .body("polymarketPolyBoltTwapState", equalTo("CONNECTED"))
                        .body("polymarketPolyBoltCryptoPriceState", equalTo("CONNECTED"))
                        .body("trackedPolymarketSlugs", hasSize(6))
                        .body("binanceRecordsWritten", equalTo(0))
                        .body("polyBoltRecordsWritten", greaterThanOrEqualTo(2))
                        .body("polymarketRecordsWritten", greaterThanOrEqualTo(18)));

        polyBoltDailyStorage.flushAll();
        polymarketStorage.flushAll();

        Path root = Path.of(dataDir);
        List<Path> zstFiles;
        try (Stream<Path> walk = Files.walk(root)) {
            zstFiles = walk.filter(p -> p.toString().endsWith(".jsonl.zst")).toList();
        }

        // 2 daily PolyBolt + 6 Polymarket = 8 .jsonl.zst files (no Binance, Kraken, or Coinbase files)
        assertThat(zstFiles).hasSize(8);
        assertThat(Files.exists(root.resolve("binance"))).isFalse();
        assertThat(Files.exists(root.resolve("kraken"))).isFalse();
        assertThat(Files.exists(root.resolve("coinbase"))).isFalse();

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

        // Verify cutover freeze and activate REST endpoints
        long freezeTs = given()
                .when().post("/api/collector/cutover/freeze")
                .then()
                .statusCode(200)
                .body("writeMode", equalTo("FROZEN"))
                .body("cutoverTimestampMs", greaterThan(0L))
                .extract().path("cutoverTimestampMs");

        given()
                .queryParam("afterTimestampMs", freezeTs)
                .when().post("/api/collector/cutover/activate")
                .then()
                .statusCode(200)
                .body("writeMode", equalTo("ACTIVE"))
                .body("activatedAfterTimestampMs", equalTo(freezeTs));
    }

    @Test
    void shouldConfigureRecognizedSecretsManagerAwsRegionAndNativeAccessManifestAttribute() throws Exception {
        List<String> lines = Files.readAllLines(Path.of("src/main/resources/application.properties"), StandardCharsets.UTF_8);

        assertThat(lines)
                .contains("%prod.quarkus.secretsmanager.aws.region=eu-west-1")
                .contains("quarkus.package.jar.manifest.attributes.\"Enable-Native-Access\"=ALL-UNNAMED")
                .contains("quarkus.arc.remove-unused-beans=fwk")
                .contains("polymarket.collector.binance.spot-book-ticker-enabled=${POLYMARKET_COLLECTOR_BINANCE_SPOT_BOOK_TICKER_ENABLED:false}")
                .contains("polymarket.collector.binance.futures-book-ticker-enabled=${POLYMARKET_COLLECTOR_BINANCE_FUTURES_BOOK_TICKER_ENABLED:false}")
                .noneMatch(line -> line.startsWith("%prod.quarkus.secretsmanager.region="))
                .noneMatch(line -> line.startsWith("quarkus.index-dependency.common."));

        Path beansXml = Path.of("src/main/resources/META-INF/beans.xml");
        assertThat(beansXml).exists();
        assertThat(Files.readString(beansXml, StandardCharsets.UTF_8))
                .contains("bean-discovery-mode=\"annotated\"");
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
