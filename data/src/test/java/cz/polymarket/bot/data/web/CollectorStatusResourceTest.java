package cz.polymarket.bot.data.web;

import com.github.luben.zstd.ZstdInputStream;
import cz.polymarket.bot.data.MockMarketStreamsTestResource;
import cz.polymarket.bot.data.storage.DailyBinanceStorageManager;
import cz.polymarket.bot.data.storage.DailyCoinbaseStorageManager;
import cz.polymarket.bot.data.storage.DailyKrakenStorageManager;
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
 * on startup to all mocked Binance, Kraken, Coinbase, and Polymarket streams, tracks the current
 * market plus 5 lookahead future markets (6 total), writes daily Binance, Kraken, Coinbase, and
 * PolyBolt ({@code price.crypto} and {@code price.crypto.twap}) files as well as per-market {@code .jsonl.zst}
 * files to disk, and reports live status.
 */
@QuarkusTest
@QuarkusTestResource(MockMarketStreamsTestResource.class)
class CollectorStatusResourceTest {

    @Inject
    DailyBinanceStorageManager binanceStorage;

    @Inject
    DailyKrakenStorageManager krakenStorage;

    @Inject
    DailyCoinbaseStorageManager coinbaseStorage;

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
                        .body("binanceStreams.SPOT_BOOK_TICKER", equalTo("CONNECTED"))
                        .body("binanceStreams.FUTURES_AGG_TRADES", equalTo("CONNECTED"))
                        .body("binanceStreams.FUTURES_BOOK_TICKER", equalTo("CONNECTED"))
                        .body("krakenUsdtUsdEnabled", equalTo(true))
                        .body("krakenStreams.USDTUSD", equalTo("CONNECTED"))
                        .body("krakenStreams.BTCUSD", equalTo("CONNECTED"))
                        .body("coinbaseStreamState", equalTo("CONNECTED"))
                        .body("polymarketPolyBoltTwapState", equalTo("CONNECTED"))
                        .body("polymarketPolyBoltCryptoPriceState", equalTo("CONNECTED"))
                        .body("trackedPolymarketSlugs", hasSize(6))
                        .body("binanceRecordsWritten", greaterThanOrEqualTo(4))
                        .body("krakenRecordsWritten", greaterThanOrEqualTo(2))
                        .body("coinbaseRecordsWritten", greaterThanOrEqualTo(1))
                        .body("polyBoltRecordsWritten", greaterThanOrEqualTo(2))
                        .body("polymarketRecordsWritten", greaterThanOrEqualTo(18)));

        binanceStorage.flushAll();
        krakenStorage.flushAll();
        coinbaseStorage.flushAll();
        polyBoltDailyStorage.flushAll();
        polymarketStorage.flushAll();

        Path root = Path.of(dataDir);
        List<Path> zstFiles;
        try (Stream<Path> walk = Files.walk(root)) {
            zstFiles = walk.filter(p -> p.toString().endsWith(".jsonl.zst")).toList();
        }

        // 4 daily Binance + 2 daily Kraken (USDTUSD, BTCUSD) + 1 daily Coinbase (BTCUSD) + 2 daily PolyBolt + 6 Polymarket = 15 .jsonl.zst files
        assertThat(zstFiles).hasSize(15);

        // Verify daily Binance Spot and Futures bookTicker files contain host receipt timestamp "ts" and payload
        Path spotBookTickerDir = root.resolve("binance/spot/BTCUSDT/bookTicker");
        List<Path> spotBookTickerFiles;
        try (Stream<Path> list = Files.list(spotBookTickerDir)) {
            spotBookTickerFiles = list.filter(p -> p.toString().endsWith(".jsonl.zst")).toList();
        }
        assertThat(spotBookTickerFiles).hasSize(1);
        List<String> spotBookTickerLines = readZstLines(spotBookTickerFiles.get(0));
        assertThat(spotBookTickerLines).anyMatch(l -> l.startsWith("{\"ts\":") && l.contains("\"u\":400900217") && l.contains("\"b\":\"78450.00\""));

        Path futuresBookTickerDir = root.resolve("binance/futures/BTCUSDT/bookTicker");
        List<Path> futuresBookTickerFiles;
        try (Stream<Path> list = Files.list(futuresBookTickerDir)) {
            futuresBookTickerFiles = list.filter(p -> p.toString().endsWith(".jsonl.zst")).toList();
        }
        assertThat(futuresBookTickerFiles).hasSize(1);
        List<String> futuresBookTickerLines = readZstLines(futuresBookTickerFiles.get(0));
        assertThat(futuresBookTickerLines).anyMatch(l -> l.startsWith("{\"ts\":") && l.contains("\"e\":\"bookTicker\"") && l.contains("\"u\":400900218"));

        // Verify daily Kraken trades files (USDTUSD and BTCUSD)
        Path krakenUsdtTradesDir = root.resolve("kraken/spot/USDTUSD/trades");
        List<Path> krakenUsdtFiles;
        try (Stream<Path> list = Files.list(krakenUsdtTradesDir)) {
            krakenUsdtFiles = list.filter(p -> p.toString().endsWith(".jsonl.zst")).toList();
        }
        assertThat(krakenUsdtFiles).hasSize(1);
        List<String> krakenUsdtLines = readZstLines(krakenUsdtFiles.get(0));
        assertThat(krakenUsdtLines).anyMatch(l -> l.contains("\"symbol\":\"USDT/USD\"") && l.contains("1.0001"));

        Path krakenBtcTradesDir = root.resolve("kraken/spot/BTCUSD/trades");
        List<Path> krakenBtcFiles;
        try (Stream<Path> list = Files.list(krakenBtcTradesDir)) {
            krakenBtcFiles = list.filter(p -> p.toString().endsWith(".jsonl.zst")).toList();
        }
        assertThat(krakenBtcFiles).hasSize(1);
        List<String> krakenBtcLines = readZstLines(krakenBtcFiles.get(0));
        assertThat(krakenBtcLines).anyMatch(l -> l.contains("\"symbol\":\"BTC/USD\"") && l.contains("78450.0"));

        // Verify daily Coinbase trades file (BTCUSD)
        Path coinbaseBtcTradesDir = root.resolve("coinbase/spot/BTCUSD/trades");
        List<Path> coinbaseBtcFiles;
        try (Stream<Path> list = Files.list(coinbaseBtcTradesDir)) {
            coinbaseBtcFiles = list.filter(p -> p.toString().endsWith(".jsonl.zst")).toList();
        }
        assertThat(coinbaseBtcFiles).hasSize(1);
        List<String> coinbaseBtcLines = readZstLines(coinbaseBtcFiles.get(0));
        assertThat(coinbaseBtcLines).anyMatch(l -> l.contains("\"product_id\":\"BTC-USD\"") && l.contains("78455.25"));

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

    @Test
    void shouldConfigureRecognizedSecretsManagerAwsRegionAndNativeAccessManifestAttribute() throws Exception {
        List<String> lines = Files.readAllLines(Path.of("src/main/resources/application.properties"), StandardCharsets.UTF_8);

        assertThat(lines)
                .contains("%prod.quarkus.secretsmanager.aws.region=eu-west-1")
                .contains("quarkus.package.jar.manifest.attributes.\"Enable-Native-Access\"=ALL-UNNAMED")
                .contains("quarkus.arc.remove-unused-beans=fwk")
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
