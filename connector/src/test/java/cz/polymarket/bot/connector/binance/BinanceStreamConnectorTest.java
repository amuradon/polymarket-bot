package cz.polymarket.bot.connector.binance;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link BinanceStreamType} and {@link BinanceStreamConnector} verifying all 5 Binance streams.
 */
class BinanceStreamConnectorTest {

    private Vertx vertx;
    private HttpServer server;

    @BeforeEach
    void setUp() {
        vertx = Vertx.vertx();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (server != null) {
            server.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
        if (vertx != null) {
            vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldBuildExpectedStreamUrlsForSymbol() {
        String spotBase = "wss://stream.binance.com:9443/ws";
        String futuresBase = "wss://fstream.binance.com";

        assertThat(BinanceStreamType.SPOT_AGG_TRADES.buildWsUrl(spotBase, futuresBase, "BTCUSDT"))
                .isEqualTo("wss://stream.binance.com:9443/ws/btcusdt@aggTrade");
        assertThat(BinanceStreamType.FUTURES_AGG_TRADES.buildWsUrl(spotBase, futuresBase, "BTCUSDT"))
                .isEqualTo("wss://fstream.binance.com/market/ws/btcusdt@aggTrade");
        assertThat(BinanceStreamType.FUTURES_DIFF_DEPTH_100MS.buildWsUrl(spotBase, futuresBase, "BTCUSDT"))
                .isEqualTo("wss://fstream.binance.com/public/ws/btcusdt@depth@100ms");
        assertThat(BinanceStreamType.FUTURES_PARTIAL_DEPTH_100MS.buildWsUrl(spotBase, futuresBase, "BTCUSDT"))
                .isEqualTo("wss://fstream.binance.com/public/ws/btcusdt@depth20@100ms");
        assertThat(BinanceStreamType.FUTURES_FORCE_ORDER.buildWsUrl(spotBase, futuresBase, "BTCUSDT"))
                .isEqualTo("wss://fstream.binance.com/market/ws/btcusdt@forceOrder");
    }

    @Test
    void shouldConnectAllFiveBinanceStreamsAndDeliverPayloadsWithLocalTimestamp() throws Exception {
        server = vertx.createHttpServer()
                .webSocketHandler(ws -> {
                    String path = ws.path();
                    ws.writeTextMessage("{\"streamPath\":\"" + path + "\",\"synthetic\":true}");
                });

        int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
        String spotWsBase = "ws://localhost:" + port + "/spot";
        String futuresWsBase = "ws://localhost:" + port + "/futures";

        BinanceStreamConnector connector = new BinanceStreamConnector(
                vertx,
                spotWsBase,
                futuresWsBase,
                200L,
                5000L
        );

        Map<BinanceStreamType, String> receivedByType = new ConcurrentHashMap<>();
        Map<BinanceStreamType, Long> timestampsByType = new ConcurrentHashMap<>();

        connector.start("BTCUSDT", (type, rawJson, recvTs) -> {
            receivedByType.put(type, rawJson);
            timestampsByType.put(type, recvTs);
        });

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(receivedByType).hasSize(5);
                    assertThat(receivedByType.get(BinanceStreamType.SPOT_AGG_TRADES))
                            .contains("/spot/btcusdt@aggTrade");
                    assertThat(receivedByType.get(BinanceStreamType.FUTURES_AGG_TRADES))
                            .contains("/futures/market/ws/btcusdt@aggTrade");
                    assertThat(receivedByType.get(BinanceStreamType.FUTURES_DIFF_DEPTH_100MS))
                            .contains("/futures/public/ws/btcusdt@depth@100ms");
                    assertThat(receivedByType.get(BinanceStreamType.FUTURES_PARTIAL_DEPTH_100MS))
                            .contains("/futures/public/ws/btcusdt@depth20@100ms");
                    assertThat(receivedByType.get(BinanceStreamType.FUTURES_FORCE_ORDER))
                            .contains("/futures/market/ws/btcusdt@forceOrder");
                    assertThat(timestampsByType.values()).allMatch(ts -> ts > 0L);
                    assertThat(connector.isAllConnected()).isTrue();
                });

        connector.stop();
        assertThat(connector.isAllConnected()).isFalse();
    }
}
