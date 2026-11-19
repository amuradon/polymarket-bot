package cz.polymarket.bot.connector.coinbase;

import cz.polymarket.bot.connector.ws.ConnectionState;
import cz.polymarket.bot.connector.ws.MarketWebSocketRuntime;
import cz.polymarket.bot.connector.ws.TimestampedFrame;
import cz.polymarket.bot.connector.ws.WebSocketStreamTelemetry;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit and component tests for {@link CoinbaseStreamConnector} and {@link CoinbaseStreamListener}
 * verifying symbol and product_id normalization, {@code matches} channel subscription, filtering of
 * {@code subscriptions} and {@code last_match} frames, zero-decode {@code byte[]} delivery,
 * dedicated I/O reader thread naming, and disconnect telemetry.
 */
class CoinbaseStreamConnectorTest {

    private Vertx vertx;
    private MarketWebSocketRuntime marketWsRuntime;
    private HttpServer server;

    @BeforeEach
    void setUp() {
        vertx = Vertx.vertx();
        marketWsRuntime = new MarketWebSocketRuntime(30L);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (marketWsRuntime != null) {
            marketWsRuntime.destroy();
        }
        if (server != null) {
            server.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
        if (vertx != null) {
            vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldNormalizeSymbolsAndProductIdsCorrectly() {
        assertThat(CoinbaseStreamConnector.normalizeSymbol("BTC")).isEqualTo("BTCUSD");
        assertThat(CoinbaseStreamConnector.normalizeSymbol("BTCUSDT")).isEqualTo("BTCUSD");
        assertThat(CoinbaseStreamConnector.normalizeSymbol("BTCUSD")).isEqualTo("BTCUSD");
        assertThat(CoinbaseStreamConnector.normalizeSymbol("BTC-USD")).isEqualTo("BTCUSD");
        assertThat(CoinbaseStreamConnector.normalizeSymbol("BTC/USD")).isEqualTo("BTCUSD");
        assertThat(CoinbaseStreamConnector.normalizeSymbol("ETHUSDT")).isEqualTo("ETHUSD");
        assertThat(CoinbaseStreamConnector.normalizeSymbol("ETHUSD")).isEqualTo("ETHUSD");
        assertThat(CoinbaseStreamConnector.normalizeSymbol("SOLUSDT")).isEqualTo("SOLUSD");
        assertThat(CoinbaseStreamConnector.normalizeSymbol(null)).isEqualTo("BTCUSD");
        assertThat(CoinbaseStreamConnector.normalizeSymbol("   ")).isEqualTo("BTCUSD");

        assertThat(CoinbaseStreamConnector.toProductId("BTC")).isEqualTo("BTC-USD");
        assertThat(CoinbaseStreamConnector.toProductId("BTCUSDT")).isEqualTo("BTC-USD");
        assertThat(CoinbaseStreamConnector.toProductId("BTCUSD")).isEqualTo("BTC-USD");
        assertThat(CoinbaseStreamConnector.toProductId("BTC-USD")).isEqualTo("BTC-USD");
        assertThat(CoinbaseStreamConnector.toProductId("ETHUSDT")).isEqualTo("ETH-USD");
        assertThat(CoinbaseStreamConnector.toProductId("ETHUSD")).isEqualTo("ETH-USD");
        assertThat(CoinbaseStreamConnector.toProductId("SOLUSDT")).isEqualTo("SOL-USD");
    }

    @Test
    void shouldConnectSubscribeToMatchesAndDeliverOnlyMatchEvents() throws Exception {
        List<String> subscriptionsReceived = new CopyOnWriteArrayList<>();

        server = vertx.createHttpServer()
                .webSocketHandler(ws -> ws.textMessageHandler(msg -> {
                    subscriptionsReceived.add(msg);
                    if (msg.contains("\"matches\"") && msg.contains("\"BTC-USD\"")) {
                        ws.writeTextMessage("{\"type\":\"subscriptions\",\"channels\":[{\"name\":\"matches\",\"product_ids\":[\"BTC-USD\"]}]}");
                        ws.writeTextMessage("{\"type\":\"last_match\",\"trade_id\":1120371408,\"side\":\"sell\",\"size\":\"0.01\",\"price\":\"71368.0\",\"product_id\":\"BTC-USD\",\"sequence\":100,\"time\":\"2026-11-13T00:00:00.010000Z\"}");
                        ws.writeTextMessage("{\"type\":\"match\",\"trade_id\":1120371409,\"maker_order_id\":\"m1\",\"taker_order_id\":\"t1\",\"side\":\"buy\",\"size\":\"0.00012577\",\"price\":\"71369.9\",\"product_id\":\"BTC-USD\",\"sequence\":101,\"time\":\"2026-11-13T00:00:00.090223Z\"}");
                    }
                }));

        int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
        String wsUrl = "ws://localhost:" + port;

        CoinbaseStreamConnector connector = new CoinbaseStreamConnector(marketWsRuntime, wsUrl, 200L, 5000L);

        Map<String, List<String>> receivedBySymbol = new ConcurrentHashMap<>();
        Map<String, Long> timestampsBySymbol = new ConcurrentHashMap<>();

        connector.start("BTCUSDT", (symbol, rawJson, recvTs) -> {
            receivedBySymbol.computeIfAbsent(symbol, k -> new CopyOnWriteArrayList<>()).add(rawJson);
            timestampsBySymbol.put(symbol, recvTs);
        });

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(subscriptionsReceived).hasSize(1);
                    assertThat(subscriptionsReceived.getFirst())
                            .contains("\"type\":\"subscribe\"")
                            .contains("\"BTC-USD\"")
                            .contains("\"matches\"");

                    assertThat(receivedBySymbol).containsOnlyKeys("BTCUSD");
                    assertThat(receivedBySymbol.get("BTCUSD")).hasSize(1);
                    assertThat(receivedBySymbol.get("BTCUSD").getFirst())
                            .contains("\"type\":\"match\"")
                            .contains("\"trade_id\":1120371409");
                    assertThat(timestampsBySymbol.get("BTCUSD")).isPositive();
                    assertThat(connector.isConnected()).isTrue();
                    assertThat(connector.getState()).isEqualTo(ConnectionState.CONNECTED);
                    assertThat(connector.getIoReaderThread()).isNotNull();
                    assertThat(connector.getIoReaderThread().getName())
                            .isEqualTo(MarketWebSocketRuntime.DEDICATED_MARKET_IO_THREAD_NAME);
                    assertThat(connector.getIoReaderThread().getPriority()).isEqualTo(Thread.MAX_PRIORITY);
                });

        assertThat(connector.getStreamTelemetries()).hasSize(1);
        connector.destroy();
        assertThat(connector.isConnected()).isFalse();
    }

    @Test
    void shouldDeliverZeroDecodeBytePayloadsAndSupportExternalWorkerAndDisconnectListener() throws Exception {
        List<WebSocketStreamTelemetry> disconnectEvents = new CopyOnWriteArrayList<>();

        server = vertx.createHttpServer()
                .webSocketHandler(ws -> ws.textMessageHandler(msg -> {
                    if (msg.contains("\"ETH-USD\"")) {
                        ws.writeTextMessage("{\"type\":\"match\",\"trade_id\":55501,\"side\":\"buy\",\"size\":\"2.5\",\"price\":\"3125.00\",\"product_id\":\"ETH-USD\",\"time\":\"2026-11-13T00:00:00.500000Z\"}");
                        vertx.setTimer(40L, id -> ws.close((short) 1013, "synthetic coinbase restart"));
                    }
                }));

        int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
        String wsUrl = "ws://localhost:" + port;

        CoinbaseStreamConnector connector = new CoinbaseStreamConnector(marketWsRuntime, wsUrl, 200L, 0L);
        connector.setDisconnectListener(disconnectEvents::add);

        Map<String, String> decodedFromBytes = new ConcurrentHashMap<>();
        AtomicBoolean workerRunning = new AtomicBoolean(true);

        Thread externalWorker = Thread.ofPlatform()
                .daemon(true)
                .name("test-coinbase-ext-worker")
                .unstarted(() -> {
                    List<TimestampedFrame> scratch = new ArrayList<>(64);
                    while (workerRunning.get()) {
                        int drained = connector.drainStream(scratch);
                        if (drained > 0) {
                            continue;
                        }
                        LockSupport.parkNanos(connector, 15_000_000L);
                    }
                });

        CoinbaseStreamListener byteListener = new CoinbaseStreamListener() {
            @Override
            public void onStreamMessage(String symbol, String rawJson, long recvTimestampMs) {
                throw new AssertionError("onStreamMessage(String) must not be called when onStreamFrame(byte[]) is overridden");
            }

            @Override
            public void onStreamFrame(String symbol, byte[] rawUtf8Bytes, long recvTimestampMs) {
                decodedFromBytes.put(symbol, new String(rawUtf8Bytes, StandardCharsets.UTF_8));
            }
        };

        externalWorker.start();
        connector.start("ETHUSD", byteListener, externalWorker);

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(decodedFromBytes).containsOnlyKeys("ETHUSD");
                    assertThat(decodedFromBytes.get("ETHUSD")).contains("\"trade_id\":55501");
                    assertThat(disconnectEvents).isNotEmpty();
                    assertThat(disconnectEvents.getFirst().lastCloseCode()).isEqualTo(1013);
                });

        workerRunning.set(false);
        LockSupport.unpark(externalWorker);
        connector.destroy();
    }
}
