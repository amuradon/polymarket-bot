package cz.polymarket.bot.connector.kraken;

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
 * Unit and component tests for {@link KrakenStreamConnector} and {@link KrakenStreamListener}
 * verifying symbol normalization, dual stream subscription ({@code USDTUSD} + target symbol),
 * {@code includeUsdtUsd} toggle, control/snapshot frame filtering, zero-decode {@code byte[]} delivery,
 * dedicated I/O reader thread naming, and disconnect telemetry.
 */
class KrakenStreamConnectorTest {

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
    void shouldNormalizeSymbolsAndWsSymbolsCorrectly() {
        assertThat(KrakenStreamConnector.normalizeSymbol("BTC")).isEqualTo("BTCUSD");
        assertThat(KrakenStreamConnector.normalizeSymbol("BTCUSDT")).isEqualTo("BTCUSD");
        assertThat(KrakenStreamConnector.normalizeSymbol("BTCUSD")).isEqualTo("BTCUSD");
        assertThat(KrakenStreamConnector.normalizeSymbol("BTC/USD")).isEqualTo("BTCUSD");
        assertThat(KrakenStreamConnector.normalizeSymbol("XBT/USD")).isEqualTo("BTCUSD");
        assertThat(KrakenStreamConnector.normalizeSymbol("ETHUSDT")).isEqualTo("ETHUSD");
        assertThat(KrakenStreamConnector.normalizeSymbol("ETHUSD")).isEqualTo("ETHUSD");
        assertThat(KrakenStreamConnector.normalizeSymbol("SOLUSDT")).isEqualTo("SOLUSD");
        assertThat(KrakenStreamConnector.normalizeSymbol("USDTUSD")).isEqualTo("USDTUSD");
        assertThat(KrakenStreamConnector.normalizeSymbol("USDT/USD")).isEqualTo("USDTUSD");
        assertThat(KrakenStreamConnector.normalizeSymbol(null)).isEqualTo("BTCUSD");
        assertThat(KrakenStreamConnector.normalizeSymbol("   ")).isEqualTo("BTCUSD");

        assertThat(KrakenStreamConnector.toWsSymbol("BTC")).isEqualTo("BTC/USD");
        assertThat(KrakenStreamConnector.toWsSymbol("BTCUSDT")).isEqualTo("BTC/USD");
        assertThat(KrakenStreamConnector.toWsSymbol("BTCUSD")).isEqualTo("BTC/USD");
        assertThat(KrakenStreamConnector.toWsSymbol("XBT/USD")).isEqualTo("BTC/USD");
        assertThat(KrakenStreamConnector.toWsSymbol("ETHUSDT")).isEqualTo("ETH/USD");
        assertThat(KrakenStreamConnector.toWsSymbol("ETHUSD")).isEqualTo("ETH/USD");
        assertThat(KrakenStreamConnector.toWsSymbol("USDTUSD")).isEqualTo("USDT/USD");
        assertThat(KrakenStreamConnector.toWsSymbol("USDT/USD")).isEqualTo("USDT/USD");
    }

    @Test
    void shouldConnectBothUsdtUsdAndSymbolStreamsAndDeliverOnlyTradeUpdates() throws Exception {
        List<String> subscriptionsReceived = new CopyOnWriteArrayList<>();

        server = vertx.createHttpServer()
                .webSocketHandler(ws -> {
                    ws.writeTextMessage("{\"channel\":\"status\",\"data\":[{\"api_version\":\"v2\",\"system\":\"online\"}],\"type\":\"update\"}");
                    ws.textMessageHandler(msg -> {
                        subscriptionsReceived.add(msg);
                        if (msg.contains("\"USDT/USD\"")) {
                            ws.writeTextMessage("{\"method\":\"subscribe\",\"result\":{\"channel\":\"trade\",\"snapshot\":false,\"symbol\":\"USDT/USD\"},\"success\":true}");
                            ws.writeTextMessage("{\"channel\":\"heartbeat\"}");
                            ws.writeTextMessage("{\"channel\":\"trade\",\"type\":\"snapshot\",\"data\":[{\"symbol\":\"USDT/USD\",\"side\":\"buy\",\"price\":0.9990,\"qty\":100.0,\"ord_type\":\"limit\",\"trade_id\":10,\"timestamp\":\"2026-11-13T00:00:00.000000Z\"}]}");
                            ws.writeTextMessage("{\"channel\":\"trade\",\"type\":\"update\",\"data\":[{\"symbol\":\"USDT/USD\",\"side\":\"sell\",\"price\":0.99879,\"qty\":99.0099,\"ord_type\":\"limit\",\"trade_id\":85674723,\"timestamp\":\"2026-11-13T00:00:00.589415Z\"}]}");
                        } else if (msg.contains("\"BTC/USD\"")) {
                            ws.writeTextMessage("{\"method\":\"subscribe\",\"result\":{\"channel\":\"trade\",\"snapshot\":false,\"symbol\":\"BTC/USD\"},\"success\":true}");
                            ws.writeTextMessage("{\"channel\":\"trade\",\"type\":\"update\",\"data\":[{\"symbol\":\"BTC/USD\",\"side\":\"buy\",\"price\":71370.7,\"qty\":0.00013941,\"ord_type\":\"limit\",\"trade_id\":115529595,\"timestamp\":\"2026-11-13T00:00:00.127740Z\"}]}");
                        }
                    });
                });

        int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
        String wsUrl = "ws://localhost:" + port + "/v2";

        KrakenStreamConnector connector = new KrakenStreamConnector(marketWsRuntime, wsUrl, 200L, 5000L);

        Map<String, List<String>> receivedBySymbol = new ConcurrentHashMap<>();
        Map<String, Long> timestampsBySymbol = new ConcurrentHashMap<>();

        connector.start("BTCUSDT", (symbol, rawJson, recvTs) -> {
            receivedBySymbol.computeIfAbsent(symbol, k -> new CopyOnWriteArrayList<>()).add(rawJson);
            timestampsBySymbol.put(symbol, recvTs);
        });

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(subscriptionsReceived).hasSize(2);
                    assertThat(subscriptionsReceived).anyMatch(m -> m.contains("\"channel\":\"trade\"") && m.contains("\"USDT/USD\"") && m.contains("\"snapshot\":false"));
                    assertThat(subscriptionsReceived).anyMatch(m -> m.contains("\"channel\":\"trade\"") && m.contains("\"BTC/USD\"") && m.contains("\"snapshot\":false"));

                    assertThat(receivedBySymbol).containsOnlyKeys("USDTUSD", "BTCUSD");
                    assertThat(receivedBySymbol.get("USDTUSD")).hasSize(1);
                    assertThat(receivedBySymbol.get("USDTUSD").getFirst()).contains("\"trade_id\":85674723");
                    assertThat(receivedBySymbol.get("BTCUSD")).hasSize(1);
                    assertThat(receivedBySymbol.get("BTCUSD").getFirst()).contains("\"trade_id\":115529595");

                    assertThat(timestampsBySymbol.values()).allMatch(ts -> ts > 0L);
                    assertThat(connector.isAllConnected()).isTrue();
                    assertThat(connector.getConnectionStates())
                            .containsEntry("USDTUSD", ConnectionState.CONNECTED)
                            .containsEntry("BTCUSD", ConnectionState.CONNECTED);
                    assertThat(connector.getIoReaderThread()).isNotNull();
                    assertThat(connector.getIoReaderThread().getName())
                            .isEqualTo(MarketWebSocketRuntime.DEDICATED_MARKET_IO_THREAD_NAME);
                    assertThat(connector.getIoReaderThread().getPriority()).isEqualTo(Thread.MAX_PRIORITY);
                });

        assertThat(connector.getStreamTelemetries()).hasSize(2);
        connector.destroy();
        assertThat(connector.isAllConnected()).isFalse();
    }

    @Test
    void shouldConnectOnlySymbolStreamWhenIncludeUsdtUsdIsFalse() throws Exception {
        List<String> subscriptionsReceived = new CopyOnWriteArrayList<>();

        server = vertx.createHttpServer()
                .webSocketHandler(ws -> ws.textMessageHandler(msg -> {
                    subscriptionsReceived.add(msg);
                    if (msg.contains("\"ETH/USD\"")) {
                        ws.writeTextMessage("{\"channel\":\"trade\",\"type\":\"update\",\"data\":[{\"symbol\":\"ETH/USD\",\"side\":\"buy\",\"price\":3120.5,\"qty\":1.25,\"ord_type\":\"market\",\"trade_id\":9001,\"timestamp\":\"2026-11-13T00:00:01.000000Z\"}]}");
                    }
                }));

        int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
        String wsUrl = "ws://localhost:" + port + "/v2";

        KrakenStreamConnector connector = new KrakenStreamConnector(marketWsRuntime, wsUrl, 200L, 5000L);
        Map<String, String> receivedBySymbol = new ConcurrentHashMap<>();

        connector.start("ETHUSDT", false, (symbol, rawJson, recvTs) -> receivedBySymbol.put(symbol, rawJson));

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(subscriptionsReceived).hasSize(1);
                    assertThat(subscriptionsReceived.getFirst()).contains("\"ETH/USD\"");
                    assertThat(receivedBySymbol).containsOnlyKeys("ETHUSD");
                    assertThat(receivedBySymbol.get("ETHUSD")).contains("\"trade_id\":9001");
                    assertThat(connector.getConnectionStates()).containsOnlyKeys("ETHUSD");
                    assertThat(connector.isAllConnected()).isTrue();
                });

        connector.destroy();
    }

    @Test
    void shouldDeliverZeroDecodeBytePayloadsAndSupportExternalWorkerAndDisconnectListener() throws Exception {
        List<WebSocketStreamTelemetry> disconnectEvents = new CopyOnWriteArrayList<>();

        server = vertx.createHttpServer()
                .webSocketHandler(ws -> ws.textMessageHandler(msg -> {
                    if (msg.contains("\"USDT/USD\"")) {
                        ws.writeTextMessage("{\"channel\":\"trade\",\"type\":\"update\",\"data\":[{\"symbol\":\"USDT/USD\",\"side\":\"buy\",\"price\":1.0,\"qty\":50.0,\"ord_type\":\"limit\",\"trade_id\":1,\"timestamp\":\"2026-11-13T00:00:00.100000Z\"}]}");
                        vertx.setTimer(40L, id -> ws.close((short) 1013, "synthetic kraken restart"));
                    } else if (msg.contains("\"BTC/USD\"")) {
                        ws.writeTextMessage("{\"channel\":\"trade\",\"type\":\"update\",\"data\":[{\"symbol\":\"BTC/USD\",\"side\":\"sell\",\"price\":71400.0,\"qty\":0.5,\"ord_type\":\"market\",\"trade_id\":2,\"timestamp\":\"2026-11-13T00:00:00.200000Z\"}]}");
                    }
                }));

        int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
        String wsUrl = "ws://localhost:" + port + "/v2";

        KrakenStreamConnector connector = new KrakenStreamConnector(marketWsRuntime, wsUrl, 200L, 0L);
        connector.setDisconnectListener(disconnectEvents::add);

        Map<String, String> decodedFromBytes = new ConcurrentHashMap<>();
        AtomicBoolean workerRunning = new AtomicBoolean(true);

        Thread externalWorker = Thread.ofPlatform()
                .daemon(true)
                .name("test-kraken-ext-worker")
                .unstarted(() -> {
                    List<TimestampedFrame> scratch = new ArrayList<>(64);
                    while (workerRunning.get()) {
                        int drained = connector.drainStreams(scratch);
                        if (drained > 0) {
                            continue;
                        }
                        LockSupport.parkNanos(connector, 15_000_000L);
                    }
                });

        KrakenStreamListener byteListener = new KrakenStreamListener() {
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
        connector.start("BTCUSD", true, byteListener, externalWorker);

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(decodedFromBytes).containsOnlyKeys("USDTUSD", "BTCUSD");
                    assertThat(disconnectEvents).isNotEmpty();
                    assertThat(disconnectEvents.getFirst().lastCloseCode()).isEqualTo(1013);
                });

        workerRunning.set(false);
        LockSupport.unpark(externalWorker);
        connector.destroy();
    }
}
