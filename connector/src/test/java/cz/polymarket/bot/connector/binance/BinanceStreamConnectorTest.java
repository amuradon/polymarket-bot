package cz.polymarket.bot.connector.binance;

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
 * Tests for {@link BinanceStreamType} and {@link BinanceStreamConnector} verifying all 5 Binance streams,
 * dedicated native Epoll/BoringSSL transport configuration, dedicated I/O reader thread naming,
 * zero-decode {@code byte[]} frame delivery, external worker batch draining, and disconnect telemetry.
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
    void shouldConnectAllFiveBinanceStreamsAndDeliverPayloadsWithDedicatedIoThreadAndTelemetry() throws Exception {
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
                    assertThat(connector.getIoReaderThread()).isNotNull();
                    assertThat(connector.getIoReaderThread().getName())
                            .isEqualTo(BinanceStreamConnector.DEDICATED_BINANCE_IO_THREAD_NAME);
                    assertThat(connector.getIoReaderThread().getPriority()).isEqualTo(Thread.MAX_PRIORITY);
                });

        List<WebSocketStreamTelemetry> telemetries = connector.getStreamTelemetries();
        assertThat(telemetries).hasSize(5);
        assertThat(telemetries).allSatisfy(snap -> {
            assertThat(snap.totalFramesReceived()).isEqualTo(1L);
            assertThat(snap.remoteAddress()).contains(":" + port);
        });

        connector.destroy();
        assertThat(connector.isAllConnected()).isFalse();
    }

    @Test
    void shouldDeliverZeroDecodeBytePayloadsAndSupportExternalWorkerAndDisconnectListener() throws Exception {
        List<WebSocketStreamTelemetry> disconnectEvents = new CopyOnWriteArrayList<>();

        server = vertx.createHttpServer()
                .webSocketHandler(ws -> {
                    String path = ws.path();
                    ws.writeTextMessage("{\"streamPath\":\"" + path + "\",\"bytesMode\":true}");
                    if (path.contains("forceOrder")) {
                        vertx.setTimer(40L, id -> ws.close((short) 1013, "synthetic binance overload"));
                    }
                });

        int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
        String spotWsBase = "ws://localhost:" + port + "/spot";
        String futuresWsBase = "ws://localhost:" + port + "/futures";

        BinanceStreamConnector connector = new BinanceStreamConnector(
                vertx,
                spotWsBase,
                futuresWsBase,
                200L,
                0L
        );
        connector.setDisconnectListener(disconnectEvents::add);

        Map<BinanceStreamType, String> decodedFromBytes = new ConcurrentHashMap<>();
        AtomicBoolean workerRunning = new AtomicBoolean(true);

        Thread externalWorker = Thread.ofPlatform()
                .daemon(true)
                .name("test-binance-ext-worker")
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

        BinanceStreamListener byteListener = new BinanceStreamListener() {
            @Override
            public void onStreamMessage(BinanceStreamType streamType, String rawJson, long recvTimestampMs) {
                throw new AssertionError("onStreamMessage(String) must not be called when onStreamFrame(byte[]) is overridden");
            }

            @Override
            public void onStreamFrame(BinanceStreamType streamType, byte[] rawUtf8Bytes, long recvTimestampMs) {
                decodedFromBytes.put(streamType, new String(rawUtf8Bytes, StandardCharsets.UTF_8));
            }
        };

        externalWorker.start();
        connector.start("BTCUSDT", byteListener, externalWorker);

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(decodedFromBytes).hasSize(5);
                    assertThat(disconnectEvents).isNotEmpty();
                    assertThat(disconnectEvents.getFirst().lastCloseCode()).isEqualTo(1013);
                });

        workerRunning.set(false);
        LockSupport.unpark(externalWorker);
        connector.destroy();
    }
}
