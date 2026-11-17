package cz.polymarket.bot.connector.binance;

import cz.polymarket.bot.connector.ws.ConnectionState;
import cz.polymarket.bot.connector.ws.MarketWebSocketRuntime;
import cz.polymarket.bot.connector.ws.StreamRedundancyStatus;
import cz.polymarket.bot.connector.ws.TimestampedFrame;
import cz.polymarket.bot.connector.ws.WebSocketStreamTelemetry;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.ServerWebSocket;
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
 * Tests for {@link BinanceStreamType} and {@link BinanceStreamConnector} verifying all 4 Binance streams
 * ({@code SPOT_AGG_TRADES}, {@code SPOT_BOOK_TICKER}, {@code FUTURES_AGG_TRADES}, {@code FUTURES_BOOK_TICKER}),
 * dedicated native Epoll/BoringSSL transport configuration, dedicated I/O reader thread naming,
 * zero-decode {@code byte[]} frame delivery with host receipt timestamps, external worker batch draining,
 * and disconnect telemetry.
 */
class BinanceStreamConnectorTest {

    private Vertx vertx;
    private MarketWebSocketRuntime marketWsRuntime;
    private HttpServer server;

    @BeforeEach
    void setUp() {
        vertx = Vertx.vertx();
        marketWsRuntime = new MarketWebSocketRuntime();
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
    void shouldBuildExpectedStreamUrlsForSymbol() {
        String spotBase = "wss://stream.binance.com:9443/ws";
        String futuresBase = "wss://fstream.binance.com";

        assertThat(BinanceStreamType.SPOT_AGG_TRADES.buildWsUrl(spotBase, futuresBase, "BTCUSDT"))
                .isEqualTo("wss://stream.binance.com:9443/ws/btcusdt@aggTrade");
        assertThat(BinanceStreamType.SPOT_BOOK_TICKER.buildWsUrl(spotBase, futuresBase, "BTCUSDT"))
                .isEqualTo("wss://stream.binance.com:9443/ws/btcusdt@bookTicker");
        assertThat(BinanceStreamType.FUTURES_AGG_TRADES.buildWsUrl(spotBase, futuresBase, "BTCUSDT"))
                .isEqualTo("wss://fstream.binance.com/market/ws/btcusdt@aggTrade");
        assertThat(BinanceStreamType.FUTURES_BOOK_TICKER.buildWsUrl(spotBase, futuresBase, "BTCUSDT"))
                .isEqualTo("wss://fstream.binance.com/public/ws/btcusdt@bookTicker");
    }

    @Test
    void shouldConnectAllBinanceStreamsAndDeliverPayloadsWithDedicatedIoThreadAndTelemetry() throws Exception {
        server = vertx.createHttpServer()
                .webSocketHandler(ws -> {
                    String path = ws.path();
                    ws.writeTextMessage("{\"streamPath\":\"" + path + "\",\"synthetic\":true}");
                });

        int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
        String spotWsBase = "ws://localhost:" + port + "/spot";
        String futuresWsBase = "ws://localhost:" + port + "/futures";

        BinanceStreamConnector connector = new BinanceStreamConnector(
                marketWsRuntime,
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
                    assertThat(receivedByType).hasSize(4);
                    assertThat(receivedByType.get(BinanceStreamType.SPOT_AGG_TRADES))
                            .contains("/spot/btcusdt@aggTrade");
                    assertThat(receivedByType.get(BinanceStreamType.SPOT_BOOK_TICKER))
                            .contains("/spot/btcusdt@bookTicker");
                    assertThat(receivedByType.get(BinanceStreamType.FUTURES_AGG_TRADES))
                            .contains("/futures/market/ws/btcusdt@aggTrade");
                    assertThat(receivedByType.get(BinanceStreamType.FUTURES_BOOK_TICKER))
                            .contains("/futures/public/ws/btcusdt@bookTicker");
                    assertThat(timestampsByType).hasSize(4);
                    assertThat(timestampsByType.values()).allMatch(ts -> ts > 0L);
                    assertThat(connector.isAllConnected()).isTrue();
                    assertThat(connector.getIoReaderThread()).isNotNull();
                    assertThat(connector.getIoReaderThread().getName())
                            .isEqualTo(MarketWebSocketRuntime.DEDICATED_MARKET_IO_THREAD_NAME);
                    assertThat(connector.getIoReaderThread().getPriority()).isEqualTo(Thread.MAX_PRIORITY);
                });

        List<WebSocketStreamTelemetry> telemetries = connector.getStreamTelemetries();
        assertThat(telemetries).hasSize(4);
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
                    if (path.contains("futures/public/ws")) {
                        vertx.setTimer(40L, id -> ws.close((short) 1013, "synthetic binance overload"));
                    }
                });

        int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
        String spotWsBase = "ws://localhost:" + port + "/spot";
        String futuresWsBase = "ws://localhost:" + port + "/futures";

        BinanceStreamConnector connector = new BinanceStreamConnector(
                marketWsRuntime,
                spotWsBase,
                futuresWsBase,
                200L,
                0L
        );
        connector.setDisconnectListener(disconnectEvents::add);

        Map<BinanceStreamType, String> decodedFromBytes = new ConcurrentHashMap<>();
        Map<BinanceStreamType, Long> byteTimestampsByType = new ConcurrentHashMap<>();
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
                byteTimestampsByType.put(streamType, recvTimestampMs);
            }
        };

        externalWorker.start();
        connector.start("BTCUSDT", java.util.EnumSet.allOf(BinanceStreamType.class), byteListener, externalWorker);

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(decodedFromBytes).hasSize(4);
                    assertThat(byteTimestampsByType).hasSize(4);
                    assertThat(byteTimestampsByType.values()).allMatch(ts -> ts > 0L);
                    assertThat(disconnectEvents).isNotEmpty();
                    assertThat(disconnectEvents.getFirst().lastCloseCode()).isEqualTo(1013);
                });

        workerRunning.set(false);
        LockSupport.unpark(externalWorker);
        connector.destroy();
    }

    @Test
    void shouldFastForwardDualSpotAggTradesLegsAndDropDelayedOutOfOrderRetransmissions() throws Exception {
        List<ServerWebSocket> aggTradeSockets = new CopyOnWriteArrayList<>();

        server = vertx.createHttpServer()
                .webSocketHandler(ws -> {
                    if ("/spot/btcusdt@aggTrade".equals(ws.path())) {
                        aggTradeSockets.add(ws);
                        if (aggTradeSockets.size() == 2) {
                            ServerWebSocket fastLeg = aggTradeSockets.get(0);
                            ServerWebSocket slowRetransmittingLeg = aggTradeSockets.get(1);
                            fastLeg.writeTextMessage("{\"e\":\"aggTrade\",\"E\":1000,\"s\":\"BTCUSDT\",\"a\":1002,\"p\":\"85020.00\",\"q\":\"0.50\",\"m\":false}");
                            vertx.setTimer(30L, id -> {
                                slowRetransmittingLeg.writeTextMessage("{\"e\":\"aggTrade\",\"E\":999,\"s\":\"BTCUSDT\",\"a\":1001,\"p\":\"85000.00\",\"q\":\"0.25\",\"m\":true}");
                                slowRetransmittingLeg.writeTextMessage("{\"e\":\"aggTrade\",\"E\":1000,\"s\":\"BTCUSDT\",\"a\":1002,\"p\":\"85020.00\",\"q\":\"0.50\",\"m\":false}");
                                slowRetransmittingLeg.writeTextMessage("{\"e\":\"aggTrade\",\"E\":1005,\"s\":\"BTCUSDT\",\"a\":1003,\"p\":\"85035.00\",\"q\":\"0.75\",\"m\":false}");
                            });
                        }
                    }
                });

        int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
        String spotWsBase = "ws://127.0.0.1:" + port + "/spot";
        String futuresWsBase = "ws://127.0.0.1:" + port + "/futures";

        BinanceStreamConnector connector = new BinanceStreamConnector(
                marketWsRuntime,
                spotWsBase,
                futuresWsBase,
                200L,
                5000L
        );

        List<Long> deliveredAggTradeIds = new CopyOnWriteArrayList<>();
        connector.startSpotAggTradesBatch("BTCUSDT", batch -> {
            for (TimestampedFrame frame : batch) {
                deliveredAggTradeIds.add(BinanceStreamConnector.extractAggTradeId(frame.rawUtf8Bytes()));
            }
        }, null);

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(aggTradeSockets).hasSize(2);
                    assertThat(deliveredAggTradeIds).containsExactly(1002L, 1003L);
                    assertThat(connector.getConnectionStates().get(BinanceStreamType.SPOT_AGG_TRADES))
                            .isEqualTo(ConnectionState.CONNECTED);
                    assertThat(connector.getStreamTelemetries()).hasSize(2);
                });

        long coalescedTotal = connector.getStreamTelemetries().stream()
                .mapToLong(WebSocketStreamTelemetry::coalescedFramesCount)
                .sum();
        assertThat(coalescedTotal).isGreaterThanOrEqualTo(2L);

        StreamRedundancyStatus redundancy =
                connector.describeStreamRedundancy("BINANCE-SPOT_AGG_TRADES-BTCUSDT");
        assertThat(redundancy.dualStream()).isTrue();
        assertThat(redundancy.peerStreamName()).isEqualTo("BINANCE-SPOT_AGG_TRADES-BTCUSDT-BACKUP");
        assertThat(redundancy.failoverActive()).isTrue();

        connector.destroy();
        assertThat(connector.isAllConnected()).isFalse();
    }
}
