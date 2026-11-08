package cz.polymarket.bot.connector.ws;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit and integration tests for {@link ReconnectingWebSocketStream} using a local synthetic Vert.x WebSocket server.
 */
class ReconnectingWebSocketStreamTest {

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
    void shouldConnectSubscribeReceiveRawJsonWithLocalTimestampAndReconnectOnDisconnect() throws Exception {
        List<String> serverReceivedSubMessages = new CopyOnWriteArrayList<>();
        List<String> clientReceivedPayloads = new CopyOnWriteArrayList<>();
        AtomicLong lastClientRecvTimestamp = new AtomicLong(0);
        AtomicInteger connectionCount = new AtomicInteger(0);
        AtomicBoolean closeFirstConnection = new AtomicBoolean(true);

        server = vertx.createHttpServer()
                .webSocketHandler(ws -> {
                    int connNum = connectionCount.incrementAndGet();
                    ws.textMessageHandler(msg -> {
                        serverReceivedSubMessages.add(msg);
                        ws.writeTextMessage("{\"synthetic_event\":" + connNum + "}");
                        if (closeFirstConnection.compareAndSet(true, false)) {
                            vertx.setTimer(50, id -> ws.close());
                        }
                    });
                });

        int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
        String wsUrl = "ws://localhost:" + port + "/ws/test";

        ReconnectingWebSocketStream stream = new ReconnectingWebSocketStream(
                vertx,
                "TEST-WS",
                wsUrl,
                () -> List.of("{\"action\":\"subscribe\"}"),
                (rawJson, recvTs) -> {
                    clientReceivedPayloads.add(rawJson);
                    lastClientRecvTimestamp.set(recvTs);
                },
                100L,
                5000L
        );

        long beforeStart = System.currentTimeMillis();
        stream.start();

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(connectionCount.get()).isGreaterThanOrEqualTo(2);
                    assertThat(serverReceivedSubMessages).hasSizeGreaterThanOrEqualTo(2);
                    assertThat(clientReceivedPayloads).contains("{\"synthetic_event\":1}", "{\"synthetic_event\":2}");
                    assertThat(lastClientRecvTimestamp.get()).isGreaterThanOrEqualTo(beforeStart);
                    assertThat(stream.getState()).isEqualTo(ConnectionState.CONNECTED);
                });

        stream.stop();
        assertThat(stream.getState()).isEqualTo(ConnectionState.DISCONNECTED);
    }

    @Test
    void shouldSendPeriodicTextPingAndInterceptPongResponse() throws Exception {
        List<String> serverReceivedMessages = new CopyOnWriteArrayList<>();
        List<String> clientReceivedPayloads = new CopyOnWriteArrayList<>();
        AtomicLong lastClientRecvTimestamp = new AtomicLong(0);

        server = vertx.createHttpServer()
                .webSocketHandler(ws -> {
                    ws.textMessageHandler(msg -> {
                        serverReceivedMessages.add(msg);
                        if ("PING".equals(msg)) {
                            ws.writeTextMessage("PONG");
                        } else {
                            ws.writeTextMessage("{\"data\":\"ok\"}");
                        }
                    });
                });

        int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
        String wsUrl = "ws://localhost:" + port + "/ws/ping-test";

        ReconnectingWebSocketStream stream = new ReconnectingWebSocketStream(
                vertx,
                "PING-TEST-WS",
                wsUrl,
                () -> List.of("{\"action\":\"subscribe\"}"),
                (rawJson, recvTs) -> {
                    clientReceivedPayloads.add(rawJson);
                    lastClientRecvTimestamp.set(recvTs);
                },
                1000L,
                50L,
                "PING"
        );

        stream.start();

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(serverReceivedMessages).contains("PING");
                    assertThat(clientReceivedPayloads).doesNotContain("PONG");
                    assertThat(clientReceivedPayloads).contains("{\"data\":\"ok\"}");
                    assertThat(stream.getLastMessageTimestampMs()).isGreaterThan(0L);
                });

        stream.stop();
        assertThat(stream.getState()).isEqualTo(ConnectionState.DISCONNECTED);
    }

    @Test
    void shouldDeliverBurstOfMessagesInOrderOffEventLoopEvenWithSlowConsumerListener() throws Exception {
        int burstSize = 500;
        List<Integer> deliveredIndices = new CopyOnWriteArrayList<>();
        AtomicBoolean ranOnEventLoop = new AtomicBoolean(false);

        server = vertx.createHttpServer()
                .webSocketHandler(ws -> ws.textMessageHandler(msg -> {
                    for (int i = 0; i < burstSize; i++) {
                        ws.writeTextMessage("{\"seq\":" + i + ",\"padding\":\"" + "x".repeat(256) + "\"}");
                    }
                }));

        int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
        String wsUrl = "ws://localhost:" + port + "/ws/burst-test";

        ReconnectingWebSocketStream stream = new ReconnectingWebSocketStream(
                vertx,
                "BURST-TEST-WS",
                wsUrl,
                () -> List.of("{\"action\":\"subscribe\"}"),
                (rawJson, recvTs) -> {
                    if (io.vertx.core.Context.isOnEventLoopThread()) {
                        ranOnEventLoop.set(true);
                    }
                    int startIdx = rawJson.indexOf("\"seq\":") + 6;
                    int endIdx = rawJson.indexOf(',', startIdx);
                    int seq = Integer.parseInt(rawJson.substring(startIdx, endIdx));
                    if (seq == 0) {
                        try {
                            Thread.sleep(25L);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    deliveredIndices.add(seq);
                },
                1000L,
                0L
        );

        stream.start();

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(deliveredIndices).hasSize(burstSize));

        assertThat(ranOnEventLoop.get()).isFalse();
        for (int i = 0; i < burstSize; i++) {
            assertThat(deliveredIndices.get(i)).isEqualTo(i);
        }
        assertThat(stream.getReconnectCount()).isZero();

        stream.stop();
        assertThat(stream.getState()).isEqualTo(ConnectionState.DISCONNECTED);
    }

    @Test
    void shouldReuseSinglePersistentDrainThreadAcrossSpacedMessages() throws Exception {
        int messageCount = 15;
        List<Long> observerThreadIds = new CopyOnWriteArrayList<>();
        List<String> receivedPayloads = new CopyOnWriteArrayList<>();

        server = vertx.createHttpServer()
                .webSocketHandler(ws -> ws.textMessageHandler(msg -> {
                    for (int i = 0; i < messageCount; i++) {
                        int idx = i;
                        vertx.setTimer((i + 1) * 10L, timerId -> {
                            if (!ws.isClosed()) {
                                ws.writeTextMessage("{\"spaced_seq\":" + idx + "}");
                            }
                        });
                    }
                }));

        int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
        String wsUrl = "ws://localhost:" + port + "/ws/persistent-thread-test";

        ReconnectingWebSocketStream stream = new ReconnectingWebSocketStream(
                vertx,
                "PERSISTENT-THREAD-WS",
                wsUrl,
                () -> List.of("{\"action\":\"subscribe\"}"),
                (rawJson, recvTs) -> {
                    observerThreadIds.add(Thread.currentThread().threadId());
                    receivedPayloads.add(rawJson);
                },
                1000L,
                0L
        );

        stream.start();

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(receivedPayloads).hasSize(messageCount));

        assertThat(observerThreadIds).isNotEmpty();
        assertThat(observerThreadIds.stream().distinct().count()).isEqualTo(1L);

        stream.stop();
        assertThat(stream.getState()).isEqualTo(ConnectionState.DISCONNECTED);
    }


    @Test
    void shouldCoalesceBatchesAndTrackTelemetryCounters() throws Exception {
        int burstSize = 50;
        List<Integer> batchSizes = new CopyOnWriteArrayList<>();

        server = vertx.createHttpServer()
                .webSocketHandler(ws -> ws.textMessageHandler(msg -> {
                    for (int i = 0; i < burstSize; i++) {
                        ws.writeTextMessage("{\"seq\":" + i + "}");
                    }
                }));

        int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
        String wsUrl = "ws://localhost:" + port + "/ws/batch-telemetry-test";

        ReconnectingWebSocketStream stream = new ReconnectingWebSocketStream(
                vertx,
                "BATCH-TELEMETRY-WS",
                wsUrl,
                () -> List.of("{\"action\":\"subscribe\"}"),
                null,
                batch -> {
                    if (batchSizes.isEmpty()) {
                        try {
                            Thread.sleep(15L);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    batchSizes.add(batch.size());
                },
                1000L,
                0L,
                null
        );

        stream.start();

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    int total = batchSizes.stream().mapToInt(Integer::intValue).sum();
                    assertThat(total).isEqualTo(burstSize);
                });

        WebSocketStreamTelemetry telemetry = stream.getTelemetrySnapshot();
        assertThat(telemetry.streamName()).isEqualTo("BATCH-TELEMETRY-WS");
        assertThat(telemetry.state()).isEqualTo(ConnectionState.CONNECTED);
        assertThat(telemetry.totalFramesReceived()).isEqualTo(burstSize);
        assertThat(telemetry.maxQueueDepth()).isGreaterThanOrEqualTo(1);
        assertThat(telemetry.queueDepth()).isZero();
        assertThat(telemetry.maxProcessTimeUs()).isGreaterThan(0L);
        assertThat(stream.getIoReaderThread()).isNotNull();

        stream.stop();
    }
}
