package cz.polymarket.bot.connector.ws;

import io.netty.handler.ssl.OpenSsl;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.core.net.OpenSSLEngineOptions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

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

    @Test
    void shouldApplyDedicatedIoThreadName() throws Exception {
        server = vertx.createHttpServer()
                .webSocketHandler(ws -> ws.textMessageHandler(msg -> ws.writeTextMessage("{\"event_type\":\"book\"}")));

        int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
        String wsUrl = "ws://localhost:" + port + "/ws/dedicated-io-test";

        ReconnectingWebSocketStream stream = new ReconnectingWebSocketStream(
                vertx,
                "DEDICATED-IO-WS",
                wsUrl,
                () -> List.of("{\"action\":\"subscribe\"}"),
                null,
                batch -> {
                    for (TimestampedFrame frame : batch) {
                        assertThat(frame.rawUtf8Bytes()).isNotEmpty();
                        assertThat(frame.rawText()).isEqualTo("{\"event_type\":\"book\"}");
                    }
                },
                1000L,
                0L,
                null
        );
        stream.setDedicatedIoThreadName("clob-ws-io-reader");

        stream.start();

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    WebSocketStreamTelemetry snap = stream.getTelemetrySnapshot();
                    assertThat(snap.totalFramesReceived()).isEqualTo(1L);
                    assertThat(snap.reconnectCount()).isZero();
                    assertThat(stream.getIoReaderThread()).isNotNull();
                    assertThat(stream.getIoReaderThread().getName()).isEqualTo("clob-ws-io-reader");
                });

        stream.stop();
    }

    @Test
    void shouldFastReconnectWithinFiftyMillisecondsAndUseExponentialBackoffBeforeResetOnValidFrame() throws Exception {
        assertThat(ReconnectingWebSocketStream.computeNextReconnectDelayMs(1, 3000L)).isEqualTo(50L);
        assertThat(ReconnectingWebSocketStream.computeNextReconnectDelayMs(2, 3000L)).isEqualTo(100L);
        assertThat(ReconnectingWebSocketStream.computeNextReconnectDelayMs(3, 3000L)).isEqualTo(200L);
        assertThat(ReconnectingWebSocketStream.computeNextReconnectDelayMs(4, 3000L)).isEqualTo(400L);
        assertThat(ReconnectingWebSocketStream.computeNextReconnectDelayMs(7, 3000L)).isEqualTo(3000L);

        AtomicInteger connectionCount = new AtomicInteger(0);
        AtomicLong closeTimestampMs = new AtomicLong(0L);
        AtomicLong secondConnectTimestampMs = new AtomicLong(0L);
        List<String> receivedFrames = new CopyOnWriteArrayList<>();

        server = vertx.createHttpServer()
                .webSocketHandler(ws -> {
                    int attempt = connectionCount.incrementAndGet();
                    if (attempt == 1) {
                        ws.textMessageHandler(msg -> {
                            ws.writeTextMessage("{\"conn\":1}");
                            vertx.setTimer(20L, id -> {
                                closeTimestampMs.set(System.currentTimeMillis());
                                ws.close((short) 1013, "slow consumer: send buffer full");
                            });
                        });
                    } else {
                        secondConnectTimestampMs.compareAndSet(0L, System.currentTimeMillis());
                        ws.textMessageHandler(msg -> ws.writeTextMessage("{\"conn\":" + attempt + "}"));
                    }
                });

        int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
        String wsUrl = "ws://localhost:" + port + "/ws/fast-reconnect-test";

        // Configure max reconnectDelayMs = 3000 ms; first reconnect must still happen at ~50 ms
        ReconnectingWebSocketStream stream = new ReconnectingWebSocketStream(
                vertx,
                "FAST-RECONNECT-WS",
                wsUrl,
                () -> List.of("{\"action\":\"subscribe\"}"),
                (rawJson, recvTs) -> receivedFrames.add(rawJson),
                3000L,
                0L
        );

        stream.start();

        Awaitility.await()
                .atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> {
                    assertThat(connectionCount.get()).isGreaterThanOrEqualTo(2);
                    assertThat(receivedFrames).contains("{\"conn\":1}", "{\"conn\":2}");
                    assertThat(stream.getConsecutiveReconnectAttempts()).isZero();
                });

        long reconnectElapsedMs = secondConnectTimestampMs.get() - closeTimestampMs.get();
        assertThat(reconnectElapsedMs).isBetween(25L, 600L);

        stream.stop();
    }

    @Test
    void shouldNotOverrideReceiveBufferSizeAndShouldConfigureBoringSslWithWarnOnFallback() {
        ReconnectingWebSocketStream wssStream = new ReconnectingWebSocketStream(
                vertx,
                "WSS-CONFIG-TEST",
                "wss://localhost:8443/ws/market",
                List::of,
                (rawJson, recvTs) -> {
                },
                1000L,
                0L
        );

        HttpClientOptions clientOptions = wssStream.getHttpClientOptions();
        // Receive buffer size MUST stay at default (-1) so Vert.x does not force a 16 MB FixedRecvByteBufAllocator
        assertThat(clientOptions.getReceiveBufferSize()).isEqualTo(HttpClientOptions.DEFAULT_RECEIVE_BUFFER_SIZE);
        assertThat(clientOptions.isSsl()).isTrue();

        if (OpenSsl.isAvailable()) {
            assertThat(clientOptions.getSslEngineOptions()).isInstanceOf(OpenSSLEngineOptions.class);
        }

        // Verify fallback helper behavior when OpenSSL is unavailable
        HttpClientOptions fallbackOptions = new HttpClientOptions();
        boolean usedOpenSsl = ReconnectingWebSocketStream.configureSslEngineOptions(
                fallbackOptions,
                true,
                false,
                new RuntimeException("synthetic tcnative missing"),
                "FALLBACK-TEST-STREAM"
        );
        assertThat(usedOpenSsl).isFalse();
        assertThat(fallbackOptions.getSslEngineOptions()).isNull();

        wssStream.stop();
    }

    @Test
    void shouldWakeExternalWorkerAcrossBurstsUsingTwoPhaseParkedProtocol() throws Exception {
        int bursts = 3;
        int messagesPerBurst = 20;
        List<String> drainedPayloads = new CopyOnWriteArrayList<>();
        AtomicBoolean workerRunning = new AtomicBoolean(true);

        server = vertx.createHttpServer()
                .webSocketHandler(ws -> ws.textMessageHandler(msg -> {
                    for (int b = 0; b < bursts; b++) {
                        int burstIdx = b;
                        vertx.setTimer(b * 40L + 5L, id -> {
                            for (int m = 0; m < messagesPerBurst; m++) {
                                ws.writeTextMessage("{\"burst\":" + burstIdx + ",\"msg\":" + m + "}");
                            }
                        });
                    }
                }));

        int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
        String wsUrl = "ws://localhost:" + port + "/ws/ext-worker-test";

        ReconnectingWebSocketStream stream = new ReconnectingWebSocketStream(
                vertx,
                "EXT-WORKER-WS",
                wsUrl,
                () -> List.of("{\"action\":\"subscribe\"}"),
                null,
                batch -> {
                    for (TimestampedFrame frame : batch) {
                        drainedPayloads.add(frame.rawText());
                    }
                },
                1000L,
                0L,
                null
        );

        Thread externalWorker = Thread.ofPlatform()
                .daemon(true)
                .name("test-ext-clob-worker")
                .unstarted(() -> {
                    List<TimestampedFrame> scratch = new ArrayList<>(64);
                    while (workerRunning.get()) {
                        int drained = stream.drainAvailableBatch(scratch);
                        if (drained > 0) {
                            continue;
                        }
                        LockSupport.parkNanos(stream, 20_000_000L);
                    }
                });

        stream.bindExternalWorker(externalWorker);
        externalWorker.start();
        stream.start();

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(drainedPayloads).hasSize(bursts * messagesPerBurst);
                    assertThat(stream.getTelemetrySnapshot().queueDepth()).isZero();
                });

        workerRunning.set(false);
        LockSupport.unpark(externalWorker);
        stream.stop();
    }

    @Test
    void shouldInvokeDisconnectListenerWithStreamTelemetryAndNotOverwriteCloseCodeWithMinusOne() throws Exception {
        List<WebSocketStreamTelemetry> disconnectSnapshots = new CopyOnWriteArrayList<>();
        AtomicInteger connAttempt = new AtomicInteger(0);

        server = vertx.createHttpServer()
                .webSocketHandler(ws -> {
                    int attempt = connAttempt.incrementAndGet();
                    ws.textMessageHandler(msg -> {
                        ws.writeTextMessage("{\"frame\":" + attempt + "}");
                        if (attempt == 1) {
                            vertx.setTimer(30L, id -> ws.close((short) 1013, "slow consumer: send buffer full"));
                        } else if (attempt == 2) {
                            vertx.setTimer(30L, id -> ws.close());
                        }
                    });
                });

        int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
        String wsUrl = "ws://localhost:" + port + "/ws/disconnect-listener-test";

        ReconnectingWebSocketStream stream = new ReconnectingWebSocketStream(
                vertx,
                "DISCONNECT-LISTENER-WS",
                wsUrl,
                () -> List.of("{\"action\":\"subscribe\"}"),
                (rawJson, recvTs) -> {
                },
                80L,
                0L
        );
        stream.setDisconnectListener(disconnectSnapshots::add);

        stream.start();

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(connAttempt.get()).isGreaterThanOrEqualTo(3);
                    assertThat(disconnectSnapshots).hasSizeGreaterThanOrEqualTo(2);
                });

        WebSocketStreamTelemetry firstDisconnect = disconnectSnapshots.getFirst();
        assertThat(firstDisconnect.streamName()).isEqualTo("DISCONNECT-LISTENER-WS");
        assertThat(firstDisconnect.lastCloseCode()).isEqualTo(1013);
        assertThat(firstDisconnect.lastCloseReason()).isEqualTo("slow consumer: send buffer full");
        assertThat(firstDisconnect.totalFramesReceived()).isGreaterThanOrEqualTo(1L);

        stream.stop();
    }

    @Test
    void shouldDeliverTimestampedFrameDirectlyViaOnMessageBytesWithoutDecodingString() throws Exception {
        List<byte[]> receivedBytes = new CopyOnWriteArrayList<>();
        List<Long> receivedTimestamps = new CopyOnWriteArrayList<>();

        server = vertx.createHttpServer()
                .webSocketHandler(ws -> ws.textMessageHandler(msg -> ws.writeTextMessage("{\"raw_bytes\":true}")));

        int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
        String wsUrl = "ws://localhost:" + port + "/ws/raw-bytes-listener-test";

        RawWebSocketListener rawBytesListener = new RawWebSocketListener() {
            @Override
            public void onMessage(String rawJson, long recvTimestampMs) {
                throw new AssertionError("onMessage(String) must not be invoked when onMessageBytes is overridden");
            }

            @Override
            public void onMessageBytes(TimestampedFrame frame) {
                assertThat(frame.isTextDecoded()).isFalse();
                receivedBytes.add(frame.rawUtf8Bytes());
                receivedTimestamps.add(frame.recvTs());
            }
        };

        ReconnectingWebSocketStream stream = new ReconnectingWebSocketStream(
                vertx,
                "RAW-BYTES-LISTENER-WS",
                wsUrl,
                () -> List.of("{\"action\":\"subscribe\"}"),
                rawBytesListener,
                1000L,
                0L
        );

        stream.start();

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(receivedBytes).hasSize(1);
                    assertThat(new String(receivedBytes.getFirst(), java.nio.charset.StandardCharsets.UTF_8))
                            .isEqualTo("{\"raw_bytes\":true}");
                    assertThat(receivedTimestamps.getFirst()).isPositive();
                });

        stream.stop();
    }

    @Test
    void shouldDrainRemainingFramesAndInvokeNormalCloseListenerWithoutReconnectingOrWarnOnCloseCode1000() throws Exception {
        List<String> deliveredPayloads = new CopyOnWriteArrayList<>();
        List<WebSocketStreamTelemetry> unexpectedDisconnects = new CopyOnWriteArrayList<>();
        List<WebSocketStreamTelemetry> normalCloses = new CopyOnWriteArrayList<>();
        AtomicInteger connectionCount = new AtomicInteger(0);

        server = vertx.createHttpServer()
                .webSocketHandler(ws -> {
                    connectionCount.incrementAndGet();
                    ws.textMessageHandler(msg -> {
                        ws.writeTextMessage("{\"event_type\":\"book\",\"seq\":1}");
                        ws.writeTextMessage("{\"event_type\":\"market_resolved\",\"winning_outcome\":\"Up\"}");
                        ws.close((short) 1000, "all subscribed assets resolved");
                    });
                });

        int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
        String wsUrl = "ws://localhost:" + port + "/ws/normal-close-1000-test";

        ReconnectingWebSocketStream stream = new ReconnectingWebSocketStream(
                vertx,
                "NORMAL-CLOSE-1000-WS",
                wsUrl,
                () -> List.of("{\"action\":\"subscribe\"}"),
                (rawJson, recvTs) -> deliveredPayloads.add(rawJson),
                50L,
                0L
        );
        stream.setDisconnectListener(unexpectedDisconnects::add);
        stream.setNormalCloseListener(normalCloses::add);

        stream.start();

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(normalCloses).hasSize(1);
                    assertThat(deliveredPayloads).containsExactly(
                            "{\"event_type\":\"book\",\"seq\":1}",
                            "{\"event_type\":\"market_resolved\",\"winning_outcome\":\"Up\"}"
                    );
                    assertThat(stream.getState()).isEqualTo(ConnectionState.DISCONNECTED);
                });

        // Wait well past the 50ms reconnect delay to verify no reconnect is scheduled
        Thread.sleep(150L);
        assertThat(connectionCount.get()).isEqualTo(1);
        assertThat(unexpectedDisconnects).isEmpty();
        assertThat(stream.getReconnectCount()).isZero();

        WebSocketStreamTelemetry finalTelemetry = normalCloses.getFirst();
        assertThat(finalTelemetry.lastCloseCode()).isEqualTo(1000);
        assertThat(finalTelemetry.lastCloseReason()).isEqualTo("all subscribed assets resolved");
        assertThat(finalTelemetry.totalFramesReceived()).isEqualTo(2L);
        assertThat(finalTelemetry.queueDepth()).isZero();
    }
}
