package cz.polymarket.bot.connector.polymarket;

import cz.polymarket.bot.connector.polymarket.auth.PolymarketCredentials;
import cz.polymarket.bot.connector.ws.ConnectionState;
import cz.polymarket.bot.connector.ws.MarketWebSocketRuntime;
import cz.polymarket.bot.connector.ws.RawWebSocketListener;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link PolymarketClobClient} verifying isolated single-leg collector streams,
 * dual Active-Active deduplicated strategy streams, zero collector duplication, and clean
 * strategy stream shutdown in {@code OFF} mode without disturbing active collector recording.
 */
class PolymarketClobClientTest {

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
    void shouldOpenAndCloseCollectorMarketStreamDeliveringRawMessages() throws Exception {
        List<String> receivedSubMessages = new CopyOnWriteArrayList<>();

        server = vertx.createHttpServer()
                .webSocketHandler(ws -> ws.textMessageHandler(msg -> {
                    receivedSubMessages.add(msg);
                    if (msg.contains("token_up_1")) {
                        ws.writeTextMessage("""
                                [{"event_type":"book","asset_id":"token_up_1","bids":[{"price":"0.49","size":"100"}],"asks":[{"price":"0.51","size":"100"}]}]
                                """.strip());
                        ws.writeTextMessage("""
                                {"event_type":"price_change","price_changes":[{"asset_id":"token_up_1","price":"0.50","size":"50","side":"BUY"}]}
                                """.strip());
                        ws.writeTextMessage("""
                                {"event_type":"last_trade_price","asset_id":"token_down_1","price":"0.50","size":"25","side":"BUY"}
                                """.strip());
                    }
                }));

        int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
        PolymarketClobClient client = new PolymarketClobClient(
                vertx,
                marketWsRuntime,
                "ws://localhost:" + port + "/ws/market",
                200L,
                5000L
        );

        String slug = "btc-updown-15m-1789041600";
        List<String> rawEvents = new CopyOnWriteArrayList<>();
        client.openCollectorMarketStream(
                slug,
                List.of("token_up_1", "token_down_1"),
                (rawJson, recvTs) -> rawEvents.add(rawJson),
                null,
                null
        );

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(receivedSubMessages).isNotEmpty();
                    assertThat(client.getCollectorConnectionStates()).containsEntry(slug, ConnectionState.CONNECTED);
                    assertThat(rawEvents).hasSize(3);
                    assertThat(rawEvents.get(0)).contains("\"event_type\":\"book\"");
                    assertThat(rawEvents.get(1)).contains("\"event_type\":\"price_change\"");
                    assertThat(rawEvents.get(2)).contains("\"event_type\":\"last_trade_price\"");
                });

        client.closeCollectorMarketStream(slug);
        assertThat(client.getCollectorConnectionStates()).doesNotContainKey(slug);
        client.stopAll();
    }

    @Test
    void shouldCloseCollectorStreamCleanlyAndInvokeNormalCloseListenerWhenServerClosesWithCode1000() throws Exception {
        AtomicInteger connectionCount = new AtomicInteger(0);
        server = vertx.createHttpServer()
                .webSocketHandler(ws -> {
                    connectionCount.incrementAndGet();
                    ws.textMessageHandler(msg -> {
                        if (msg.contains("token_up_1")) {
                            ws.writeTextMessage("""
                                    [{"event_type":"book","asset_id":"token_up_1","bids":[{"price":"0.49","size":"100"}]}]
                                    """.strip());
                            ws.writeTextMessage("""
                                    {"event_type":"market_resolved","winning_asset_id":"token_up_1","winning_outcome":"Up"}
                                    """.strip());
                            ws.close((short) 1000, "all subscribed assets resolved");
                        }
                    });
                });

        int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
        PolymarketClobClient client = new PolymarketClobClient(
                vertx,
                marketWsRuntime,
                "ws://localhost:" + port + "/ws/market",
                80L,
                5000L
        );

        List<WebSocketStreamTelemetry> unexpectedDisconnects = new CopyOnWriteArrayList<>();
        List<WebSocketStreamTelemetry> normalCloses = new CopyOnWriteArrayList<>();
        List<String> rawEvents = new CopyOnWriteArrayList<>();
        client.setDisconnectListener(unexpectedDisconnects::add);

        String slug = "btc-updown-15m-1789041600";
        client.openCollectorMarketStream(
                slug,
                List.of("token_up_1", "token_down_1"),
                (rawJson, recvTs) -> rawEvents.add(rawJson),
                normalCloses::add,
                null
        );

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(normalCloses).hasSize(1);
                    assertThat(rawEvents).hasSize(2);
                    assertThat(rawEvents.get(1)).contains("\"event_type\":\"market_resolved\"");
                    assertThat(client.getCollectorConnectionStates()).doesNotContainKey(slug);
                });

        Thread.sleep(150L);
        assertThat(connectionCount.get()).isEqualTo(1);
        assertThat(unexpectedDisconnects).isEmpty();
        assertThat(normalCloses.getFirst().lastCloseCode()).isEqualTo(1000);
        assertThat(normalCloses.getFirst().lastCloseReason()).isEqualTo("all subscribed assets resolved");

        client.stopAll();
    }

    @Test
    void shouldNotReconnectCollectorStreamAndInvokeCloseListenerWhenDisconnectedAfterMarketIntervalEnd() throws Exception {
        AtomicInteger connectionCount = new AtomicInteger(0);
        server = vertx.createHttpServer()
                .webSocketHandler(ws -> {
                    connectionCount.incrementAndGet();
                    ws.textMessageHandler(msg -> {
                        if (msg.contains("token_up_1")) {
                            ws.writeTextMessage("""
                                    [{"event_type":"book","asset_id":"token_up_1","bids":[{"price":"0.49","size":"100"}]}]
                                    """.strip());
                            ws.close((short) 1001, "going away after market end");
                        }
                    });
                });

        int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
        PolymarketClobClient client = new PolymarketClobClient(
                vertx,
                marketWsRuntime,
                "ws://localhost:" + port + "/ws/market",
                60L,
                5000L
        );

        List<WebSocketStreamTelemetry> unexpectedDisconnects = new CopyOnWriteArrayList<>();
        List<WebSocketStreamTelemetry> terminalCloses = new CopyOnWriteArrayList<>();
        List<String> rawEvents = new CopyOnWriteArrayList<>();
        client.setDisconnectListener(unexpectedDisconnects::add);

        String slug = "btc-updown-15m-1789041600";
        client.openCollectorMarketStream(
                slug,
                List.of("token_up_1", "token_down_1"),
                (rawJson, recvTs) -> rawEvents.add(rawJson),
                terminalCloses::add,
                () -> false
        );

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(terminalCloses).hasSize(1);
                    assertThat(rawEvents).hasSize(1);
                    assertThat(client.getCollectorConnectionStates()).doesNotContainKey(slug);
                });

        Thread.sleep(150L);
        assertThat(connectionCount.get()).isEqualTo(1);
        assertThat(unexpectedDisconnects).isEmpty();
        assertThat(terminalCloses.getFirst().lastCloseCode()).isEqualTo(1001);
        assertThat(terminalCloses.getFirst().lastCloseReason()).isEqualTo("going away after market end");

        client.stopAll();
    }

    @Test
    void shouldIsolateCollectorSingleStreamFromStrategyDualStreamWithZeroDuplicationAndZeroQueueFillWhenStrategyStopped() throws Exception {
        List<ServerWebSocket> serverSockets = new CopyOnWriteArrayList<>();

        server = vertx.createHttpServer()
                .webSocketHandler(ws -> {
                    serverSockets.add(ws);
                    ws.textMessageHandler(msg -> {
                        if (msg.contains("token_up_1")) {
                            ws.writeTextMessage("{\"event_type\":\"book\",\"seq\":1}");
                        }
                    });
                });

        int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
        PolymarketClobClient client = new PolymarketClobClient(
                vertx,
                marketWsRuntime,
                "ws://localhost:" + port + "/ws/market",
                200L,
                0L
        );

        String slug = "btc-updown-15m-1795000000";
        List<String> collectorPayloads = new CopyOnWriteArrayList<>();
        List<String> strategyPayloads = new CopyOnWriteArrayList<>();

        // 1. Collector opens single-leg lookahead stream first
        client.openCollectorMarketStream(
                slug,
                List.of("token_up_1", "token_down_1"),
                new RawWebSocketListener() {
                    @Override
                    public void onMessage(String rawJson, long recvTs) {
                        collectorPayloads.add(rawJson);
                    }

                    @Override
                    public void onMessageBytes(TimestampedFrame frame) {
                        collectorPayloads.add(new String(frame.rawUtf8Bytes(), StandardCharsets.UTF_8));
                    }
                },
                null,
                null
        );

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(collectorPayloads).containsExactly("{\"event_type\":\"book\",\"seq\":1}");
                    assertThat(client.getCollectorConnectionStates()).containsEntry(slug, ConnectionState.CONNECTED);
                });

        // 2. Strategy opens dual Active-Active stream for the SAME slug without colliding or duplicating collector data
        Thread dummyStrategyWorker = Thread.ofPlatform().daemon(true).name("dummy-strat-worker").unstarted(() -> {});
        client.openMarketStreamBatch(
                slug,
                List.of("token_up_1", "token_down_1"),
                batch -> {
                    for (TimestampedFrame f : batch) {
                        strategyPayloads.add(f.rawText());
                    }
                },
                dummyStrategyWorker
        );

        List<TimestampedFrame> scratch = new ArrayList<>(16);
        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(serverSockets).hasSize(3); // 1 collector + 2 strategy (PRIMARY + BACKUP)
                    client.drainMarketStreams(scratch);
                    assertThat(strategyPayloads).containsExactly("{\"event_type\":\"book\",\"seq\":1}");
                    // Collector still has ONLY its single copy (never duplicated from strategy's dual legs)
                    assertThat(collectorPayloads).containsExactly("{\"event_type\":\"book\",\"seq\":1}");
                });

        // 3. Switch Strategy to OFF (stopAllStrategyStreams): collector stays CONNECTED, strategy queues remain empty
        client.stopAllStrategyStreams();
        assertThat(client.getConnectionStates()).isEmpty();
        assertThat(client.getCollectorConnectionStates()).containsEntry(slug, ConnectionState.CONNECTED);

        // Broadcast another update across all still-open server sockets
        for (ServerWebSocket ws : serverSockets) {
            if (!ws.isClosed()) {
                ws.writeTextMessage("{\"event_type\":\"price_change\",\"seq\":2}");
            }
        }

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(collectorPayloads).hasSize(2));

        // Strategy queues must be 100% empty while strategy is OFF
        assertThat(client.hasQueuedMarketFrames()).isFalse();
        assertThat(client.drainMarketStreams(scratch)).isZero();
        assertThat(strategyPayloads).hasSize(1);

        client.stopAll();
    }

    @Test
    void shouldOpenAndCloseAuthenticatedUserStreamDeliveringOrderAndTradeEvents() throws Exception {
        List<String> receivedUserSubMessages = new CopyOnWriteArrayList<>();

        server = vertx.createHttpServer()
                .webSocketHandler(ws -> {
                    if ("/ws/user".equals(ws.path())) {
                        ws.textMessageHandler(msg -> {
                            receivedUserSubMessages.add(msg);
                            if (msg.contains("\"type\":\"user\"") && msg.contains("\"apiKey\":\"api-key-1\"")) {
                                ws.writeTextMessage("""
                                        {"event_type":"trade","status":"MATCHED","id":"trade-1","market":"0xcond1"}
                                        """.strip());
                            }
                        });
                    }
                });

        int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
        PolymarketClobClient client = new PolymarketClobClient(
                vertx,
                marketWsRuntime,
                "ws://localhost:" + port + "/ws/market",
                200L,
                5000L
        );

        PolymarketCredentials credentials = new PolymarketCredentials(
                "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318",
                "api-key-1",
                "passphrase-1",
                "c2VjcmV0X2tleV9mb3JfdGVzdGluZ19wb2x5bWFya2V0X2htYWM=",
                "0x70997970C51812dc3A010C7d01b50e0d17dc79C8",
                "0x70997970C51812dc3A010C7d01b50e0d17dc79C8",
                "0x2c7536E3605D9C16a7a3D7b1898e529396a65c23"
        );

        List<String> userEvents = new CopyOnWriteArrayList<>();
        client.openUserStream(credentials, (rawJson, recvTs) -> userEvents.add(rawJson), null);

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(receivedUserSubMessages).isNotEmpty();
                    assertThat(client.getConnectionStates())
                            .containsEntry(PolymarketClobClient.USER_STREAM_KEY, ConnectionState.CONNECTED);
                    assertThat(userEvents).hasSize(1);
                    assertThat(userEvents.get(0)).contains("\"event_type\":\"trade\"");
                });

        client.closeUserStream();
        assertThat(client.getConnectionStates()).doesNotContainKey(PolymarketClobClient.USER_STREAM_KEY);
        client.stopAll();
    }

    @Test
    void shouldMaintainDualActiveActiveStreamsWithDeduplicationAndZeroInterruptionOnPrimaryDisconnect() throws Exception {
        List<ServerWebSocket> activeServerSockets = new CopyOnWriteArrayList<>();
        AtomicInteger subscribedSocketCount = new AtomicInteger(0);

        String duplicateFrame1 = """
                {"event_type":"price_change","price_changes":[{"asset_id":"token_up_1","price":"0.52","size":"100","side":"BUY"}],"timestamp":"1794221100100"}
                """.strip();
        String backupOnlyFrame2 = """
                {"event_type":"price_change","price_changes":[{"asset_id":"token_up_1","price":"0.54","size":"150","side":"BUY"}],"timestamp":"1794221100200"}
                """.strip();

        server = vertx.createHttpServer()
                .webSocketHandler(ws -> {
                    activeServerSockets.add(ws);
                    ws.textMessageHandler(msg -> {
                        if (msg.contains("token_up_1")) {
                            int count = subscribedSocketCount.incrementAndGet();
                            ws.writeTextMessage(duplicateFrame1);
                            if (count == 2) {
                                ServerWebSocket primaryWs = activeServerSockets.get(0);
                                ServerWebSocket backupWs = activeServerSockets.get(1);
                                vertx.setTimer(30L, id -> {
                                    primaryWs.close((short) 1013, "slow consumer: send buffer full");
                                    backupWs.writeTextMessage(backupOnlyFrame2);
                                });
                            }
                        }
                    });
                });

        int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
        PolymarketClobClient client = new PolymarketClobClient(
                vertx,
                marketWsRuntime,
                "ws://localhost:" + port + "/ws/market",
                3000L,
                0L
        );

        String slug = "btc-updown-15m-1794221100";
        List<String> deliveredUniquePayloads = new CopyOnWriteArrayList<>();
        client.openMarketStreamBatch(
                slug,
                List.of("token_up_1", "token_down_1"),
                batch -> {
                    for (TimestampedFrame frame : batch) {
                        deliveredUniquePayloads.add(frame.rawText());
                    }
                },
                null
        );

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(subscribedSocketCount.get()).isGreaterThanOrEqualTo(2);
                    assertThat(client.getConnectionStates()).containsEntry(slug, ConnectionState.CONNECTED);
                    assertThat(deliveredUniquePayloads).containsExactly(duplicateFrame1, backupOnlyFrame2);
                });

        List<WebSocketStreamTelemetry> telemetries = client.getStreamTelemetries();
        assertThat(telemetries).hasSize(2);
        long totalCoalescedOrDeduped = telemetries.stream()
                .mapToLong(WebSocketStreamTelemetry::coalescedFramesCount)
                .sum();
        assertThat(totalCoalescedOrDeduped).isGreaterThanOrEqualTo(1L);

        StreamRedundancyStatus primaryRedundancy =
                client.describeMarketStreamRedundancy("POLYMARKET-CLOB-" + slug);
        assertThat(primaryRedundancy.dualStream()).isTrue();
        assertThat(primaryRedundancy.peerStreamName()).isEqualTo("POLYMARKET-CLOB-" + slug + "-BACKUP");
        assertThat(primaryRedundancy.peerState()).isEqualTo(ConnectionState.CONNECTED);
        assertThat(primaryRedundancy.failoverActive()).isTrue();

        client.closeMarketStream(slug);
        client.stopAll();
    }

    @Test
    void shouldComputeDistinct64BitFingerprintsForShortAndLargePayloads() {
        byte[] empty = new byte[0];
        assertThat(PolymarketClobClient.PayloadDeduplicator.computeFingerprint(null)).isZero();
        assertThat(PolymarketClobClient.PayloadDeduplicator.computeFingerprint(empty)).isZero();

        byte[] shortA = "PING".getBytes(StandardCharsets.UTF_8);
        byte[] shortB = "PONG".getBytes(StandardCharsets.UTF_8);
        byte[] oddLen = "{\"event_type\":\"book\"}".getBytes(StandardCharsets.UTF_8);
        assertThat(PolymarketClobClient.PayloadDeduplicator.computeFingerprint(shortA))
                .isNotZero()
                .isNotEqualTo(PolymarketClobClient.PayloadDeduplicator.computeFingerprint(shortB));
        assertThat(PolymarketClobClient.PayloadDeduplicator.computeFingerprint(oddLen)).isNotZero();

        byte[] large1 = new byte[16_387];
        for (int i = 0; i < large1.length; i++) {
            large1[i] = (byte) ('0' + (i % 10));
        }
        byte[] large2 = large1.clone();
        large2[large2.length - 1] = 'X';
        byte[] large3 = large1.clone();
        large3[8_192] = 'Y';

        long fp1 = PolymarketClobClient.PayloadDeduplicator.computeFingerprint(large1);
        long fp2 = PolymarketClobClient.PayloadDeduplicator.computeFingerprint(large2);
        long fp3 = PolymarketClobClient.PayloadDeduplicator.computeFingerprint(large3);
        assertThat(fp1).isNotZero();
        assertThat(fp1).isNotEqualTo(fp2);
        assertThat(fp1).isNotEqualTo(fp3);
        assertThat(fp2).isNotEqualTo(fp3);
    }

    @Test
    void shouldFilterUniqueFramesInPlaceWithoutConcurrentBatchMutationWhenPrimaryAndBackupDrainSimultaneously() throws Exception {
        List<ServerWebSocket> activeServerSockets = new CopyOnWriteArrayList<>();
        AtomicInteger subscribedSocketCount = new AtomicInteger(0);

        server = vertx.createHttpServer()
                .webSocketHandler(ws -> {
                    activeServerSockets.add(ws);
                    ws.textMessageHandler(msg -> {
                        if (msg.contains("token_up_1")) {
                            int count = subscribedSocketCount.incrementAndGet();
                            if (count == 2) {
                                ServerWebSocket s0 = activeServerSockets.get(0);
                                ServerWebSocket s1 = activeServerSockets.get(1);
                                for (int i = 0; i < 20; i++) {
                                    String frame = "{\"event_type\":\"price_change\",\"seq\":" + i + "}";
                                    s0.writeTextMessage(frame);
                                    s1.writeTextMessage(frame);
                                }
                            }
                        }
                    });
                });

        int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
        PolymarketClobClient client = new PolymarketClobClient(
                vertx,
                marketWsRuntime,
                "ws://localhost:" + port + "/ws/market",
                3000L,
                0L
        );

        String slug = "btc-updown-15m-1794849300";
        List<String> deliveredPayloads = new CopyOnWriteArrayList<>();
        List<Throwable> callbackErrors = new CopyOnWriteArrayList<>();

        client.openMarketStreamBatch(
                slug,
                List.of("token_up_1", "token_down_1"),
                batch -> {
                    try {
                        int n = batch.size();
                        for (int i = 0; i < n; i++) {
                            if (i == 0 && n > 1) {
                                Thread.sleep(2L);
                            }
                            deliveredPayloads.add(batch.get(i).rawText());
                        }
                    } catch (Throwable t) {
                        callbackErrors.add(t);
                    }
                },
                null
        );

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(callbackErrors).isEmpty();
                    assertThat(deliveredPayloads).hasSize(20);
                });

        client.stopAll();
    }
}
