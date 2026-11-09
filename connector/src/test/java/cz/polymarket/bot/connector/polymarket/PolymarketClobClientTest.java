package cz.polymarket.bot.connector.polymarket;

import cz.polymarket.bot.connector.polymarket.auth.PolymarketCredentials;
import cz.polymarket.bot.connector.ws.ConnectionState;
import cz.polymarket.bot.connector.ws.TimestampedFrame;
import cz.polymarket.bot.connector.ws.WebSocketStreamTelemetry;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.ServerWebSocket;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link PolymarketClobClient} verifying per-market WebSocket stream subscription,
 * raw unparsed message delivery, and individual market stream disconnection.
 */
class PolymarketClobClientTest {

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
    void shouldOpenAndClosePerMarketStreamDeliveringRawMessages() throws Exception {
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
                "ws://localhost:" + port + "/ws/market",
                200L,
                5000L
        );

        String slug = "btc-updown-15m-1789041600";
        List<String> rawEvents = new CopyOnWriteArrayList<>();
        client.openMarketStream(slug, List.of("token_up_1", "token_down_1"), (rawJson, recvTs) -> rawEvents.add(rawJson));

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(receivedSubMessages).isNotEmpty();
                    assertThat(client.getConnectionStates()).containsEntry(slug, ConnectionState.CONNECTED);
                    assertThat(rawEvents).hasSize(3);
                    assertThat(rawEvents.get(0)).contains("\"event_type\":\"book\"");
                    assertThat(rawEvents.get(1)).contains("\"event_type\":\"price_change\"");
                    assertThat(rawEvents.get(2)).contains("\"event_type\":\"last_trade_price\"");
                });

        client.closeMarketStream(slug);
        assertThat(client.getConnectionStates()).doesNotContainKey(slug);
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
        client.openUserStream(credentials, (rawJson, recvTs) -> userEvents.add(rawJson));

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
                            // Both PRIMARY and BACKUP streams receive the exact same initial frame
                            ws.writeTextMessage(duplicateFrame1);
                            if (count == 2) {
                                // Simulate upstream 1013 disconnect on socket #1 while socket #2 keeps streaming
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
                    // Market state stays CONNECTED seamlessly via BACKUP stream
                    assertThat(client.getConnectionStates()).containsEntry(slug, ConnectionState.CONNECTED);
                    // Duplicate frame 1 must be delivered ONLY ONCE, followed by backupOnlyFrame2
                    assertThat(deliveredUniquePayloads).containsExactly(duplicateFrame1, backupOnlyFrame2);
                });

        List<WebSocketStreamTelemetry> telemetries = client.getStreamTelemetries();
        assertThat(telemetries).hasSize(2);
        long totalCoalescedOrDeduped = telemetries.stream()
                .mapToLong(WebSocketStreamTelemetry::coalescedFramesCount)
                .sum();
        assertThat(totalCoalescedOrDeduped).isGreaterThanOrEqualTo(1L);

        // Closing the market stream and opening a new market must not leak reconnectCount into the new market
        client.closeMarketStream(slug);
        String nextSlug = "btc-updown-15m-1794222000";
        client.openMarketStreamBatch(nextSlug, List.of("token_up_2", "token_down_2"), batch -> {
        }, null);

        for (WebSocketStreamTelemetry nextSnap : client.getStreamTelemetries()) {
            assertThat(nextSnap.reconnectCount()).isZero();
            assertThat(nextSnap.lastCloseCode()).isZero();
        }

        client.stopAll();
    }

    @Test
    void shouldComputeDistinct64BitFingerprintsForShortAndLargePayloads() {
        byte[] empty = new byte[0];
        assertThat(PolymarketClobClient.PayloadDeduplicator.computeFingerprint(null)).isZero();
        assertThat(PolymarketClobClient.PayloadDeduplicator.computeFingerprint(empty)).isZero();

        // Short payloads (< 8 bytes and non-multiple of 8 bytes)
        byte[] shortA = "PING".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] shortB = "PONG".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] oddLen = "{\"event_type\":\"book\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        assertThat(PolymarketClobClient.PayloadDeduplicator.computeFingerprint(shortA))
                .isNotZero()
                .isNotEqualTo(PolymarketClobClient.PayloadDeduplicator.computeFingerprint(shortB));
        assertThat(PolymarketClobClient.PayloadDeduplicator.computeFingerprint(oddLen)).isNotZero();

        // Large 16 KB orderbook snapshot payload with single-byte mutation near the tail
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
}
