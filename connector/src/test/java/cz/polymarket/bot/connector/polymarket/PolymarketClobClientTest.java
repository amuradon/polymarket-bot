package cz.polymarket.bot.connector.polymarket;

import cz.polymarket.bot.connector.polymarket.auth.PolymarketCredentials;
import cz.polymarket.bot.connector.ws.ConnectionState;
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
}
