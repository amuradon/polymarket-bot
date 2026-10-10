package cz.polymarket.bot.connector.polymarket;

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
}
