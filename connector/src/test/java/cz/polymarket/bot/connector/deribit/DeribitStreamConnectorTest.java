package cz.polymarket.bot.connector.deribit;

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
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link DeribitStreamConnector} verifying instrument normalization, JSON-RPC subscription,
 * heartbeat test-request response, and synthetic trade event delivery with local receipt timestamps.
 */
class DeribitStreamConnectorTest {

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
    void shouldNormalizeInstrumentsCorrectly() {
        assertThat(DeribitStreamConnector.normalizeInstrument("BTC")).isEqualTo("BTC-PERPETUAL");
        assertThat(DeribitStreamConnector.normalizeInstrument("BTCUSDT")).isEqualTo("BTC-PERPETUAL");
        assertThat(DeribitStreamConnector.normalizeInstrument("BTC-PERPETUAL")).isEqualTo("BTC-PERPETUAL");
        assertThat(DeribitStreamConnector.normalizeInstrument("ETHUSDT")).isEqualTo("ETH-PERPETUAL");
        assertThat(DeribitStreamConnector.normalizeInstrument("ETH")).isEqualTo("ETH-PERPETUAL");
        assertThat(DeribitStreamConnector.normalizeInstrument("SOLUSDT")).isEqualTo("SOL-PERPETUAL");
        assertThat(DeribitStreamConnector.normalizeInstrument(null)).isEqualTo("BTC-PERPETUAL");
        assertThat(DeribitStreamConnector.normalizeInstrument("   ")).isEqualTo("BTC-PERPETUAL");
    }

    @Test
    void shouldSubscribeAndReceiveTradePayloadsAndRespondToHeartbeat() throws Exception {
        List<String> clientMessages = new CopyOnWriteArrayList<>();
        AtomicBoolean testRequestReceived = new AtomicBoolean(false);

        server = vertx.createHttpServer()
                .webSocketHandler(ws -> {
                    ws.textMessageHandler(msg -> {
                        clientMessages.add(msg);
                        if (msg.contains("public/subscribe") && msg.contains("trades.BTC-PERPETUAL.100ms")) {
                            // Send subscription confirmation
                            ws.writeTextMessage("{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":[\"trades.BTC-PERPETUAL.100ms\"]}");
                            // Send heartbeat test_request
                            ws.writeTextMessage("{\"jsonrpc\":\"2.0\",\"method\":\"heartbeat\",\"params\":{\"type\":\"test_request\"}}");
                            // Send trade event
                            ws.writeTextMessage("""
                                    {"jsonrpc":"2.0","method":"subscription","params":{"channel":"trades.BTC-PERPETUAL.100ms","data":[{"trade_seq":101,"price":78450.5,"amount":100.0,"direction":"buy"}]}}
                                    """.strip());
                        } else if (msg.contains("public/test")) {
                            testRequestReceived.set(true);
                        }
                    });
                });

        int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
        DeribitStreamConnector connector = new DeribitStreamConnector(
                vertx,
                "ws://localhost:" + port + "/ws/api/v2",
                200L,
                5000L
        );

        List<String> receivedPayloads = new CopyOnWriteArrayList<>();
        List<Long> receivedTimestamps = new CopyOnWriteArrayList<>();

        connector.start("BTCUSDT", (rawJson, recvTs) -> {
            receivedPayloads.add(rawJson);
            receivedTimestamps.add(recvTs);
        });

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(connector.getState()).isEqualTo(ConnectionState.CONNECTED);
                    assertThat(connector.isConnected()).isTrue();
                    assertThat(connector.getActiveInstrument()).isEqualTo("BTC-PERPETUAL");
                    assertThat(clientMessages).anyMatch(m -> m.contains("public/subscribe") && m.contains("BTC-PERPETUAL"));
                    assertThat(testRequestReceived.get()).isTrue();
                    assertThat(receivedPayloads).hasSize(1);
                    assertThat(receivedPayloads.get(0)).contains("\"channel\":\"trades.BTC-PERPETUAL.100ms\"");
                    assertThat(receivedPayloads.get(0)).contains("78450.5");
                    assertThat(receivedTimestamps.get(0)).isPositive();
                });

        connector.stop();
        assertThat(connector.isConnected()).isFalse();
        assertThat(connector.getState()).isEqualTo(ConnectionState.DISCONNECTED);
    }
}
