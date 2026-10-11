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
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link PolymarketPolyBoltClient} verifying the mandatory PolyBolt v2 authentication handshake
 * ({@code op: "auth"} -> {@code op: "authed"} -> {@code op: "subscribe"}) for both {@code price.crypto.twap} (60s)
 * and {@code price.crypto} streams and unparsed raw message delivery.
 */
class PolymarketPolyBoltClientTest {

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
    void shouldAuthenticateAndSubscribeBothTwapAndCryptoPriceStreamsIndependently() throws Exception {
        List<String> clientMessages = new CopyOnWriteArrayList<>();

        server = vertx.createHttpServer()
                .webSocketHandler(ws -> {
                    AtomicBoolean authenticated = new AtomicBoolean(false);
                    ws.textMessageHandler(msg -> {
                        clientMessages.add(msg);
                        if (msg.contains("\"op\":\"auth\"")
                                && msg.contains("\"apiKey\":\"test-api-key\"")
                                && msg.contains("\"secret\":\"test-secret\"")
                                && msg.contains("\"passphrase\":\"test-passphrase\"")) {
                            authenticated.set(true);
                            ws.writeTextMessage("{\"op\":\"authed\",\"rid\":\"a1\"}");
                        } else if (msg.contains("\"op\":\"subscribe\"")) {
                            if (!authenticated.get()) {
                                ws.writeTextMessage("{\"op\":\"error\",\"code\":\"auth_required\",\"rid\":\"s1\"}");
                            } else if (msg.contains("\"channel\":\"price.crypto.twap\"")) {
                                ws.writeTextMessage("{\"op\":\"subscribed\",\"channel\":\"price.crypto.twap\",\"rid\":\"s1\"}");
                                ws.writeTextMessage("""
                                        {"v":1,"channel":"price.crypto.twap","seq":1,"ts":1789041601000,"payload":{"symbol":"btcusd","value":78455.00,"full_accuracy_value":"78455.00000000","timestamp":1789041601000,"source":"chainlink","window_seconds":60}}
                                        """.strip());
                            } else if (msg.contains("\"channel\":\"price.crypto\"")) {
                                ws.writeTextMessage("{\"op\":\"subscribed\",\"channel\":\"price.crypto\",\"rid\":\"s1\"}");
                                ws.writeTextMessage("""
                                        {"v":1,"channel":"price.crypto","seq":1,"ts":1789041601000,"payload":{"symbol":"btcusd","value":78460.00,"full_accuracy_value":"78460.00000000","timestamp":1789041601000,"source":"chainlink"}}
                                        """.strip());
                            }
                        }
                    });
                });

        int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
        PolymarketPolyBoltClient client = new PolymarketPolyBoltClient(
                vertx,
                "ws://localhost:" + port + "/ws",
                 java.util.Optional.of("test-api-key"),
                java.util.Optional.of("test-secret"),
                java.util.Optional.of("test-passphrase"),
                200L,
                5000L
        );

        List<String> twapPayloads = new CopyOnWriteArrayList<>();
        List<String> cryptoPayloads = new CopyOnWriteArrayList<>();
        client.startTwapStream("BTCUSDT", (rawJson, recvTs) -> twapPayloads.add(rawJson));
        client.startCryptoPriceStream("BTCUSDT", (rawJson, recvTs) -> cryptoPayloads.add(rawJson));

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(client.getTwapState()).isEqualTo(ConnectionState.CONNECTED);
                    assertThat(client.getCryptoPriceState()).isEqualTo(ConnectionState.CONNECTED);
                    assertThat(twapPayloads).anyMatch(p -> p.contains("\"channel\":\"price.crypto.twap\"") && p.contains("78455.00"));
                    assertThat(cryptoPayloads).anyMatch(p -> p.contains("\"channel\":\"price.crypto\"") && p.contains("78460.00"));
                });

        client.stop();
    }

    @Test
    void shouldFailFastWhenCredentialsAreMissing() {
        PolymarketPolyBoltClient clientWithoutCreds = new PolymarketPolyBoltClient(
                vertx,
                "ws://localhost:9999/ws",
                java.util.Optional.empty(),
                java.util.Optional.empty(),
                java.util.Optional.empty(),
                200L,
                5000L
        );

        assertThat(clientWithoutCreds.hasCredentials()).isFalse();
        org.assertj.core.api.Assertions.assertThatThrownBy(
                        () -> clientWithoutCreds.startTwapStream("BTCUSDT", (raw, ts) -> {})
                )
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Missing required Polymarket CLOB API credentials");

        org.assertj.core.api.Assertions.assertThatThrownBy(
                        () -> clientWithoutCreds.startCryptoPriceStream("BTCUSDT", (raw, ts) -> {})
                )
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Missing required Polymarket CLOB API credentials");
    }
}
