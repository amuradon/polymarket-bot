package cz.polymarket.bot.connector.polymarket;

import cz.polymarket.bot.connector.ws.ConnectionState;
import cz.polymarket.bot.connector.ws.MarketWebSocketRuntime;
import cz.polymarket.bot.connector.ws.TimestampedFrame;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.ServerWebSocket;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link PolymarketPolyBoltClient} verifying the mandatory PolyBolt v2 authentication handshake
 * ({@code op: "auth"} -&gt; {@code op: "authed"} -&gt; {@code op: "subscribe"}), concurrent collector + strategy
 * delivery, and zero strategy queue accumulation when the strategy consumer is detached ({@code OFF} mode).
 */
class PolymarketPolyBoltClientTest {

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
    void shouldDeliverToCollectorAndAttachableStrategyQueueAndKeepStrategyQueueEmptyWhenDetached() throws Exception {
        List<ServerWebSocket> twapSockets = new CopyOnWriteArrayList<>();
        List<ServerWebSocket> cryptoSockets = new CopyOnWriteArrayList<>();

        server = vertx.createHttpServer()
                .webSocketHandler(ws -> {
                    AtomicBoolean authenticated = new AtomicBoolean(false);
                    ws.textMessageHandler(msg -> {
                        if (msg.contains("\"op\":\"auth\"")
                                && msg.contains("\"apiKey\":\"test-api-key\"")
                                && msg.contains("\"secret\":\"test-secret\"")
                                && msg.contains("\"passphrase\":\"test-passphrase\"")) {
                            authenticated.set(true);
                            ws.writeTextMessage("{\"op\":\"authed\",\"rid\":\"a1\"}");
                        } else if (msg.contains("\"op\":\"ping\"")) {
                            ws.writeTextMessage("{\"op\":\"pong\"}");
                        } else if (msg.contains("\"op\":\"subscribe\"")) {
                            if (!authenticated.get()) {
                                ws.writeTextMessage("{\"op\":\"error\",\"code\":\"auth_required\",\"rid\":\"s1\"}");
                            } else if (msg.contains("\"channel\":\"price.crypto.twap\"")) {
                                twapSockets.add(ws);
                                ws.writeTextMessage("{\"op\":\"subscribed\",\"channel\":\"price.crypto.twap\",\"rid\":\"s1\"}");
                                ws.writeTextMessage("""
                                        {"v":1,"channel":"price.crypto.twap","seq":1,"ts":1789041601000,"payload":{"symbol":"btcusd","value":78455.00,"timestamp":1789041601000,"source":"chainlink","window_seconds":60}}
                                        """.strip());
                            } else if (msg.contains("\"channel\":\"price.crypto\"")) {
                                cryptoSockets.add(ws);
                                ws.writeTextMessage("{\"op\":\"subscribed\",\"channel\":\"price.crypto\",\"rid\":\"s1\"}");
                                ws.writeTextMessage("""
                                        {"v":1,"channel":"price.crypto","seq":1,"ts":1789041601000,"payload":{"symbol":"btcusd","value":78460.00,"timestamp":1789041601000,"source":"chainlink"}}
                                        """.strip());
                            }
                        }
                    });
                });

        int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
        PolymarketPolyBoltClient client = new PolymarketPolyBoltClient(
                marketWsRuntime,
                "ws://localhost:" + port + "/ws",
                Optional.of("test-api-key"),
                Optional.of("test-secret"),
                Optional.of("test-passphrase"),
                200L,
                5000L
        );

        List<String> collectorTwap = new CopyOnWriteArrayList<>();
        List<String> collectorCrypto = new CopyOnWriteArrayList<>();
        List<String> strategyFrames = new CopyOnWriteArrayList<>();

        Thread dummyStrategyWorker = Thread.ofPlatform().daemon(true).name("dummy-polybolt-strat").unstarted(() -> {});
        client.attachStrategyConsumer((rawJson, recvTs) -> strategyFrames.add(rawJson), dummyStrategyWorker);
        client.startCollectorStreams(
                "BTCUSDT",
                (rawJson, recvTs) -> collectorCrypto.add(rawJson),
                (rawJson, recvTs) -> collectorTwap.add(rawJson)
        );

        List<TimestampedFrame> scratch = new ArrayList<>(16);
        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(client.getTwapState()).isEqualTo(ConnectionState.CONNECTED);
                    assertThat(client.getCryptoPriceState()).isEqualTo(ConnectionState.CONNECTED);
                    assertThat(collectorTwap).hasSize(1);
                    assertThat(collectorCrypto).hasSize(1);
                    client.drainStreams(scratch);
                    assertThat(strategyFrames).hasSize(2);
                });

        // Switch Strategy to OFF (detachStrategyConsumer) while Collector stays active
        client.detachStrategyConsumer();
        assertThat(client.hasQueuedFrames()).isFalse();
        assertThat(client.getStrategyQueueDepth()).isZero();

        // Push more frames on both PolyBolt sockets
        for (int seq = 2; seq <= 5; seq++) {
            twapSockets.getFirst().writeTextMessage(
                    "{\"v\":1,\"channel\":\"price.crypto.twap\",\"seq\":" + seq + ",\"payload\":{\"symbol\":\"btcusd\",\"value\":78500.00}}");
            cryptoSockets.getFirst().writeTextMessage(
                    "{\"v\":1,\"channel\":\"price.crypto\",\"seq\":" + seq + ",\"payload\":{\"symbol\":\"btcusd\",\"value\":78510.00}}");
        }

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(collectorTwap).hasSize(5);
                    assertThat(collectorCrypto).hasSize(5);
                });

        // Verify strategy queue NEVER accumulated any frames while detached
        assertThat(client.hasQueuedFrames()).isFalse();
        assertThat(client.getStrategyQueueDepth()).isZero();
        assertThat(client.drainStreams(scratch)).isZero();
        assertThat(strategyFrames).hasSize(2);

        client.stop();
    }

    @Test
    void shouldFailFastWhenCredentialsAreMissing() {
        PolymarketPolyBoltClient clientWithoutCreds = new PolymarketPolyBoltClient(
                marketWsRuntime,
                "ws://localhost:9999/ws",
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                200L,
                5000L
        );

        assertThat(clientWithoutCreds.hasCredentials()).isFalse();
        assertThatThrownBy(() -> clientWithoutCreds.startCollectorStreams("BTCUSDT", (r, t) -> {}, (r, t) -> {}))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Missing required Polymarket CLOB API credentials");
    }
}
