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
}
