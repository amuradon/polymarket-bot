package cz.polymarket.bot;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.ServerWebSocket;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

/**
 * Quarkus test resource starting an embedded Vert.x server that mocks external streams used by the Data Collector:
 * <ul>
 *   <li>Binance Spot WebSocket ({@code @bookTicker})</li>
 *   <li>Binance Futures WebSocket ({@code @bookTicker})</li>
 *   <li>Polymarket Gamma HTTP API ({@code /gamma/markets/slug/...})</li>
 *   <li>Polymarket PolyBolt v2 WebSocket ({@code /polybolt/ws}) with mandatory {@code auth} -&gt; {@code authed} -&gt; {@code subscribe} handshake</li>
 *   <li>Polymarket CLOB WebSocket ({@code /clob/ws/market})</li>
 * </ul>
 */
public class MockMarketStreamsTestResource implements QuarkusTestResourceLifecycleManager {

    private Vertx vertx;
    private HttpServer server;
    private Path tempDataDir;

    @Override
    public Map<String, String> start() {
        try {
            tempDataDir = Files.createTempDirectory("polymarket-bot-it-");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        vertx = Vertx.vertx();
        server = vertx.createHttpServer()
                .requestHandler(req -> {
                    if ("websocket".equalsIgnoreCase(req.getHeader("Upgrade"))) {
                        req.toWebSocket().onSuccess(this::handleWebSocket);
                        return;
                    }
                    String reqPath = req.path();
                    String prefix = "/gamma/markets/slug/";
                    if (reqPath.startsWith(prefix) && reqPath.length() > prefix.length()) {
                        String slug = reqPath.substring(prefix.length());
                        if (!slug.isBlank()) {
                            req.response()
                                    .putHeader("Content-Type", "application/json")
                                    .end("""
                                            {
                                              "id": "m-%s",
                                              "conditionId": "0xcond_%s",
                                              "slug": "%s",
                                              "clobTokenIds": ["token_up_%s", "token_down_%s"],
                                              "closed": false
                                            }
                                            """.formatted(slug, slug, slug, slug, slug));
                            return;
                        }
                    }
                    req.response().setStatusCode(404).end();
                });

        try {
            int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
            return Map.ofEntries(
                    Map.entry("polymarket.collector.streams-enabled", "true"),
                    Map.entry("polymarket.collector.data-dir", tempDataDir.resolve("collector").toString()),
                    Map.entry("polymarket.runtime.state-file", tempDataDir.resolve("state/runtime-state.json").toString()),
                    Map.entry("polymarket.collector.lookahead-count", "1"),
                    Map.entry("polymarket.collector.flush-interval", "1s"),
                    Map.entry("polymarket.strategy.mode", "OFF"),
                    Map.entry("polymarket.connector.binance.spot-ws-base-url", "ws://localhost:" + port + "/binance/spot"),
                    Map.entry("polymarket.connector.binance.futures-ws-base-url", "ws://localhost:" + port + "/binance/futures"),
                    Map.entry("polymarket.connector.gamma.base-url", "http://localhost:" + port + "/gamma"),
                    Map.entry("polymarket.connector.polybolt.ws-url", "ws://localhost:" + port + "/polybolt/ws"),
                    Map.entry("polymarket.connector.clob.ws-url", "ws://localhost:" + port + "/clob/ws/market"),
                    Map.entry("polymarket.connector.api-key", "test-api-key"),
                    Map.entry("polymarket.connector.secret", "c2VjcmV0X2tleV9mb3JfdGVzdGluZ19wb2x5bWFya2V0X2htYWM="),
                    Map.entry("polymarket.connector.passphrase", "test-passphrase")
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to start MockMarketStreamsTestResource", e);
        }
    }

    private void handleWebSocket(ServerWebSocket ws) {
        String path = ws.path();
        if (path.startsWith("/binance/spot/") && path.endsWith("@bookTicker")) {
            ws.writeTextMessage("{\"u\":400900217,\"s\":\"BTCUSDT\",\"b\":\"78450.00\",\"B\":\"1.25\",\"a\":\"78450.10\",\"A\":\"0.80\"}");
        } else if (path.startsWith("/binance/futures/") && path.endsWith("@bookTicker")) {
            ws.writeTextMessage("{\"e\":\"bookTicker\",\"u\":400900218,\"s\":\"BTCUSDT\",\"b\":\"78450.50\",\"B\":\"2.50\",\"a\":\"78451.00\",\"A\":\"1.75\",\"T\":1789041600115,\"E\":1789041600116}");
        } else if (path.equals("/polybolt/ws")) {
            AtomicBoolean authed = new AtomicBoolean(false);
            ws.textMessageHandler(msg -> {
                if (msg.contains("\"op\":\"auth\"") && msg.contains("\"apiKey\":\"test-api-key\"")) {
                    authed.set(true);
                    ws.writeTextMessage("{\"op\":\"authed\",\"rid\":\"a1\"}");
                } else if (msg.contains("\"op\":\"subscribe\"")) {
                    if (!authed.get()) {
                        ws.writeTextMessage("{\"op\":\"error\",\"code\":\"auth_required\",\"rid\":\"s1\"}");
                    } else if (msg.contains("\"channel\":\"price.crypto.twap\"")) {
                        ws.writeTextMessage("{\"op\":\"subscribed\",\"channel\":\"price.crypto.twap\",\"rid\":\"s1\"}");
                        ws.writeTextMessage("{\"v\":1,\"channel\":\"price.crypto.twap\",\"seq\":1,\"ts\":1789041600200,\"payload\":{\"symbol\":\"btcusd\",\"value\":78450.5,\"full_accuracy_value\":\"78450.50000000\",\"timestamp\":1789041600200,\"source\":\"chainlink\",\"window_seconds\":60}}");
                    } else if (msg.contains("\"channel\":\"price.crypto\"")) {
                        ws.writeTextMessage("{\"op\":\"subscribed\",\"channel\":\"price.crypto\",\"rid\":\"s1\"}");
                        ws.writeTextMessage("{\"v\":1,\"channel\":\"price.crypto\",\"seq\":1,\"ts\":1789041600200,\"payload\":{\"symbol\":\"btcusd\",\"value\":78460.0,\"full_accuracy_value\":\"78460.00000000\",\"timestamp\":1789041600200,\"source\":\"chainlink\"}}");
                    }
                }
            });
        } else if (path.equals("/clob/ws/market")) {
            ws.textMessageHandler(msg -> {
                if (msg.contains("\"type\":\"market\"")) {
                    ws.writeTextMessage("[{\"event_type\":\"book\",\"bids\":[{\"price\":\"0.49\",\"size\":\"100\"}],\"asks\":[{\"price\":\"0.51\",\"size\":\"100\"}]}]");
                    ws.writeTextMessage("{\"event_type\":\"last_trade_price\",\"price\":\"0.50\",\"size\":\"25\",\"side\":\"BUY\"}");
                }
            });
        }
    }

    @Override
    public void stop() {
        try {
            if (server != null) {
                server.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
            }
            if (vertx != null) {
                vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
            }
        } catch (Exception ignored) {
        }
        if (tempDataDir != null && Files.exists(tempDataDir)) {
            try (Stream<Path> walk = Files.walk(tempDataDir)) {
                walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (IOException ignored) {
                    }
                });
            } catch (IOException ignored) {
            }
        }
    }
}
