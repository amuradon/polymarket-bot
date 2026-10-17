package cz.polymarket.bot.data;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;

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
 * Quarkus test resource starting an embedded Vert.x server that mocks all external streams:
 * <ul>
 *   <li>Binance Spot WebSocket ({@code @aggTrade})</li>
 *   <li>Binance Futures WebSockets ({@code @aggTrade}, {@code @depth@100ms}, {@code @depth20@100ms}, {@code @forceOrder})</li>
 *   <li>Polymarket Gamma HTTP API ({@code /gamma/markets?slug=...})</li>
 *   <li>Polymarket PolyBolt v2 WebSocket ({@code /polybolt/ws}) with mandatory {@code auth} -> {@code authed} -> {@code subscribe} handshake</li>
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
            tempDataDir = Files.createTempDirectory("polymarket-collector-it-");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        vertx = Vertx.vertx();
        server = vertx.createHttpServer()
                .requestHandler(req -> {
                    if (req.path().equals("/gamma/markets")) {
                        String slug = req.getParam("slug");
                        if (slug != null && !slug.isBlank()) {
                            req.response()
                                    .putHeader("Content-Type", "application/json")
                                    .end("""
                                            [
                                              {
                                                "id": "m-%s",
                                                "slug": "%s",
                                                "clobTokenIds": ["token_up_%s", "token_down_%s"],
                                                "closed": false
                                              }
                                            ]
                                            """.formatted(slug, slug, slug, slug));
                            return;
                        }
                    }
                    req.response().setStatusCode(404).end();
                })
                .webSocketHandler(ws -> {
                    String path = ws.path();
                    if (path.startsWith("/binance/spot/")) {
                        ws.writeTextMessage("{\"e\":\"aggTrade\",\"E\":1789041600100,\"s\":\"BTCUSDT\",\"a\":1001,\"p\":\"78450.10\",\"q\":\"0.25\",\"m\":false}");
                    } else if (path.startsWith("/binance/futures/")) {
                        if (path.endsWith("@aggTrade")) {
                            ws.writeTextMessage("{\"e\":\"aggTrade\",\"E\":1789041600110,\"s\":\"BTCUSDT\",\"a\":2001,\"p\":\"78451.00\",\"q\":\"1.50\",\"m\":true}");
                        } else if (path.endsWith("@depth@100ms")) {
                            ws.writeTextMessage("{\"e\":\"depthUpdate\",\"E\":1789041600120,\"s\":\"BTCUSDT\",\"b\":[[\"78450.00\",\"3.0\"]],\"a\":[[\"78451.00\",\"2.0\"]]}");
                        } else if (path.endsWith("@depth20@100ms")) {
                            ws.writeTextMessage("{\"e\":\"depthUpdate\",\"E\":1789041600130,\"s\":\"BTCUSDT\",\"b\":[[\"78450.00\",\"3.0\"]],\"a\":[[\"78451.00\",\"2.0\"]]}");
                        } else if (path.endsWith("@forceOrder")) {
                            ws.writeTextMessage("{\"e\":\"forceOrder\",\"E\":1789041600140,\"o\":{\"s\":\"BTCUSDT\",\"S\":\"SELL\",\"q\":\"0.10\",\"p\":\"78000.00\"}}");
                        }
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
                    } else if (path.equals("/deribit/ws/api/v2")) {
                        ws.textMessageHandler(msg -> {
                            if (msg.contains("public/subscribe") && msg.contains("trades.")) {
                                ws.writeTextMessage("{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":[\"trades.BTC-PERPETUAL.100ms\"]}");
                                ws.writeTextMessage("{\"jsonrpc\":\"2.0\",\"method\":\"subscription\",\"params\":{\"channel\":\"trades.BTC-PERPETUAL.100ms\",\"data\":[{\"trade_seq\":101,\"price\":78452.0,\"amount\":10.0,\"direction\":\"buy\",\"instrument_name\":\"BTC-PERPETUAL\"}]}}");
                            }
                        });
                    }
                });

        try {
            int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
            return Map.ofEntries(
                    Map.entry("polymarket.collector.data-dir", tempDataDir.toString()),
                    Map.entry("polymarket.collector.lookahead-count", "5"),
                    Map.entry("polymarket.collector.flush-interval", "1s"),
                    Map.entry("polymarket.connector.binance.spot-ws-base-url", "ws://localhost:" + port + "/binance/spot"),
                    Map.entry("polymarket.connector.binance.futures-ws-base-url", "ws://localhost:" + port + "/binance/futures"),
                    Map.entry("polymarket.connector.deribit.ws-url", "ws://localhost:" + port + "/deribit/ws/api/v2"),
                    Map.entry("polymarket.connector.gamma.base-url", "http://localhost:" + port + "/gamma"),
                    Map.entry("polymarket.connector.polybolt.ws-url", "ws://localhost:" + port + "/polybolt/ws"),
                    Map.entry("polymarket.connector.clob.ws-url", "ws://localhost:" + port + "/clob/ws/market"),
                    Map.entry("polymarket.connector.api-key", "test-api-key"),
                    Map.entry("polymarket.connector.secret", "test-secret"),
                    Map.entry("polymarket.connector.passphrase", "test-passphrase")
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to start MockMarketStreamsTestResource", e);
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
