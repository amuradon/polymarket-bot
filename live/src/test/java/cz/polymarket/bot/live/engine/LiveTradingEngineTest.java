package cz.polymarket.bot.live.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.cache.HourlyPriceCache;
import cz.polymarket.bot.calculator.PerformanceMetricsCalculator;
import cz.polymarket.bot.connector.polymarket.PolymarketClobClient;
import cz.polymarket.bot.connector.polymarket.PolymarketGammaClient;
import cz.polymarket.bot.connector.polymarket.PolymarketPolyBoltClient;
import cz.polymarket.bot.connector.polymarket.auth.PolymarketSecretsResolver;
import cz.polymarket.bot.connector.polymarket.clob.PolymarketClobRestClient;
import cz.polymarket.bot.connector.polymarket.stream.PolymarketStreamParser;
import cz.polymarket.bot.domain.Timeframe;
import cz.polymarket.bot.live.storage.LiveTradeRepository;
import cz.polymarket.bot.strategy.OrderCommand;
import cz.polymarket.bot.strategy.StrategyRegistry;
import cz.polymarket.bot.strategy.UtterParameciumStrategy;
import cz.polymarket.bot.strategy.UtterParameciumStrategyConfig;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LiveTradingEngineTest {

    private static final String TEST_PRIVATE_KEY =
            "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
    private static final String TEST_EOA_ADDRESS =
            "0x2c7536E3605D9C16a7a3D7b1898e529396a65c23";
    private static final String TEST_MAKER_ADDRESS =
            "0x70997970C51812dc3A010C7d01b50e0d17dc79C8";
    private static final String TEST_TOKEN_UP =
            "50942702967128588059751532145364262040082665861951814492817689719364041142318";
    private static final String TEST_TOKEN_DOWN =
            "50942702967128588059751532145364262040082665861951814492817689719364041142319";

    @TempDir
    Path tempDir;

    private Vertx vertx;
    private HttpServer mockServer;
    private int port;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final List<String> orderBodies = new CopyOnWriteArrayList<>();
    private final List<String> polyAddresses = new CopyOnWriteArrayList<>();
    private final List<String> cancelBodies = new CopyOnWriteArrayList<>();
    private final AtomicBoolean polyBoltAuthedBeforeSubscribe = new AtomicBoolean(false);
    private final AtomicBoolean clobAutoRespond = new AtomicBoolean(true);
    private final List<ServerWebSocket> polyBoltSockets = new CopyOnWriteArrayList<>();
    private final List<ServerWebSocket> clobSockets = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() throws Exception {
        polyBoltAuthedBeforeSubscribe.set(false);
        clobAutoRespond.set(true);
        orderBodies.clear();
        polyAddresses.clear();
        cancelBodies.clear();
        polyBoltSockets.clear();
        clobSockets.clear();
        vertx = Vertx.vertx();
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());

        router.get("/markets/slug/:slug").handler(ctx -> {
            String slug = ctx.pathParam("slug");
            ctx.response()
                    .putHeader("Content-Type", "application/json")
                    .end("""
                            {
                              "id": "1901269",
                              "slug": "%s",
                              "conditionId": "0xcond-live-1",
                              "clobTokenIds": "[\\"%s\\", \\"%s\\"]",
                              "outcomes": "[\\"Up\\", \\"Down\\"]",
                              "closed": false
                            }
                            """.formatted(slug, TEST_TOKEN_UP, TEST_TOKEN_DOWN));
        });

        router.get("/time").handler(ctx -> ctx.response()
                .putHeader("Content-Type", "application/json")
                .end("1775261657"));

        router.post("/order").handler(ctx -> {
            String body = ctx.body().asString();
            orderBodies.add(body);
            polyAddresses.add(ctx.request().getHeader("POLY_ADDRESS"));
            boolean isSell = body.contains("\"side\":\"SELL\"");
            String taking = isSell ? "70" : "100";
            String making = isSell ? "100" : "55";
            ctx.response()
                    .putHeader("Content-Type", "application/json")
                    .end("""
                            {
                              "success": true,
                              "errorMsg": "",
                              "orderID": "0xorder-live-filled-1",
                              "transactionsHashes": ["0xtx1"],
                              "status": "matched",
                              "takingAmount": "%s",
                              "makingAmount": "%s"
                            }
                            """.formatted(taking, making));
        });

        router.delete("/order").handler(ctx -> {
            cancelBodies.add(ctx.body().asString());
            ctx.response()
                    .putHeader("Content-Type", "application/json")
                    .end("""
                            {
                              "canceled": ["0xorder-live-filled-1"],
                              "not_canceled": {}
                            }
                            """);
        });

        mockServer = vertx.createHttpServer();
        mockServer.webSocketHandler(ws -> {
            if ("/ws/polybolt".equals(ws.path())) {
                polyBoltSockets.add(ws);
                AtomicBoolean authed = new AtomicBoolean(false);
                ws.textMessageHandler(msg -> {
                    try {
                        JsonNode json = objectMapper.readTree(msg);
                        String op = json.path("op").asText("");
                        if ("auth".equals(op)) {
                            authed.set(true);
                            ws.writeTextMessage("{\"op\":\"authed\"}");
                        } else if ("subscribe".equals(op)) {
                            if (authed.get()) {
                                polyBoltAuthedBeforeSubscribe.set(true);
                                long nowMs = System.currentTimeMillis();
                                ws.writeTextMessage("""
                                        {
                                          "channel": "price.crypto.twap",
                                          "key": "btcusd",
                                          "snapshot": true,
                                          "payload": {
                                            "symbol": "btcusd",
                                            "data": [
                                              {"timestamp": %d, "value": 85000.0},
                                              {"timestamp": %d, "value": 85010.0}
                                            ]
                                          }
                                        }
                                        """.formatted(nowMs - 2000L, nowMs - 1000L));
                                ws.writeTextMessage("""
                                        {
                                          "channel": "price.crypto",
                                          "key": "btcusd",
                                          "timestamp": %d,
                                          "payload": {
                                            "symbol": "btcusd",
                                            "timestamp": %d,
                                            "value": 85020.0
                                          }
                                        }
                                        """.formatted(nowMs, nowMs));
                            }
                        }
                    } catch (Exception ignored) {
                    }
                });
            } else if ("/ws/clob".equals(ws.path())) {
                clobSockets.add(ws);
                ws.textMessageHandler(msg -> {
                    if (clobAutoRespond.get() && !"PING".equals(msg)) {
                        ws.writeTextMessage("""
                                [
                                  {
                                    "event_type": "book",
                                    "asset_id": "%s",
                                    "timestamp": "1775261657000",
                                    "bids": [{"price": "0.54", "size": "250"}],
                                    "asks": [{"price": "0.55", "size": "300"}]
                                  },
                                  {
                                    "event_type": "book",
                                    "asset_id": "%s",
                                    "timestamp": "1775261657000",
                                    "bids": [{"price": "0.44", "size": "250"}],
                                    "asks": [{"price": "0.45", "size": "300"}]
                                  }
                                ]
                                """.formatted(TEST_TOKEN_UP, TEST_TOKEN_DOWN));
                    }
                });
            }
        });

        mockServer.requestHandler(router);
        mockServer.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        port = mockServer.actualPort();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (mockServer != null) {
            mockServer.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
        if (vertx != null) {
            vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldConnectStreamsAuthenticatePolyBoltSubmitLiveFakOrdersAndSettlePositions() throws Exception {
        PolymarketSecretsResolver secretsResolver = new PolymarketSecretsResolver(
                Optional.of(TEST_PRIVATE_KEY),
                Optional.of("api-key-live"),
                Optional.of("passphrase-live"),
                Optional.of("MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI="),
                Optional.of(TEST_MAKER_ADDRESS),
                Optional.of(TEST_EOA_ADDRESS)
        );

        HttpClient httpClient = HttpClient.newHttpClient();
        String baseHttpUrl = "http://localhost:" + port;
        PolymarketGammaClient gammaClient = new PolymarketGammaClient(baseHttpUrl, httpClient, objectMapper);
        PolymarketPolyBoltClient polyBoltClient = new PolymarketPolyBoltClient(
                vertx,
                "ws://localhost:" + port + "/ws/polybolt",
                Optional.of("api-key-live"),
                Optional.of("MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI="),
                Optional.of("passphrase-live"),
                1000L,
                10000L
        );
        PolymarketClobClient clobClient = new PolymarketClobClient(
                vertx,
                "ws://localhost:" + port + "/ws/clob",
                1000L,
                10000L
        );
        PolymarketClobRestClient clobRestClient = new PolymarketClobRestClient(
                baseHttpUrl,
                httpClient,
                objectMapper,
                secretsResolver
        );
        PolymarketStreamParser streamParser = new PolymarketStreamParser(objectMapper);
        LiveTradeRepository tradeRepository = new LiveTradeRepository(
                tempDir.toString(),
                1000.0,
                objectMapper,
                new PerformanceMetricsCalculator()
        );
        UtterParameciumStrategy strategy = new UtterParameciumStrategy(UtterParameciumStrategyConfig.defaultModelV1());
        StrategyRegistry strategyRegistry = new StrategyRegistry(Map.of(strategy.getName(), strategy));
        HourlyPriceCache priceCache = new HourlyPriceCache(3600L);

        LiveTradingEngine engine = new LiveTradingEngine(
                strategyRegistry,
                priceCache,
                tradeRepository,
                secretsResolver,
                clobRestClient,
                gammaClient,
                clobClient,
                polyBoltClient,
                streamParser,
                "UtterParameciumStrategy",
                "BTCUSDT",
                "15m",
                true,
                false, // dryRun = false -> real live execution against mock CLOB server
                "FAK"
        );

        try {
            engine.onStart(null);

            Awaitility.await()
                    .atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> {
                        assertThat(polyBoltAuthedBeforeSubscribe.get()).isTrue();
                        assertThat(engine.getActiveMarketInfo()).isNotNull();
                        assertThat(engine.getActiveMarketInfo().yesTokenId()).isEqualTo(TEST_TOKEN_UP);
                        assertThat(engine.getActiveMarketInfo().noTokenId()).isEqualTo(TEST_TOKEN_DOWN);
                        assertThat(engine.getCurrentSpotPrice()).isEqualTo(85020.0);
                        assertThat(engine.getLatestQuote()).isNotNull();
                        assertThat(engine.getLatestQuote().bestAskUp()).isEqualTo(0.55);
                    });

            // Submit a live BUY order via StrategyContext / ExecutionRouter
            engine.submitOrder(new OrderCommand(
                    "client-order-live-1",
                    "pm-btc-15m",
                    "BTC-UP",
                    Timeframe.FIFTEEN_MINUTES,
                    "BUY",
                    BigDecimal.valueOf(0.55),
                    BigDecimal.valueOf(100)
            ));

            assertThat(orderBodies).hasSize(1);
            assertThat(polyAddresses).containsExactly(TEST_EOA_ADDRESS);
            assertThat(engine.getOpenPositions()).hasSize(1);

            JsonNode submittedEnvelope = objectMapper.readTree(orderBodies.get(0));
            assertThat(submittedEnvelope.get("owner").asText()).isEqualTo("api-key-live");
            assertThat(submittedEnvelope.get("orderType").asText()).isEqualTo("FAK");
            assertThat(submittedEnvelope.get("deferExec").asBoolean()).isFalse();
            JsonNode orderNode = submittedEnvelope.get("order");
            assertThat(orderNode.get("maker").asText()).isEqualTo(TEST_MAKER_ADDRESS);
            assertThat(orderNode.get("signer").asText()).isEqualTo(TEST_MAKER_ADDRESS);
            assertThat(orderNode.get("signatureType").asInt()).isEqualTo(3);
            assertThat(orderNode.get("side").asText()).isEqualTo("BUY");
            assertThat(orderNode.get("signature").asText()).startsWith("0x").hasSize(636);

            LivePosition openPosition = engine.getOpenPositions().get(0);
            assertThat(openPosition.clobTokenId()).isEqualTo(TEST_TOKEN_UP);
            assertThat(openPosition.entryPrice()).isEqualTo(0.55);
            assertThat(openPosition.shares()).isEqualTo(100.0);

            // Test live order cancellation via DELETE /order
            engine.cancelOrder("client-order-live-1");
            assertThat(cancelBodies).hasSize(1);
            assertThat(cancelBodies.get(0)).contains("0xorder-live-filled-1");

            // Submit a live SELL exit order to close the position early and record PnL
            engine.submitOrder(new OrderCommand(
                    "client-order-exit-1",
                    "pm-btc-15m",
                    "BTC-UP",
                    Timeframe.FIFTEEN_MINUTES,
                    "SELL",
                    BigDecimal.valueOf(0.70),
                    BigDecimal.valueOf(100)
            ));

            assertThat(orderBodies).hasSize(2);
            assertThat(engine.getOpenPositions()).isEmpty();
            assertThat(tradeRepository.getTrades()).hasSize(1);
            assertThat(tradeRepository.getTrades().get(0).isWin()).isTrue();
            assertThat(tradeRepository.getTrades().get(0).exitPrice()).isEqualTo(0.70);
        } finally {
            engine.onStop(null);
        }
    }

    @Test
    void shouldLogErrorWhenPolyBoltOrClobWebSocketReceivesZeroMessagesForMarket() {
        List<LogRecord> capturedLogs = new CopyOnWriteArrayList<>();
        Logger engineLogger = Logger.getLogger(LiveTradingEngine.class.getName());
        Handler testHandler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                if (record != null) {
                    capturedLogs.add(record);
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        engineLogger.addHandler(testHandler);

        clobAutoRespond.set(false);
        LiveTradingEngine engine = null;
        try {
            engine = createTestEngine(true);

            // Market 1: [1792741500, 1792742400] - only PolyBolt receives a message, CLOB WS receives 0 messages
            long market1StartSec = 1792741500L;
            engine.discoverAndRotateMarket(market1StartSec + 10L);
            engine.onRawPolyBoltFrame("""
                    {
                      "channel": "price.crypto.twap",
                      "key": "btcusd",
                      "timestamp": 1792741500000,
                      "payload": {
                        "symbol": "btcusd",
                        "timestamp": 1792741500000,
                        "value": 90000.0
                      }
                    }
                    """, market1StartSec * 1000L);

            assertThat(engine.getPolyBoltMarketMessageCount()).isEqualTo(1L);
            assertThat(engine.getClobMarketMessageCount()).isZero();

            // Rotate to Market 2: [1792742400, 1792743300] -> should detect 0 CLOB WS messages in Market 1 and log ERROR
            long market2StartSec = 1792742400L;
            engine.discoverAndRotateMarket(market2StartSec + 10L);

            assertThat(capturedLogs)
                    .anyMatch(r -> r.getLevel().intValue() >= Level.SEVERE.intValue()
                            && r.getMessage() != null
                            && r.getMessage().contains("btc-updown-15m-1792741500")
                            && r.getMessage().contains("polyBoltMessages=1")
                            && r.getMessage().contains("clobWsMessages=0"));

            capturedLogs.clear();

            // Market 2: only CLOB WS receives a message, PolyBolt receives 0 messages
            engine.onRawClobFrame("""
                    [
                      {
                        "event_type": "book",
                        "asset_id": "%s",
                        "timestamp": "1792742410000",
                        "bids": [{"price": "0.54", "size": "250"}],
                        "asks": [{"price": "0.55", "size": "300"}]
                      }
                    ]
                    """.formatted(TEST_TOKEN_UP), (market2StartSec + 10L) * 1000L);

            assertThat(engine.getPolyBoltMarketMessageCount()).isZero();
            assertThat(engine.getClobMarketMessageCount()).isEqualTo(1L);

            // Rotate to Market 3: [1792743300, 1792744200] -> should detect 0 PolyBolt messages in Market 2 and log ERROR
            long market3StartSec = 1792743300L;
            engine.discoverAndRotateMarket(market3StartSec + 10L);

            assertThat(capturedLogs)
                    .anyMatch(r -> r.getLevel().intValue() >= Level.SEVERE.intValue()
                            && r.getMessage() != null
                            && r.getMessage().contains("btc-updown-15m-1792742400")
                            && r.getMessage().contains("polyBoltMessages=0")
                            && r.getMessage().contains("clobWsMessages=1"));

            capturedLogs.clear();

            // Market 3: both PolyBolt and CLOB WS receive messages -> no zero-message ERROR on rotation to Market 4
            engine.onRawPolyBoltFrame("""
                    {
                      "channel": "price.crypto",
                      "key": "btcusd",
                      "timestamp": 1792743310000,
                      "payload": {
                        "symbol": "btcusd",
                        "timestamp": 1792743310000,
                        "value": 90050.0
                      }
                    }
                    """, (market3StartSec + 10L) * 1000L);
            engine.onRawClobFrame("""
                    [
                      {
                        "event_type": "book",
                        "asset_id": "%s",
                        "timestamp": "1792743310000",
                        "bids": [{"price": "0.54", "size": "250"}],
                        "asks": [{"price": "0.55", "size": "300"}]
                      }
                    ]
                    """.formatted(TEST_TOKEN_UP), (market3StartSec + 10L) * 1000L);

            long market4StartSec = 1792744200L;
            engine.discoverAndRotateMarket(market4StartSec + 10L);

            assertThat(capturedLogs)
                    .noneMatch(r -> r.getLevel().intValue() >= Level.SEVERE.intValue());
            assertThat(capturedLogs)
                    .anyMatch(r -> r.getLevel().equals(Level.INFO)
                            && r.getMessage() != null
                            && r.getMessage().contains("btc-updown-15m-1792743300")
                            && r.getMessage().contains("polyBoltMessages=1")
                            && r.getMessage().contains("clobWsMessages=1"));
        } finally {
            if (engine != null) {
                engine.onStop(null);
            }
            clobAutoRespond.set(true);
            engineLogger.removeHandler(testHandler);
        }
    }

    @Test
    void shouldLogFatalAndThrowWhenStartingLiveModeWithoutCredentials() {
        List<LogRecord> capturedLogs = new CopyOnWriteArrayList<>();
        Logger engineLogger = Logger.getLogger(LiveTradingEngine.class.getName());
        Handler testHandler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                if (record != null) {
                    capturedLogs.add(record);
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        engineLogger.addHandler(testHandler);

        try {
            PolymarketSecretsResolver emptyResolver = new PolymarketSecretsResolver(
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty()
            );
            HttpClient httpClient = HttpClient.newHttpClient();
            String baseHttpUrl = "http://localhost:" + port;
            LiveTradingEngine engine = new LiveTradingEngine(
                    new StrategyRegistry(Map.of("UtterParameciumStrategy",
                            new UtterParameciumStrategy(UtterParameciumStrategyConfig.defaultModelV1()))),
                    new HourlyPriceCache(3600L),
                    new LiveTradeRepository(tempDir.toString(), 1000.0, objectMapper, new PerformanceMetricsCalculator()),
                    emptyResolver,
                    new PolymarketClobRestClient(baseHttpUrl, httpClient, objectMapper, emptyResolver),
                    new PolymarketGammaClient(baseHttpUrl, httpClient, objectMapper),
                    new PolymarketClobClient(vertx, "ws://localhost:" + port + "/ws/clob", 1000L, 10000L),
                    new PolymarketPolyBoltClient(vertx, "ws://localhost:" + port + "/ws/polybolt",
                            Optional.empty(), Optional.empty(), Optional.empty(), 1000L, 10000L),
                    new PolymarketStreamParser(objectMapper),
                    "UtterParameciumStrategy",
                    "BTCUSDT",
                    "15m",
                    false,
                    false, // dryRun = false -> live mode
                    "FAK"
            );

            assertThatThrownBy(() -> engine.onStart(null))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Failed to resolve Polymarket credentials for live trading");

            assertThat(capturedLogs)
                    .anyMatch(r -> r.getLevel().intValue() >= Level.SEVERE.intValue()
                            && r.getMessage() != null
                            && r.getMessage().contains("Fatal configuration error"));
        } finally {
            engineLogger.removeHandler(testHandler);
        }
    }

    private LiveTradingEngine createTestEngine(boolean dryRun) {
        PolymarketSecretsResolver secretsResolver = new PolymarketSecretsResolver(
                Optional.of(TEST_PRIVATE_KEY),
                Optional.of("api-key-live"),
                Optional.of("passphrase-live"),
                Optional.of("MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI="),
                Optional.of(TEST_MAKER_ADDRESS),
                Optional.of(TEST_EOA_ADDRESS)
        );
        HttpClient httpClient = HttpClient.newHttpClient();
        String baseHttpUrl = "http://localhost:" + port;
        UtterParameciumStrategy strategy = new UtterParameciumStrategy(UtterParameciumStrategyConfig.defaultModelV1());
        return new LiveTradingEngine(
                new StrategyRegistry(Map.of(strategy.getName(), strategy)),
                new HourlyPriceCache(3600L),
                new LiveTradeRepository(tempDir.toString(), 1000.0, objectMapper, new PerformanceMetricsCalculator()),
                secretsResolver,
                new PolymarketClobRestClient(baseHttpUrl, httpClient, objectMapper, secretsResolver),
                new PolymarketGammaClient(baseHttpUrl, httpClient, objectMapper),
                new PolymarketClobClient(vertx, "ws://localhost:" + port + "/ws/clob", 1000L, 10000L),
                new PolymarketPolyBoltClient(vertx, "ws://localhost:" + port + "/ws/polybolt",
                        Optional.of("api-key-live"),
                        Optional.of("MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI="),
                        Optional.of("passphrase-live"),
                        1000L, 10000L),
                new PolymarketStreamParser(objectMapper),
                "UtterParameciumStrategy",
                "BTCUSDT",
                "15m",
                false,
                dryRun,
                "FAK"
        );
    }
}
