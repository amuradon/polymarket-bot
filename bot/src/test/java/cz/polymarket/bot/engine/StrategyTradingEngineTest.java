package cz.polymarket.bot.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.calculator.PerformanceMetricsCalculator;
import cz.polymarket.bot.collector.DataCollectorService;
import cz.polymarket.bot.collector.PolymarketLifecycleCoordinator;
import cz.polymarket.bot.collector.storage.DailyBinanceStorageManager;
import cz.polymarket.bot.collector.storage.DailyPolyBoltStorageManager;
import cz.polymarket.bot.collector.storage.PolymarketMarketStorageManager;
import cz.polymarket.bot.collector.storage.ZstdJsonlWriterTest;
import cz.polymarket.bot.connector.binance.BinanceStreamConnector;
import cz.polymarket.bot.connector.coinbase.CoinbaseStreamConnector;
import cz.polymarket.bot.connector.kraken.KrakenStreamConnector;
import cz.polymarket.bot.connector.polymarket.PolymarketClobClient;
import cz.polymarket.bot.connector.polymarket.PolymarketGammaClient;
import cz.polymarket.bot.connector.polymarket.PolymarketMarketInfo;
import cz.polymarket.bot.connector.polymarket.PolymarketPolyBoltClient;
import cz.polymarket.bot.connector.polymarket.auth.PolymarketSecretsResolver;
import cz.polymarket.bot.connector.polymarket.clob.PolymarketClobRestClient;
import cz.polymarket.bot.connector.polymarket.stream.PolymarketStreamParser;
import cz.polymarket.bot.connector.ws.MarketWebSocketRuntime;
import cz.polymarket.bot.connector.ws.TimestampedFrame;
import cz.polymarket.bot.domain.OrderBookQuote;
import cz.polymarket.bot.domain.Timeframe;
import cz.polymarket.bot.domain.TradeRecord;
import cz.polymarket.bot.indicators.FastSpotMicroIndicator;
import cz.polymarket.bot.indicators.FastSpotMicroIndicatorConfig;
import cz.polymarket.bot.indicators.SpotTwapIndicator;
import cz.polymarket.bot.monitoring.SystemNetworkTelemetryMonitor;
import cz.polymarket.bot.service.RuntimeCoordinator;
import cz.polymarket.bot.state.RuntimeStateStore;
import cz.polymarket.bot.storage.PaperTradeRepository;
import cz.polymarket.bot.strategy.OrderCommand;
import cz.polymarket.bot.strategy.StrategyRegistry;
import cz.polymarket.bot.strategy.TradeDirection;
import cz.polymarket.bot.strategy.TradingStrategy;
import cz.polymarket.bot.strategy.UtterParameciumStrategy;
import cz.polymarket.bot.strategy.UtterParameciumStrategyConfig;
import cz.polymarket.bot.stream.SpotTwapProcessor;
import cz.polymarket.bot.web.TelemetryDto;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Comprehensive synthetic tests for {@link StrategyTradingEngine} and {@link RuntimeCoordinator} covering:
 * <ul>
 *   <li>{@link StrategyMode#LIVE}: 4-core pipeline, EIP-712 signing, FAK orders, cancel, {@code /ws/user} balance sync, freshness guard</li>
 *   <li>{@link StrategyMode#PAPER}: simulated latency fills, limit/bestAsk price improvement &amp; slippage rejection, candle settlement to {@code trades.jsonl}</li>
 *   <li>{@link StrategyMode#OFF} &amp; runtime mode transitions: zero strategy worker threads and zero strategy CLOB/PolyBolt queue accumulation while Collector records</li>
 *   <li>Zero duplicate records in Data Collector archives when running alongside dual Active-Active strategy connections</li>
 *   <li>Seamless {@code twapOpenPrice} (target price), {@link StrategyMode}, and {@link Position} restoration across restarts via {@link RuntimeStateStore}</li>
 * </ul>
 */
class StrategyTradingEngineTest {

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
    private MarketWebSocketRuntime marketWsRuntime;
    private HttpServer mockServer;
    private int port;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private BinanceStreamConnector binanceConnector;
    private KrakenStreamConnector krakenConnector;
    private CoinbaseStreamConnector coinbaseConnector;
    private SpotTwapIndicator spotTwapIndicator;
    private SpotTwapProcessor spotStreamProcessor;
    private PaperTradeRepository paperRepository;
    private RuntimeStateStore stateStore;

    private final List<String> orderBodies = new CopyOnWriteArrayList<>();
    private final List<String> polyAddresses = new CopyOnWriteArrayList<>();
    private final List<String> cancelBodies = new CopyOnWriteArrayList<>();
    private final AtomicBoolean polyBoltAuthedBeforeSubscribe = new AtomicBoolean(false);
    private final AtomicBoolean clobUserAuthed = new AtomicBoolean(false);
    private final AtomicBoolean clobAutoRespond = new AtomicBoolean(true);
    private final AtomicLong mockOrderResponseDelayMs = new AtomicLong(0L);
    private final AtomicLong mockCollateralMicroUsdc = new AtomicLong(2_328_000_000L);
    private final List<ServerWebSocket> polyBoltSockets = new CopyOnWriteArrayList<>();
    private final List<ServerWebSocket> clobSockets = new CopyOnWriteArrayList<>();
    private final List<ServerWebSocket> userSockets = new CopyOnWriteArrayList<>();
    private final List<ServerWebSocket> binanceSockets = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() throws Exception {
        polyBoltAuthedBeforeSubscribe.set(false);
        clobUserAuthed.set(false);
        clobAutoRespond.set(true);
        mockOrderResponseDelayMs.set(0L);
        mockCollateralMicroUsdc.set(2_328_000_000L);
        orderBodies.clear();
        polyAddresses.clear();
        cancelBodies.clear();
        polyBoltSockets.clear();
        clobSockets.clear();
        userSockets.clear();
        binanceSockets.clear();

        vertx = Vertx.vertx();
        marketWsRuntime = new MarketWebSocketRuntime(30L);
        paperRepository = new PaperTradeRepository(
                tempDir.resolve("paper").toString(),
                10_000.0,
                objectMapper,
                new PerformanceMetricsCalculator()
        );
        stateStore = new RuntimeStateStore(
                tempDir.resolve("state/runtime-state.json").toString(),
                objectMapper
        );

        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());

        router.get("/balance-allowance").handler(ctx -> ctx.response()
                .putHeader("Content-Type", "application/json")
                .end("""
                        {
                          "balance": "%d",
                          "allowances": {}
                        }
                        """.formatted(mockCollateralMicroUsdc.get())));

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
            String responseBody = """
                    {
                      "success": true,
                      "errorMsg": "",
                      "orderID": "0xorder-live-filled-1",
                      "transactionsHashes": ["0xtx1"],
                      "status": "matched",
                      "takingAmount": "%s",
                      "makingAmount": "%s"
                    }
                    """.formatted(taking, making);
            long delayMs = mockOrderResponseDelayMs.get();
            if (delayMs > 0L) {
                vertx.setTimer(delayMs, id -> ctx.response()
                        .putHeader("Content-Type", "application/json")
                        .end(responseBody));
            } else {
                ctx.response()
                        .putHeader("Content-Type", "application/json")
                        .end(responseBody);
            }
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
        mockServer.requestHandler(req -> {
            if ("websocket".equalsIgnoreCase(req.getHeader("Upgrade"))) {
                req.toWebSocket().onSuccess(this::handleMockWebSocket);
                return;
            }
            router.handle(req);
        });
        mockServer.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        port = mockServer.actualPort();

        binanceConnector = new BinanceStreamConnector(
                marketWsRuntime,
                "ws://localhost:" + port + "/binance/spot",
                "ws://localhost:" + port + "/binance/futures",
                1000L,
                10000L
        );
        krakenConnector = new KrakenStreamConnector(
                marketWsRuntime,
                "ws://localhost:" + port + "/kraken/ws/v2",
                1000L,
                10000L
        );
        coinbaseConnector = new CoinbaseStreamConnector(
                marketWsRuntime,
                "ws://localhost:" + port + "/coinbase/ws",
                1000L,
                10000L
        );
        spotTwapIndicator = new SpotTwapIndicator();
        FastSpotMicroIndicator fastSpotMicroIndicator =
                new FastSpotMicroIndicator(FastSpotMicroIndicatorConfig.defaults());
        spotStreamProcessor = new SpotTwapProcessor(objectMapper, spotTwapIndicator, fastSpotMicroIndicator, "BTCUSDT");
    }

    private void handleMockWebSocket(ServerWebSocket ws) {
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
                    } else if ("ping".equals(op)) {
                        ws.writeTextMessage("{\"op\":\"pong\"}");
                    } else if ("subscribe".equals(op) && authed.get()) {
                        polyBoltAuthedBeforeSubscribe.set(true);
                        long nowMs = System.currentTimeMillis();
                        if (msg.contains("\"channel\":\"price.crypto.twap\"")) {
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
                        } else if (msg.contains("\"channel\":\"price.crypto\"")) {
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
                    long nowMs = System.currentTimeMillis();
                    ws.writeTextMessage("""
                            [
                              {
                                "event_type": "book",
                                "asset_id": "%s",
                                "timestamp": "%d",
                                "bids": [{"price": "0.54", "size": "250"}],
                                "asks": [{"price": "0.55", "size": "300"}]
                              },
                              {
                                "event_type": "book",
                                "asset_id": "%s",
                                "timestamp": "%d",
                                "bids": [{"price": "0.44", "size": "250"}],
                                "asks": [{"price": "0.45", "size": "300"}]
                              }
                            ]
                            """.formatted(TEST_TOKEN_UP, nowMs, TEST_TOKEN_DOWN, nowMs));
                }
            });
        } else if ("/ws/user".equals(ws.path())) {
            userSockets.add(ws);
            ws.textMessageHandler(msg -> {
                if (msg.contains("\"type\":\"user\"") && msg.contains("\"apiKey\"")) {
                    clobUserAuthed.set(true);
                }
            });
        } else if (ws.path().startsWith("/binance/spot/")) {
            binanceSockets.add(ws);
            if (ws.path().endsWith("@aggTrade")) {
                ws.writeTextMessage("{\"e\":\"aggTrade\",\"E\":1775261657000,\"s\":\"BTCUSDT\",\"a\":1001,\"p\":\"85020.00\",\"q\":\"0.50\",\"m\":false}");
            }
        } else if ("/kraken/ws/v2".equals(ws.path())) {
            ws.textMessageHandler(msg -> {
                if (msg.contains("\"USDT/USD\"")) {
                    ws.writeTextMessage("{\"channel\":\"trade\",\"type\":\"update\",\"data\":[{\"symbol\":\"USDT/USD\",\"side\":\"buy\",\"price\":1.0000,\"qty\":250.0,\"trade_id\":9001}]}");
                } else if (msg.contains("\"BTC/USD\"")) {
                    ws.writeTextMessage("{\"channel\":\"trade\",\"type\":\"update\",\"data\":[{\"symbol\":\"BTC/USD\",\"side\":\"buy\",\"price\":85020.0,\"qty\":0.40,\"trade_id\":9002}]}");
                }
            });
        } else if ("/coinbase/ws".equals(ws.path())) {
            ws.textMessageHandler(msg -> {
                if (msg.contains("\"matches\"") && msg.contains("\"BTC-USD\"")) {
                    ws.writeTextMessage("{\"type\":\"match\",\"trade_id\":8001,\"side\":\"buy\",\"size\":\"0.60\",\"price\":\"85020.00\",\"product_id\":\"BTC-USD\"}");
                }
            });
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        if (binanceConnector != null) {
            binanceConnector.destroy();
        }
        if (krakenConnector != null) {
            krakenConnector.destroy();
        }
        if (coinbaseConnector != null) {
            coinbaseConnector.destroy();
        }
        if (marketWsRuntime != null) {
            marketWsRuntime.destroy();
        }
        if (mockServer != null) {
            mockServer.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
        if (vertx != null) {
            vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldConnectLivePipelineSubmitSignedFakOrdersUpdateUserBalanceOnCpu0AndCancelOrders() throws Exception {
        StrategyTradingEngine engine = createEngine("LIVE", true, 250L, 0L);
        try {
            engine.startConfiguredMode(Instant.now().getEpochSecond());

            Awaitility.await()
                    .atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> {
                        assertThat(polyBoltAuthedBeforeSubscribe.get()).isTrue();
                        assertThat(clobUserAuthed.get()).isTrue();
                        assertThat(engine.getInitialCapital()).isEqualTo(2328.0);
                        assertThat(engine.getCurrentBalance()).isEqualTo(2328.0);
                        assertThat(engine.getActiveMarketInfo()).isNotNull();
                        assertThat(engine.getCurrentSpotPrice()).isEqualTo(85020.0);
                        assertThat(engine.getLatestQuote()).isNotNull();
                        assertThat(engine.getLatestQuote().bestAskUp()).isEqualTo(0.55);
                    });

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
            JsonNode submittedEnvelope = objectMapper.readTree(orderBodies.getFirst());
            assertThat(submittedEnvelope.get("orderType").asText()).isEqualTo("FAK");

            // Push user trade debit over /ws/user
            for (ServerWebSocket userWs : userSockets) {
                userWs.writeTextMessage("""
                        {
                          "event_type": "trade",
                          "id": "trade-buy-1",
                          "side": "BUY",
                          "size": "100",
                          "price": "0.55",
                          "fee_rate_bps": "700",
                          "status": "MATCHED"
                        }
                        """);
            }
            Awaitility.await()
                    .atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> assertThat(engine.getCurrentBalance())
                            .isCloseTo(2328.0 - 56.7325, org.assertj.core.data.Offset.offset(0.01)));

            engine.cancelOrder("client-order-live-1");
            assertThat(cancelBodies).hasSize(1);

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
        } finally {
            engine.stopStrategyStreams();
        }
    }

    @Test
    void shouldExecutePaperOrdersWithLatencyBestAskPriceImprovementSlippageRejectionAndCandleSettlement() throws Exception {
        StrategyTradingEngine engine = createEngine("PAPER", false, 250L, 15L);

        long candleStartSec = 1_785_542_400L;
        long candleStartMs = candleStartSec * 1_000L;
        engine.onPolymarketCryptoTwap60s(candleStartMs, "btcusd", "chainlink", 85_000.0, 60, candleStartMs);
        assertThat(engine.getTwapOpenPrice()).isEqualTo(85_000.0);

        engine.onOrderBookQuote(new OrderBookQuote(
                0.65, 0.63, 0.37, 0.35, 500.0, 500.0, 0.64, 0.36, candleStartMs + 1_000L
        ));

        // 1. Price improvement: limit 0.70 fills at bestAskUp = 0.65 after 15ms latency without calling CLOB REST
        engine.submitOrder(new OrderCommand(
                "paper-buy-1", "0xMarket15m", "BTC-UP",
                Timeframe.FIFTEEN_MINUTES, "BUY", BigDecimal.valueOf(0.70), BigDecimal.valueOf(20.0)
        ));
        assertThat(engine.getActivePosition()).isNull();

        Awaitility.await()
                .atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertThat(engine.getOpenPositions()).hasSize(1));
        assertThat(engine.getOpenPositions().getFirst().entryPrice()).isEqualTo(0.65);
        assertThat(orderBodies).isEmpty();

        // 2. Slippage rejection: limit 0.62 < bestAskUp 0.65
        engine.submitOrder(new OrderCommand(
                "paper-buy-reject", "0xMarket15m", "BTC-UP",
                Timeframe.FIFTEEN_MINUTES, "BUY", BigDecimal.valueOf(0.62), BigDecimal.valueOf(20.0)
        ));
        Thread.sleep(40L);
        assertThat(engine.getOpenPositions()).hasSize(1);

        // 3. Settle candle via next candle's 60s TWAP -> persisted to trades.jsonl
        long candleEndMs = (candleStartSec + 900L) * 1_000L;
        engine.onPolymarketCryptoTwap60s(candleEndMs, "btcusd", "chainlink", 85_050.0, 60, candleEndMs);

        assertThat(engine.getOpenPositions()).isEmpty();
        assertThat(paperRepository.getTrades()).hasSize(1);
        TradeRecord recorded = paperRepository.getTrades().getFirst();
        assertThat(recorded.side()).isEqualTo(TradeDirection.UP);
        assertThat(recorded.exitPrice()).isEqualTo(1.00);
        assertThat(recorded.isWin()).isTrue();
    }

    @Test
    void shouldNeverDuplicateCollectorRecordsFromDualStrategyStreamsAndNeverFillStrategyQueuesInOffMode() throws Exception {
        clobAutoRespond.set(false);
        HttpClient httpClient = HttpClient.newHttpClient();
        String baseHttpUrl = "http://localhost:" + port;
        PolymarketSecretsResolver secretsResolver = defaultSecretsResolver();
        PolymarketGammaClient gammaClient = new PolymarketGammaClient(baseHttpUrl, httpClient, objectMapper);
        PolymarketClobClient clobClient = new PolymarketClobClient(
                vertx, marketWsRuntime, "ws://localhost:" + port + "/ws/clob", 1000L, 10000L
        );
        PolymarketPolyBoltClient polyBoltClient = new PolymarketPolyBoltClient(
                marketWsRuntime,
                "ws://localhost:" + port + "/ws/polybolt",
                Optional.of("api-key-live"),
                Optional.of("MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI="),
                Optional.of("passphrase-live"),
                1000L,
                10000L,
                30_000L
        );

        Path collectorDir = tempDir.resolve("collector");
        DailyBinanceStorageManager binanceStorage = new DailyBinanceStorageManager(collectorDir.toString(), "BTCUSDT");
        DailyPolyBoltStorageManager polyBoltStorage = new DailyPolyBoltStorageManager(collectorDir.toString(), "BTCUSDT");
        PolymarketMarketStorageManager polymarketStorage = new PolymarketMarketStorageManager(collectorDir.toString(), "BTCUSDT", "15m");
        PolymarketLifecycleCoordinator lifecycleCoordinator = new PolymarketLifecycleCoordinator(
                gammaClient, clobClient, polymarketStorage, "BTCUSDT", "15m", 1
        );
        DataCollectorService collectorService = new DataCollectorService(
                binanceConnector, binanceStorage, polyBoltStorage, lifecycleCoordinator, polymarketStorage,
                polyBoltClient, clobClient, "BTCUSDT", "15m", true, false, false
        );

        UtterParameciumStrategy strategy = new UtterParameciumStrategy(UtterParameciumStrategyConfig.defaultModelV1());
        StrategyTradingEngine engine = new StrategyTradingEngine(
                new StrategyRegistry(Map.of(strategy.getName(), strategy)),
                secretsResolver,
                new PolymarketClobRestClient(baseHttpUrl, httpClient, objectMapper, secretsResolver),
                gammaClient,
                clobClient,
                polyBoltClient,
                binanceConnector,
                krakenConnector,
                coinbaseConnector,
                new PolymarketStreamParser(objectMapper, "BTCUSDT"),
                spotStreamProcessor,
                paperRepository,
                stateStore,
                new SystemNetworkTelemetryMonitor(),
                vertx,
                "LIVE",
                "UtterParameciumStrategy",
                "BTCUSDT",
                "15m",
                true,
                "FAK",
                250L,
                0L
        );
        RuntimeCoordinator coordinator = new RuntimeCoordinator(collectorService, engine, paperRepository, stateStore);

        long nowSec = Instant.now().getEpochSecond();
        String activeSlug = PolymarketMarketInfo.buildMarketSlug(
                "BTCUSDT", "15m", PolymarketMarketInfo.alignCycleStart(nowSec, 900L));

        try {
            coordinator.startRuntime(nowSec);

            // Wait until shared Active market (2 dual-leg CLOB sockets: PRIMARY + BACKUP) + 1 Lookahead market = 3 CLOB sockets connect
            Awaitility.await()
                    .atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> {
                        assertThat(clobSockets).hasSize(3);
                        assertThat(binanceSockets).hasSize(2);
                        assertThat(polyBoltAuthedBeforeSubscribe.get()).isTrue();
                    });

            // Broadcast an identical unique CLOB book frame to ALL 3 connected CLOB sockets (2 shared active PRIMARY+BACKUP + 1 lookahead)
            String uniqueBookPayload = """
                    [{"event_type":"book","asset_id":"%s","timestamp":"%d","bids":[{"price":"0.51","size":"999"}],"asks":[{"price":"0.52","size":"888"}]}]
                    """.formatted(TEST_TOKEN_UP, nowSec * 1000L).trim();
            for (ServerWebSocket ws : clobSockets) {
                ws.writeTextMessage(uniqueBookPayload);
            }

            Awaitility.await()
                    .atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> {
                        assertThat(engine.getLatestQuote()).isNotNull();
                        assertThat(engine.getLatestQuote().bestAskUp()).isEqualTo(0.52);
                        assertThat(polymarketStorage.getTotalRecordsWritten()).isGreaterThanOrEqualTo(4L);
                    });

            // Switch Strategy to OFF at runtime -> BACKUP closes (leaving 2 open CLOB sockets), Collector stays active, Strategy queues stay at 0!
            coordinator.switchStrategyMode(StrategyMode.OFF);
            assertThat(engine.getStrategyMode()).isEqualTo(StrategyMode.OFF);
            assertThat(clobClient.hasQueuedMarketFrames()).isFalse();
            assertThat(clobClient.hasQueuedUserFrames()).isFalse();
            assertThat(polyBoltClient.getStrategyQueueDepth()).isZero();
            Awaitility.await()
                    .atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> assertThat(clobSockets.stream().filter(ws -> !ws.isClosed()).count()).isEqualTo(2L));

            // Push additional frames on CLOB and PolyBolt while Strategy is OFF
            for (ServerWebSocket ws : clobSockets) {
                try {
                    ws.writeTextMessage("{\"event_type\":\"last_trade_price\",\"price\":\"0.53\",\"size\":\"42\",\"side\":\"BUY\"}");
                } catch (Exception ignored) {
                }
            }
            for (ServerWebSocket ws : polyBoltSockets) {
                ws.writeTextMessage("""
                        {"v":1,"channel":"price.crypto","seq":99,"ts":%d,"payload":{"symbol":"btcusd","value":86123.0,"timestamp":%d,"source":"chainlink"}}
                        """.formatted((nowSec + 2L) * 1000L, (nowSec + 2L) * 1000L).trim());
            }

            Awaitility.await()
                    .atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> assertThat(polyBoltStorage.getTotalRecordsWritten()).isGreaterThanOrEqualTo(3L));

            // Strategy queues MUST still be strictly 0!
            assertThat(clobClient.hasQueuedMarketFrames()).isFalse();
            assertThat(clobClient.hasQueuedUserFrames()).isFalse();
            assertThat(polyBoltClient.getStrategyQueueDepth()).isZero();
            assertThat(coordinator.evaluateHealth().healthy()).isTrue();
        } finally {
            coordinator.stopRuntime();
        }

        // Inspect activeSlug .jsonl.zst archive: uniqueBookPayload MUST appear strictly ONCE (never duplicated by dual strategy legs!)
        // and Binance aggTrades directory MUST NOT exist!
        assertThat(Files.exists(collectorDir.resolve("binance/spot/BTCUSDT/aggTrades"))).isFalse();
        Path activeMarketFile = collectorDir.resolve("polymarket/btc-updown-15m/" + activeSlug + ".jsonl.zst");
        assertThat(Files.exists(activeMarketFile)).isTrue();
        List<String> lines = ZstdJsonlWriterTest.readAllZstdLines(activeMarketFile);
        long bookCount = lines.stream().filter(l -> l.contains("\"size\":\"999\"")).count();
        assertThat(bookCount).isEqualTo(1L);
    }

    @Test
    void shouldPersistAndRestoreTargetPriceAcrossShutdownSoNewInstanceLocksUtterParameciumTargetPriceWithoutBoundaryTick() {
        long candleStartSec = 1_796_000_400L; // 900s aligned (1796000400 % 900 == 0)
        long candleStartMs = candleStartSec * 1_000L;
        long candleEndMs = (candleStartSec + 900L) * 1_000L;

        // 1. Old instance receives boundary 60s TWAP ($90,000.00) at t=0s and shuts down at t=+3s
        StrategyTradingEngine oldEngine = createEngine("PAPER", false, 2_500L, 0L);
        oldEngine.discoverAndRotateMarket(candleStartSec + 1L);
        oldEngine.onPolymarketCryptoTwap60s(candleStartMs, "btcusd", "chainlink", 90_000.00, 60, candleStartMs);
        oldEngine.onPolymarketCryptoPrice(candleStartMs + 2_000L, "btcusd", "chainlink", 90_002.00, candleStartMs + 2_000L);

        DataCollectorService dummyCollector = org.mockito.Mockito.mock(DataCollectorService.class);
        RuntimeCoordinator oldCoordinator = new RuntimeCoordinator(dummyCollector, oldEngine, paperRepository, stateStore);
        oldCoordinator.stopRuntime();

        assertThat(stateStore.load()).isPresent();
        assertThat(stateStore.load().get().twapOpenPrice()).isEqualTo(90_000.00);

        // 2. New instance boots 120s into the SAME 15m candle (long after the 10s boundary window!)
        StrategyTradingEngine newEngine = createEngine("LIVE", false, 2_500L, 0L);
        RuntimeCoordinator newCoordinator = new RuntimeCoordinator(dummyCollector, newEngine, paperRepository, stateStore);
        newCoordinator.startRuntime(candleStartSec + 120L);

        // Persisted mode (PAPER), active market metadata, and twapOpenPrice ($90,000.00) are restored!
        assertThat(newEngine.getStrategyMode()).isEqualTo(StrategyMode.PAPER);
        assertThat(newEngine.getTwapOpenPrice()).isEqualTo(90_000.00);
        assertThat(newEngine.getActiveMarketInfo()).isNotNull();
        assertThat(newEngine.getActiveMarketInfo().slug()).isEqualTo("btc-updown-15m-" + candleStartSec);

        // 3. Feed late-candle spot + Chainlink + OrderBookQuote WITHOUT ever re-sending t=0s TWAP:
        //    UtterParameciumStrategy MUST already have targetPriceCents locked to $90,000.00 and trigger an UP order!
        newEngine.onRawKrakenFrame(
                "USDTUSD",
                "{\"channel\":\"trade\",\"type\":\"update\",\"data\":[{\"symbol\":\"USDT/USD\",\"side\":\"buy\",\"price\":1.0000,\"qty\":500.0}]}",
                candleEndMs - 61_000L
        );
        long windowStartMs = candleEndMs - 60_000L;
        for (int s = 0; s <= 14; s++) {
            long ts = windowStartMs + s * 1_000L;
            double osc = (s % 2 == 0) ? 0.75 : -0.75;
            double spot = 89_995.00 + osc;
            newEngine.onRawBinanceFrameBatch(List.of(new TimestampedFrame(
                    ("{\"e\":\"aggTrade\",\"p\":\"" + spot + "\",\"q\":\"1.0\",\"m\":false}").getBytes(StandardCharsets.UTF_8),
                    ts - 240L
            )));
            newEngine.onRawCoinbaseFrame("BTCUSD", "{\"type\":\"match\",\"price\":\"" + spot + "\",\"size\":\"1.0\"}", ts - 220L);
            newEngine.onRawKrakenFrame("BTCUSD", "{\"channel\":\"trade\",\"type\":\"update\",\"data\":[{\"symbol\":\"BTC/USD\",\"price\":" + spot + ",\"qty\":1.0}]}", ts - 200L);
            newEngine.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", 89_997.00 + osc, ts);
        }

        long tau46Ms = candleEndMs - 46_000L;
        newEngine.onOrderBookQuote(new OrderBookQuote(
                0.46, 0.44, 0.72, 0.70, 100.0, 100.0, 0.46, 0.72, tau46Ms + 100L
        ));
        long breakoutTs = tau46Ms + 400L;
        newEngine.onRawBinanceFrameBatch(List.of(new TimestampedFrame(
                "{\"e\":\"aggTrade\",\"p\":\"90018.00\",\"q\":\"2.0\",\"m\":false}".getBytes(StandardCharsets.UTF_8),
                breakoutTs
        )));
        newEngine.onRawCoinbaseFrame("BTCUSD", "{\"type\":\"match\",\"price\":\"90018.00\",\"size\":\"2.0\"}", breakoutTs + 5L);
        newEngine.onRawKrakenFrame("BTCUSD", "{\"channel\":\"trade\",\"type\":\"update\",\"data\":[{\"symbol\":\"BTC/USD\",\"price\":90018.00,\"qty\":2.0}]}", breakoutTs + 10L);

        assertThat(newEngine.getOpenPositions()).hasSize(1);
        assertThat(newEngine.getOpenPositions().getFirst().side()).isEqualTo(TradeDirection.UP);

        // 4. Starting a third instance in a SUBSEQUENT 15m candle (candleStartSec + 900) skips expired twapOpenPrice cleanly
        StrategyTradingEngine nextCandleEngine = createEngine("PAPER", false, 2_500L, 0L);
        RuntimeCoordinator nextCandleCoordinator = new RuntimeCoordinator(dummyCollector, nextCandleEngine, paperRepository, stateStore);
        nextCandleCoordinator.startRuntime(candleStartSec + 905L);
        assertThat(nextCandleEngine.getTwapOpenPrice()).isEqualTo(0.0);
        assertThat(nextCandleEngine.getOpenPositions()).isEmpty();
    }

    @Test
    void shouldSkipStaleQuotesAndFailFastWhenStartingLiveModeWithoutCredentials() {
        StrategyTradingEngine engine = createEngine("LIVE", false, 200L, 0L);
        engine.syncAccountBalance(1000.0);
        engine.discoverAndRotateMarket(1792741500L);
        engine.onPolymarketCryptoPrice(1792742000_000L, "btcusd", "chainlink", 85100.0, 1792742000_000L);
        engine.onOrderBookQuote(new OrderBookQuote(
                0.55, 0.54, 0.45, 0.44, 250.0, 300.0, 0.545, 0.445, 1792741999_500L
        ));

        TelemetryDto telemetry = engine.getTelemetry();
        assertThat(telemetry.staleQuotesSkipped()).isEqualTo(1L);
        assertThat(telemetry.lastQuoteAgeMs()).isEqualTo(500L);

        PolymarketSecretsResolver emptyResolver = new PolymarketSecretsResolver(
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()
        );
        HttpClient httpClient = HttpClient.newHttpClient();
        String baseHttpUrl = "http://localhost:" + port;
        TradingStrategy strat = new UtterParameciumStrategy(UtterParameciumStrategyConfig.defaultModelV1());
        StrategyTradingEngine unconfiguredEngine = new StrategyTradingEngine(
                new StrategyRegistry(Map.of(strat.getName(), strat)),
                emptyResolver,
                new PolymarketClobRestClient(baseHttpUrl, httpClient, objectMapper, emptyResolver),
                new PolymarketGammaClient(baseHttpUrl, httpClient, objectMapper),
                new PolymarketClobClient(vertx, marketWsRuntime, "ws://localhost:" + port + "/ws/clob", 1000L, 10000L),
                new PolymarketPolyBoltClient(marketWsRuntime, "ws://localhost:" + port + "/ws/polybolt",
                        Optional.empty(), Optional.empty(), Optional.empty(), 1000L, 10000L, 30_000L),
                binanceConnector,
                krakenConnector,
                coinbaseConnector,
                new PolymarketStreamParser(objectMapper, "BTCUSDT"),
                spotStreamProcessor,
                paperRepository,
                stateStore,
                new SystemNetworkTelemetryMonitor(),
                vertx,
                "LIVE",
                "UtterParameciumStrategy",
                "BTCUSDT",
                "15m",
                false,
                "FAK",
                250L,
                0L
        );
        assertThatThrownBy(() -> unconfiguredEngine.startConfiguredMode(1792741500L))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to resolve Polymarket credentials");
    }

    @Test
    void shouldSkipStaleOrSkippedCandleResolutionAndKeepMonotonicMarketCounters() {
        StrategyTradingEngine engine = createEngine("PAPER", false, 5_000L, 0L);

        long c0 = 1_795_536_000L; // 16:00:00Z
        long c1 = c0 + 900L;      // 16:15:00Z
        long c2 = c0 + 1_800L;    // 16:30:00Z
        long c3 = c0 + 2_700L;    // 16:45:00Z
        long c4 = c0 + 3_600L;    // 17:00:00Z

        // Start on c0 with valid opening TWAP and an open paper position
        engine.discoverAndRotateMarket(c0 + 1L);
        engine.onPolymarketCryptoTwap60s(c0 * 1000L, "btcusd", "chainlink", 60_000.0, 60, c0 * 1000L);
        engine.onOrderBookQuote(new OrderBookQuote(
                0.55, 0.54, 0.45, 0.44, 500.0, 500.0, 0.545, 0.445, c0 * 1000L + 500L
        ));
        engine.submitOrder(new OrderCommand(
                "client-c0-1", "mkt-c0", "BTC-UP", Timeframe.FIFTEEN_MINUTES, "BUY",
                BigDecimal.valueOf(0.55), BigDecimal.valueOf(100.0)
        ));
        assertThat(engine.getOpenPositions()).hasSize(1);

        // Simulate 1-hour PolyBolt outage while scheduler rotates markets c0 -> c1 -> c2 -> c3 -> c4
        engine.discoverAndRotateMarket(c1 + 1L);
        engine.discoverAndRotateMarket(c2 + 1L);
        engine.discoverAndRotateMarket(c3 + 1L);
        engine.discoverAndRotateMarket(c4 + 1L);

        // Record CLOB frames belonging to live market c4
        engine.onRawClobFrame("""
                {"event_type":"book","asset_id":"%s","bids":[{"price":"0.50","size":"500"}],"asks":[{"price":"0.51","size":"500"}]}
                """.formatted(TEST_TOKEN_UP), c4 * 1000L + 10_000L);
        assertThat(engine.getClobMarketMessageCount()).isEqualTo(1L);

        // PolyBolt resumes 6m51s into c4: must NOT settle c0 at stale price and must NOT reset c4's CLOB counter!
        long resumeTsMs = c4 * 1000L + 411_000L;
        engine.onPolymarketCryptoTwap60s(resumeTsMs, "btcusd", "chainlink", 59_341.62, 60, resumeTsMs);

        assertThat(paperRepository.getTrades()).isEmpty();
        assertThat(engine.getOpenPositions()).isEmpty();
        assertThat(engine.getClobMarketMessageCount()).isEqualTo(1L);
        assertThat(engine.getTwapOpenPrice()).isZero();

        // Next adjacent cycle c5: open cleanly at c5 boundary, open position, then test late TWAP (>10s past c6)
        long c5 = c4 + 900L;
        long c6 = c5 + 900L;
        engine.discoverAndRotateMarket(c5 + 1L);
        engine.onPolymarketCryptoTwap60s(c5 * 1000L, "btcusd", "chainlink", 59_800.0, 60, c5 * 1000L);
        assertThat(engine.getTwapOpenPrice()).isEqualTo(59_800.0);
        engine.onOrderBookQuote(new OrderBookQuote(
                0.55, 0.54, 0.45, 0.44, 500.0, 500.0, 0.545, 0.445, c5 * 1000L + 1000L
        ));
        engine.submitOrder(new OrderCommand(
                "client-c5-1", "mkt-c5", "BTC-UP", Timeframe.FIFTEEN_MINUTES, "BUY",
                BigDecimal.valueOf(0.55), BigDecimal.valueOf(100.0)
        ));
        assertThat(engine.getOpenPositions()).hasSize(1);

        // TWAP arrives 60s late into c6 -> must skip candle resolution rather than settling with stale price
        long lateC6TsMs = c6 * 1000L + 60_000L;
        engine.onPolymarketCryptoTwap60s(lateC6TsMs, "btcusd", "chainlink", 59_900.0, 60, lateC6TsMs);
        assertThat(paperRepository.getTrades()).isEmpty();
        assertThat(engine.getOpenPositions()).isEmpty();
    }

    private PolymarketSecretsResolver defaultSecretsResolver() {
        return new PolymarketSecretsResolver(
                Optional.of(TEST_PRIVATE_KEY),
                Optional.of("api-key-live"),
                Optional.of("passphrase-live"),
                Optional.of("MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI="),
                Optional.of(TEST_MAKER_ADDRESS),
                Optional.of(TEST_EOA_ADDRESS)
        );
    }

    private StrategyTradingEngine createEngine(
            String mode,
            boolean streamsEnabled,
            long maxQuoteAgeMs,
            long orderLatencyMs) {
        PolymarketSecretsResolver secretsResolver = defaultSecretsResolver();
        HttpClient httpClient = HttpClient.newHttpClient();
        String baseHttpUrl = "http://localhost:" + port;
        UtterParameciumStrategy strategy = new UtterParameciumStrategy(UtterParameciumStrategyConfig.defaultModelV1());
        return new StrategyTradingEngine(
                new StrategyRegistry(Map.of(strategy.getName(), strategy)),
                secretsResolver,
                new PolymarketClobRestClient(baseHttpUrl, httpClient, objectMapper, secretsResolver),
                new PolymarketGammaClient(baseHttpUrl, httpClient, objectMapper),
                new PolymarketClobClient(vertx, marketWsRuntime, "ws://localhost:" + port + "/ws/clob", 1000L, 10000L),
                new PolymarketPolyBoltClient(
                        marketWsRuntime,
                        "ws://localhost:" + port + "/ws/polybolt",
                        Optional.of("api-key-live"),
                        Optional.of("MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI="),
                        Optional.of("passphrase-live"),
                        1000L,
                        10000L,
                        30_000L
                ),
                binanceConnector,
                krakenConnector,
                coinbaseConnector,
                new PolymarketStreamParser(objectMapper, "BTCUSDT"),
                spotStreamProcessor,
                paperRepository,
                stateStore,
                new SystemNetworkTelemetryMonitor(),
                vertx,
                mode,
                "UtterParameciumStrategy",
                "BTCUSDT",
                "15m",
                streamsEnabled,
                "FAK",
                maxQuoteAgeMs,
                orderLatencyMs
        );
    }
}
